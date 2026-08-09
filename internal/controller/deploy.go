/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/nuromirg/petri/api/v1alpha1"
	"github.com/nuromirg/petri/internal/deployer"
	"golang.org/x/sync/errgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *EphemeralEnvironmentReconciler) processLevel(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, level []v1alpha1.ComponentSpec, phaseByName map[string]v1alpha1.Phase) (ctrl.Result, error) {
	needDeploy, submitting, needCheck := partitionLevel(level, phaseByName)

	logf.FromContext(ctx).V(1).Info("processing frontier",
		"needDeploy", len(needDeploy), "submitting", len(submitting), "needCheck", len(needCheck))

	if len(needDeploy) > 0 {
		return r.submitDeploys(ctx, env, targetNs, needDeploy)
	}

	if len(submitting) > 0 {
		if res, handled, err := r.observeDeploys(ctx, env, targetNs, submitting); handled || err != nil {
			return res, err
		}
	}

	return r.checkReadiness(ctx, env, targetNs, needCheck)
}

func partitionLevel(level []v1alpha1.ComponentSpec, phaseByName map[string]v1alpha1.Phase) (needDeploy, submitting, needCheck []v1alpha1.ComponentSpec) {
	for _, component := range level {
		switch phaseByName[component.Name] {
		case v1alpha1.PhaseReady, v1alpha1.PhaseFailed:
			continue
		case v1alpha1.PhaseSubmitting:
			submitting = append(submitting, component)
		case v1alpha1.PhaseDeploying:
			needCheck = append(needCheck, component)
		default:
			needDeploy = append(needDeploy, component)
		}
	}
	return needDeploy, submitting, needCheck
}

func (r *EphemeralEnvironmentReconciler) submitDeploys(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, needDeploy []v1alpha1.ComponentSpec) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("deploying components", "components", componentNames(needDeploy))

	submitErrs := r.eachComponent(ctx, needDeploy, func(gctx context.Context, _ int, c v1alpha1.ComponentSpec) error {
		return r.Deployer.Submit(gctx, r.deployOpts(env, targetNs, c))
	})

	exhausted := false
	stillRetrying := false
	for i, component := range needDeploy {
		if submitErrs[i] == nil {
			// deploy Job submitted; wait for it via Observe on the next pass
			setComponentPhase(env, component.Name, v1alpha1.PhaseSubmitting)
			resetComponentFailure(env, component.Name)
			continue
		}

		if recordRuntimeFailure(env, component.Name, submitErrs[i].Error()) {
			log.Error(submitErrs[i], "component deploy failed permanently", "component", component.Name)
			setComponentPhase(env, component.Name, v1alpha1.PhaseFailed)
			exhausted = true
		} else {
			log.Info("component deploy failed, will retry", "component", component.Name, "error", submitErrs[i].Error())
			stillRetrying = true
		}
	}

	if exhausted {
		return ctrl.Result{}, r.setFailed(env, "DeployFailed", "one or more components failed to deploy")
	}
	if stillRetrying {
		return ctrl.Result{RequeueAfter: deployBackoff(env, needDeploy)}, nil
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *EphemeralEnvironmentReconciler) observeDeploys(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, submitting []v1alpha1.ComponentSpec) (res ctrl.Result, handled bool, err error) {
	log := logf.FromContext(ctx)

	stillRunning := false
	retrying := false
	for _, component := range submitting {
		st, err := r.Deployer.Observe(ctx, r.deployOpts(env, targetNs, component))
		if err != nil {
			return ctrl.Result{}, true, err
		}

		switch st.Phase {
		case deployer.SucceededJobPhase:
			setComponentPhase(env, component.Name, v1alpha1.PhaseDeploying)
			setComponentDeployingSince(env, component.Name, metav1.Now())
		case deployer.FailedJobPhase:
			if recordRuntimeFailure(env, component.Name, st.Reason) {
				setComponentPhase(env, component.Name, v1alpha1.PhaseFailed)
				return ctrl.Result{}, true, r.setFailed(env, "DeployFailed", component.Name+": "+st.Reason)
			}

			log.Info("deploy job failed, will retry", "component", component.Name, "reason", st.Reason)
			retrying = true
		default:
			log.V(1).Info("deploy job in progress", "component", component.Name, "phase", st.Phase)
			stillRunning = true
		}
	}

	if retrying {
		return ctrl.Result{RequeueAfter: deployBackoff(env, submitting)}, true, nil
	}
	if stillRunning {
		return ctrl.Result{RequeueAfter: requeueAfter}, true, nil
	}
	// all succeeded this pass; let the caller run readiness
	return ctrl.Result{}, false, nil
}

func (r *EphemeralEnvironmentReconciler) checkReadiness(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, needCheck []v1alpha1.ComponentSpec) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	allDone := true
	for _, component := range needCheck {
		if since := componentDeployingSince(env, component.Name); since != nil && time.Since(since.Time) > deployTimeout {
			if recordRuntimeFailure(env, component.Name, "readiness timeout after "+deployTimeout.String()) {
				setComponentPhase(env, component.Name, v1alpha1.PhaseFailed)
				return ctrl.Result{}, r.setFailed(env, "ReadinessTimeout",
					fmt.Sprintf("%s did not become ready within %s (retries exhausted)", component.Name, deployTimeout))
			}
			log.Info("readiness timed out, will resubmit", "component", component.Name)
			continue
		}

		ready, reason, err := r.Checker.IsReady(ctx, targetNs, env.Name+"-"+component.Name, component.Readiness)
		if err != nil {
			return ctrl.Result{}, err
		}
		if ready {
			log.Info("component ready", "component", component.Name)
			setComponentPhase(env, component.Name, v1alpha1.PhaseReady)
			resetComponentFailure(env, component.Name)
		} else {
			log.Info("component not ready", "component", component.Name, "reason", reason)
			allDone = false
		}
	}

	if allDone {
		return ctrl.Result{RequeueAfter: requeueImmediate}, nil
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *EphemeralEnvironmentReconciler) eachComponent(ctx context.Context, components []v1alpha1.ComponentSpec, fn func(ctx context.Context, i int, c v1alpha1.ComponentSpec) error) []error {
	errs := make([]error, len(components))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(deployConcurrency)
	for i, component := range components {
		g.Go(func() error {
			errs[i] = fn(gctx, i, component)
			return nil
		})
	}
	_ = g.Wait()
	return errs
}

// deployOpts builds the DeployOptions for a component's release.
func (r *EphemeralEnvironmentReconciler) deployOpts(env *v1alpha1.EphemeralEnvironment, targetNs string, c v1alpha1.ComponentSpec) deployer.DeployOptions {
	return deployer.DeployOptions{
		Namespace:   targetNs,
		ReleaseName: env.Name + "-" + c.Name,
		Component:   c,
	}
}

func componentNames(components []v1alpha1.ComponentSpec) []string {
	names := make([]string, len(components))
	for i, c := range components {
		names[i] = c.Name
	}
	return names
}
