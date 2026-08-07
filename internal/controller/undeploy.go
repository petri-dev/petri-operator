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
	"errors"

	"github.com/nuromirg/petri/api/v1alpha1"
	"github.com/nuromirg/petri/internal/deployer"
	"github.com/nuromirg/petri/internal/graph"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *EphemeralEnvironmentReconciler) undeployAll(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, components []v1alpha1.ComponentSpec) (done bool, res ctrl.Result, err error) {
	log := logf.FromContext(ctx)

	componentsByLevel, err := graph.BuildLevels(components)
	if err != nil {
		log.Error(err, "failed to build levels for undeploy, tearing down flat")
		componentsByLevel = [][]v1alpha1.ComponentSpec{components}
	}

	for l := len(componentsByLevel) - 1; l >= 0; l-- {
		level := componentsByLevel[l]

		levelDone, needSubmit, err := r.observeUndeployLevel(ctx, env, targetNs, level)
		if err != nil {
			return false, ctrl.Result{}, err
		}

		if len(needSubmit) > 0 {
			if err := r.submitUndeployLevel(ctx, env, targetNs, needSubmit); err != nil {
				return false, ctrl.Result{}, err
			}
		}

		if !levelDone {
			return false, ctrl.Result{RequeueAfter: requeueAfter}, nil
		}
	}

	return true, ctrl.Result{}, nil
}

func (r *EphemeralEnvironmentReconciler) observeUndeployLevel(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, level []v1alpha1.ComponentSpec) (levelDone bool, needSubmit []v1alpha1.ComponentSpec, err error) {
	log := logf.FromContext(ctx)

	states := make([]deployer.JobState, len(level))
	obsErrs := r.eachComponent(ctx, level, func(gctx context.Context, i int, c v1alpha1.ComponentSpec) error {
		st, err := r.Deployer.ObserveUndeploy(gctx, r.deployOpts(env, targetNs, c))
		states[i] = st
		return err
	})

	levelDone = true
	for i, component := range level {
		if obsErrs[i] != nil {
			return false, nil, obsErrs[i]
		}

		switch states[i].Phase {
		case deployer.SucceededJobPhase:
		case deployer.FailedJobPhase:
			if recordRuntimeFailure(env, component.Name, "undeploy: "+states[i].Reason) {
				log.Error(errors.New(states[i].Reason), "undeploy exhausted retries, forcing cleanup",
					"component", component.Name)
			} else {
				needSubmit = append(needSubmit, component)
				levelDone = false
			}
		default:
			needSubmit = append(needSubmit, component)
			levelDone = false
		}
	}

	return levelDone, needSubmit, nil
}

func (r *EphemeralEnvironmentReconciler) submitUndeployLevel(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, components []v1alpha1.ComponentSpec) error {
	errs := r.eachComponent(ctx, components, func(gctx context.Context, _ int, c v1alpha1.ComponentSpec) error {
		return r.Deployer.SubmitUndeploy(gctx, r.deployOpts(env, targetNs, c))
	})
	return errors.Join(errs...)
}
