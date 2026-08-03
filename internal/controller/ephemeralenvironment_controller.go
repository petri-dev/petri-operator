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
	"fmt"
	"strings"
	"time"

	"github.com/nuromirg/petri/api/v1alpha1"
	"github.com/nuromirg/petri/internal/deployer"
	"github.com/nuromirg/petri/internal/graph"
	"github.com/nuromirg/petri/internal/helpers"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	finalizer    = "petri.run/cleanup"
	managedLabel = "petri.run/managed"

	helmMaxReleaseNameLength = 53

	requeueAfter     = 10 * time.Second
	requeueImmediate = time.Second
	maxDeployRetries = 5
	maxDeployBackoff = 5 * time.Minute

	nsPrefix = "petri-"

	deployerRoleBinding = "petri-deployer"
)

var ErrNamespaceNotManaged = errors.New("namespace not managed by Petri")

type checker interface {
	IsReady(ctx context.Context, namespace string, releaseName string, readiness *v1alpha1.ReadinessSpec) (bool, string, error)
}

// EphemeralEnvironmentReconciler reconciles a EphemeralEnvironment object.
type EphemeralEnvironmentReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Deployer deployer.Deployer
	Checker  checker
}

// +kubebuilder:rbac:groups=core.petri.run,resources=ephemeralenvironments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.petri.run,resources=ephemeralenvironments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.petri.run,resources=ephemeralenvironments/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.petri.run,resources=environmenttemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,verbs=bind,resourceNames=admin

func (r *EphemeralEnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	env := new(v1alpha1.EphemeralEnvironment)

	if err := r.Get(ctx, req.NamespacedName, env); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !env.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, env)
	}

	if !controllerutil.ContainsFinalizer(env, finalizer) {
		patch := client.MergeFrom(env.DeepCopy())
		controllerutil.AddFinalizer(env, finalizer)
		return ctrl.Result{}, r.Patch(ctx, env, patch)
	}

	return r.reconcile(ctx, env)
}

func (r *EphemeralEnvironmentReconciler) reconcile(ctx context.Context, env *v1alpha1.EphemeralEnvironment) (res ctrl.Result, err error) {
	log := logf.FromContext(ctx)

	patcher := helpers.NewStatusPatcher(r.Client, env)
	defer func() {
		err = errors.Join(err, patcher.Patch(ctx, env))
	}()

	if env.Status.ObservedGeneration != env.Generation {
		log.Info("spec changed, resetting environment state",
			"observedGeneration", env.Status.ObservedGeneration, "generation", env.Generation)
		env.Status.Phase = ""

		// we rely on helm idempotency here, so each non-equal scenario will do an acceptable install-or-upgrade with helm
		env.Status.Components = nil
		env.Status.ObservedGeneration = env.Generation
	}

	template, err := r.getEnvironmentTemplate(ctx, env)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.setFailed(env, "TemplateNotFound", "template "+env.Spec.Template+" not found")
		}
		return ctrl.Result{}, err
	}

	targetNs, err := r.targetNamespace(env)
	if err != nil {
		return ctrl.Result{}, r.setFailed(env, "InvalidConfiguration", err.Error())
	}

	if err := r.createNamespace(ctx, targetNs); err != nil {
		if errors.Is(err, ErrNamespaceNotManaged) {
			return ctrl.Result{}, r.setFailed(env, "NamespaceNotManaged", err.Error())
		}

		return ctrl.Result{}, err
	}

	componentsByLevel, err := graph.BuildLevels(template.Spec.Components)
	if err != nil {
		return ctrl.Result{}, r.setFailed(env, "InvalidConfiguration", err.Error())
	}

	for _, component := range template.Spec.Components {
		if releaseName := env.Name + "-" + component.Name; len(releaseName) > helmMaxReleaseNameLength {
			return ctrl.Result{}, r.setFailed(env, "InvalidConfiguration",
				fmt.Sprintf("release name %q exceeds %d characters", releaseName, helmMaxReleaseNameLength))
		}
	}

	for _, cs := range env.Status.Components {
		if cs.Phase == v1alpha1.PhaseFailed {
			return ctrl.Result{}, r.setFailed(env, "ComponentFailed", cs.Name+" failed to deploy")
		}
	}

	env.Status.Phase = v1alpha1.PhaseDeploying

	phaseByName := make(map[string]v1alpha1.Phase, len(env.Status.Components))
	for _, cs := range env.Status.Components {
		phaseByName[cs.Name] = cs.Phase
	}

	for i, level := range componentsByLevel {
		if allReady(level, phaseByName) {
			log.V(1).Info("level already ready, advancing", "level", i)
			continue
		}

		return r.processLevel(ctx, env, targetNs, level, phaseByName)
	}

	// TODO also update the status.URL field with domain
	if env.Status.Phase != v1alpha1.PhaseReady {
		log.Info("environment ready", "components", len(env.Status.Components))
	}
	env.Status.Phase = v1alpha1.PhaseReady
	return ctrl.Result{}, nil
}

func (r *EphemeralEnvironmentReconciler) ensureDeployerRoleBinding(ctx context.Context, targetNs string) error {
	rb := &rbacv1.RoleBinding{}
	err := r.Get(ctx, client.ObjectKey{Namespace: targetNs, Name: deployerRoleBinding}, rb)
	if err == nil {
		return nil
	}

	if !apierrors.IsNotFound(err) {
		return err
	}

	rb = &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: deployerRoleBinding, Namespace: targetNs},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     "admin",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      "petri-controller-manager",
			Namespace: "petri-system",
		}},
	}

	return r.Create(ctx, rb)
}

func allReady(level []v1alpha1.ComponentSpec, phaseByName map[string]v1alpha1.Phase) bool {
	for _, component := range level {
		if phaseByName[component.Name] != v1alpha1.PhaseReady {
			return false
		}
	}

	return true
}

func (r *EphemeralEnvironmentReconciler) processLevel(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, level []v1alpha1.ComponentSpec, phaseByName map[string]v1alpha1.Phase) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	needCheck := make([]v1alpha1.ComponentSpec, 0)
	needDeploy := make([]v1alpha1.ComponentSpec, 0)

	for _, component := range level {
		phase := phaseByName[component.Name]
		switch phase {
		case v1alpha1.PhaseReady:
			continue
		case v1alpha1.PhaseDeploying:
			needCheck = append(needCheck, component)
		default:
			needDeploy = append(needDeploy, component)
		}
	}

	log.V(1).Info("processing frontier", "needDeploy", len(needDeploy), "needCheck", len(needCheck))

	if len(needDeploy) > 0 {
		names := make([]string, len(needDeploy))
		for i, c := range needDeploy {
			names[i] = c.Name
		}
		log.Info("deploying components", "components", names)

		deployErrs := make([]error, len(needDeploy))
		g, gctx := errgroup.WithContext(ctx)
		for i, component := range needDeploy {
			g.Go(func() error {
				deployErrs[i] = r.Deployer.Deploy(gctx, deployer.DeployOptions{
					Namespace:   targetNs,
					ReleaseName: env.Name + "-" + component.Name,
					Component:   component,
				})
				return nil
			})
		}
		_ = g.Wait()

		exhausted := false
		stillRetrying := false
		for i, component := range needDeploy {
			if deployErrs[i] == nil {
				setComponentPhase(env, component.Name, v1alpha1.PhaseDeploying)
				setComponentRetries(env, component.Name, 0)
				continue
			}

			retries := incrementRetries(env, component.Name)
			if retries >= maxDeployRetries {
				log.Error(deployErrs[i], "component deploy failed permanently", "component", component.Name, "retries", retries)
				setComponentPhase(env, component.Name, v1alpha1.PhaseFailed)
				exhausted = true
			} else {
				log.Info("component deploy failed, will retry", "component", component.Name, "retries", retries, "error", deployErrs[i].Error())
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

	allDone := true
	for _, component := range needCheck {
		ready, reason, err := r.Checker.IsReady(ctx, targetNs, env.Name+"-"+component.Name, component.Readiness)
		if err != nil {
			return ctrl.Result{}, err
		}
		if ready {
			log.Info("component ready", "component", component.Name)
			setComponentPhase(env, component.Name, v1alpha1.PhaseReady)
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

func setComponentPhase(env *v1alpha1.EphemeralEnvironment, name string, phase v1alpha1.Phase) {
	// make sure we change the value, not the slice copy
	for i := range env.Status.Components {
		if env.Status.Components[i].Name == name {
			env.Status.Components[i].Phase = phase
			return
		}
	}
	env.Status.Components = append(env.Status.Components, v1alpha1.ComponentStatus{
		Name:  name,
		Phase: phase,
	})
}

func incrementRetries(env *v1alpha1.EphemeralEnvironment, name string) int32 {
	for i := range env.Status.Components {
		if env.Status.Components[i].Name == name {
			env.Status.Components[i].DeployRetries++
			return env.Status.Components[i].DeployRetries
		}
	}
	env.Status.Components = append(env.Status.Components, v1alpha1.ComponentStatus{
		Name:          name,
		DeployRetries: 1,
	})
	return 1
}

func setComponentRetries(env *v1alpha1.EphemeralEnvironment, name string, retries int32) {
	for i := range env.Status.Components {
		if env.Status.Components[i].Name == name {
			env.Status.Components[i].DeployRetries = retries
			return
		}
	}
}

func deployBackoff(env *v1alpha1.EphemeralEnvironment, components []v1alpha1.ComponentSpec) time.Duration {
	var maxRetries int32
	names := make(map[string]struct{}, len(components))
	for _, c := range components {
		names[c.Name] = struct{}{}
	}
	for _, cs := range env.Status.Components {
		if _, ok := names[cs.Name]; ok && cs.DeployRetries > maxRetries {
			maxRetries = cs.DeployRetries
		}
	}

	backoff := requeueAfter << maxRetries
	if backoff > maxDeployBackoff || backoff <= 0 {
		return maxDeployBackoff
	}
	return backoff
}

func (r *EphemeralEnvironmentReconciler) createNamespace(ctx context.Context, targetNs string) error {
	ns := &corev1.Namespace{}
	err := r.Get(ctx, client.ObjectKey{Name: targetNs}, ns)
	if err == nil {
		if _, managed := ns.Labels[managedLabel]; !managed {
			return fmt.Errorf("%w: %q", ErrNamespaceNotManaged, targetNs)
		}
		return r.ensureDeployerRoleBinding(ctx, targetNs)
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	ns = &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: targetNs,
			Labels: map[string]string{
				managedLabel: "true",
			},
		},
	}

	err = r.Create(ctx, ns)
	if err != nil {
		return fmt.Errorf("failed to create a namespace: %w", err)
	}

	return r.ensureDeployerRoleBinding(ctx, targetNs)
}

func (r *EphemeralEnvironmentReconciler) reconcileDelete(ctx context.Context, env *v1alpha1.EphemeralEnvironment) (res ctrl.Result, err error) {
	patcher := helpers.NewStatusPatcher(r.Client, env)
	defer func() {
		err = errors.Join(err, patcher.Patch(ctx, env))
	}()

	log := logf.FromContext(ctx)

	targetNs, err := r.targetNamespace(env)
	if err != nil {
		log.Error(err, "invalid namespace during deletion, skipping cleanup")
		patch := client.MergeFrom(env.DeepCopy())
		controllerutil.RemoveFinalizer(env, finalizer)
		return ctrl.Result{}, r.Patch(ctx, env, patch)
	}

	template, err := r.getEnvironmentTemplate(ctx, env)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	if err := r.ensureDeployerRoleBinding(ctx, targetNs); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	if env.Status.Phase != v1alpha1.PhaseTerminating {
		log.Info("tearing down environment", "namespace", targetNs)
	}
	env.Status.Phase = v1alpha1.PhaseTerminating

	if template != nil {
		undeployErrs := make([]error, 0)
		// tear down in reverse deploy order: dependents before their dependencies
		components := template.Spec.Components
		for i := len(components) - 1; i >= 0; i-- {
			component := components[i]
			log.V(1).Info("undeploying component", "component", component.Name)
			if err := r.Deployer.Undeploy(ctx, deployer.DeployOptions{
				Namespace:   targetNs,
				ReleaseName: env.Name + "-" + component.Name,
				Component:   component,
			}); err != nil {
				log.Error(err, "failed to undeploy component", "component", component.Name)
				undeployErrs = append(undeployErrs, err)
			}
		}

		if len(undeployErrs) > 0 {
			return ctrl.Result{}, errors.Join(undeployErrs...)
		}
	}

	ns := &corev1.Namespace{}
	err = r.Get(ctx, client.ObjectKey{Name: targetNs}, ns)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	if err == nil {
		if _, managed := ns.Labels[managedLabel]; !managed {
			log.Info("namespace not managed by Petri, skipping cleanup", "namespace", targetNs)
			patch := client.MergeFrom(env.DeepCopy())
			controllerutil.RemoveFinalizer(env, finalizer)
			return ctrl.Result{}, r.Patch(ctx, env, patch)
		}
		if err := r.deleteNamespace(ctx, targetNs); err != nil {
			return ctrl.Result{}, err
		}
	}

	patch := client.MergeFrom(env.DeepCopy())
	controllerutil.RemoveFinalizer(env, finalizer)
	return ctrl.Result{}, r.Patch(ctx, env, patch)
}

func (r *EphemeralEnvironmentReconciler) getEnvironmentTemplate(ctx context.Context, env *v1alpha1.EphemeralEnvironment) (*v1alpha1.EnvironmentTemplate, error) {
	template := new(v1alpha1.EnvironmentTemplate)
	if err := r.Get(ctx, client.ObjectKey{Name: env.Spec.Template, Namespace: env.Namespace}, template); err != nil {
		return nil, err
	}
	return template, nil
}

func (r *EphemeralEnvironmentReconciler) targetNamespace(env *v1alpha1.EphemeralEnvironment) (string, error) {
	suffix := env.Name
	if env.Spec.Namespace != "" {
		suffix = env.Spec.Namespace
	}

	ns := nsPrefix + suffix
	if errs := validation.IsDNS1123Label(ns); errs != nil {
		return "", fmt.Errorf("invalid namespace %q: %s", ns, strings.Join(errs, ", "))
	}

	return ns, nil
}

func (r *EphemeralEnvironmentReconciler) deleteNamespace(ctx context.Context, name string) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	err := r.Delete(ctx, ns)
	return client.IgnoreNotFound(err)
}

func (r *EphemeralEnvironmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.EphemeralEnvironment{}).
		Named("ephemeralenvironment").
		Complete(r)
}

func (r *EphemeralEnvironmentReconciler) setFailed(env *v1alpha1.EphemeralEnvironment, reason, message string) error {
	meta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: env.Generation,
	})
	if env.Status.Phase != v1alpha1.PhaseFailed {
		logf.Log.WithName("ephemeralenvironment").Info("environment failed",
			"name", env.Name, "reason", reason, "message", message)
	}
	env.Status.Phase = v1alpha1.PhaseFailed
	return nil
}
