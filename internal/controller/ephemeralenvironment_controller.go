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
	"github.com/nuromirg/petri/internal/provisioner"
	"github.com/nuromirg/petri/internal/renderer"
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
	// TODO make them configurable through EphemeralEnvironment.Spec.TTL.
	deployTimeout = 15 * time.Minute
	// deployConcurrency bounds how many deploy/undeploy Jobs are submitted or
	// observed at once, per level.
	deployConcurrency = 4

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
	Scheme      *runtime.Scheme
	Deployer    deployer.Deployer
	Provisioner provisioner.Provisioner
	Checker     checker
}

// +kubebuilder:rbac:groups=core.petri.run,resources=ephemeralenvironments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.petri.run,resources=ephemeralenvironments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.petri.run,resources=ephemeralenvironments/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.petri.run,resources=environmenttemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=core.petri.run,resources=sharedcomponentproviders,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;delete;patch
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,verbs=bind,resourceNames=admin
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;create;delete

func (r *EphemeralEnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
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

	if env.Status.Phase == v1alpha1.PhaseFailed {
		return ctrl.Result{}, nil
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

func (r *EphemeralEnvironmentReconciler) reconcileDelete(ctx context.Context, env *v1alpha1.EphemeralEnvironment) (res ctrl.Result, err error) {
	patcher := helpers.NewStatusPatcher(r.Client, env)
	defer func() {
		err = errors.Join(err, patcher.Patch(ctx, env))
	}()

	log := logf.FromContext(ctx)

	targetNs, err := r.targetNamespace(env)
	if err != nil {
		log.Error(err, "invalid namespace during deletion, skipping cleanup")
		return ctrl.Result{}, r.removeFinalizer(ctx, env)
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
		done, res, err := r.undeployAll(ctx, env, targetNs, template.Spec.Components)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !done {
			return res, nil
		}

		done, res, err = r.deprovisionShared(ctx, env, targetNs, template)
		if err != nil {
			log.Error(err, "deprovision shared failed")
			return ctrl.Result{}, err
		}
		if !done {
			log.Info("deprovision shared in progress, requeuing")
			return res, nil
		}
		log.Info("deprovision shared complete")
	}

	ns := &corev1.Namespace{}
	err = r.Get(ctx, client.ObjectKey{Name: targetNs}, ns)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	if err == nil {
		if _, managed := ns.Labels[managedLabel]; !managed {
			log.Info("namespace not managed by Petri, skipping cleanup", "namespace", targetNs)
			return ctrl.Result{}, r.removeFinalizer(ctx, env)
		}
		if err := r.deleteNamespace(ctx, targetNs); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, r.removeFinalizer(ctx, env)
}

func (r *EphemeralEnvironmentReconciler) deprovisionShared(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, template *v1alpha1.EnvironmentTemplate) (done bool, res ctrl.Result, err error) {
	log := logf.FromContext(ctx)

	for _, component := range template.Spec.Components {
		if component.SharedComponentRef == "" {
			continue
		}

		sc := new(v1alpha1.SharedComponent)
		if err := r.Get(ctx, client.ObjectKey{Name: component.SharedComponentRef, Namespace: env.Namespace}, sc); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}

			return false, ctrl.Result{}, err
		}

		scp := new(v1alpha1.SharedComponentProvider)
		if err := r.Get(ctx, client.ObjectKey{Name: sc.Spec.Provider, Namespace: env.Namespace}, scp); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, ctrl.Result{}, err
		}

		provName := "shared-" + sc.Name + "-provision-" + env.Name
		bindingName := env.Name + "-" + component.Name + "-binding"

		if scp.Spec.Deprovision == nil {
			if err := r.deleteSecret(ctx, bindingName, targetNs); err != nil {
				return false, ctrl.Result{}, err
			}
			continue
		}

		binding := new(corev1.Secret)
		if err := r.Get(ctx, client.ObjectKey{Name: bindingName, Namespace: targetNs}, binding); err != nil && !apierrors.IsNotFound(err) {
			return false, ctrl.Result{}, err
		}
		var genSecret string
		if binding.Data != nil {
			genSecret = string(binding.Data[generatedSecretKey])
		}

		if err := r.ensureProvisionSecret(ctx, provName, env.Name, genSecret); err != nil {
			return false, ctrl.Result{}, err
		}

		state, err := r.Provisioner.ObserveDeprovision(ctx, provisioner.ProvisionOptions{
			EnvName:       env.Name,
			ComponentName: component.Name,
			SharedName:    sc.Name,
		})
		if err != nil {
			return false, ctrl.Result{}, err
		}

		switch state.Phase {
		case deployer.SucceededJobPhase:
			deprovJobName := provisioner.DeprovisionJobName(env.Name, component.Name)
			if err := r.deleteJob(ctx, deprovJobName, sharedNamespace); err != nil {
				return false, ctrl.Result{}, err
			}
			if err := r.deleteSecret(ctx, provName, sharedNamespace); err != nil {
				return false, ctrl.Result{}, err
			}
			if err := r.deleteSecret(ctx, bindingName, targetNs); err != nil {
				return false, ctrl.Result{}, err
			}

		case deployer.PendingJobPhase, deployer.FailedJobPhase:
			if err := scp.Spec.Deprovision.Validate(); err != nil {
				return false, ctrl.Result{}, fmt.Errorf("invalid deprovision script: %w", err)
			}
			script := *scp.Spec.Deprovision
			script.Command = append([]string(nil), scp.Spec.Deprovision.Command...)
			deprovVars := renderer.Vars{Env: renderer.EnvVarsFor(env.Name, genSecret)}
			if script.Script != "" {
				rendered, err := renderer.Render(script.Script, deprovVars)
				if err != nil {
					return false, ctrl.Result{}, fmt.Errorf("render deprovision script: %w", err)
				}
				script.Script = rendered
			}
			for i, c := range script.Command {
				rendered, err := renderer.Render(c, deprovVars)
				if err != nil {
					return false, ctrl.Result{}, fmt.Errorf("render deprovision command[%d]: %w", i, err)
				}
				script.Command[i] = rendered
			}

			if err := r.Provisioner.SubmitDeprovision(ctx, provisioner.ProvisionOptions{
				EnvName:              env.Name,
				ComponentName:        component.Name,
				SharedName:           sc.Name,
				Script:               script,
				ProvisionerSecretRef: provName,
			}); err != nil {
				return false, ctrl.Result{}, err
			}

			if state.Phase == deployer.FailedJobPhase {
				if recordRuntimeFailure(env, component.Name, "deprovision: "+state.Reason) {
					log.Error(errors.New(state.Reason), "deprovision exhausted retries, forcing cleanup", "component", component.Name)
					if err := r.deleteSecret(ctx, provName, sharedNamespace); err != nil {
						return false, ctrl.Result{}, err
					}
					if err := r.deleteSecret(ctx, bindingName, targetNs); err != nil {
						return false, ctrl.Result{}, err
					}
					continue
				}
			}
			return false, ctrl.Result{RequeueAfter: requeueAfter}, nil

		case deployer.RunningJobPhase:
			return false, ctrl.Result{RequeueAfter: requeueAfter}, nil
		}
	}

	return true, ctrl.Result{}, nil
}

func (r *EphemeralEnvironmentReconciler) ensureDeployerRoleBinding(ctx context.Context, targetNs string) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: deployerRoleBinding, Namespace: targetNs},
	}
	if err := r.Create(ctx, sa); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}

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
			Name:      deployerRoleBinding,
			Namespace: targetNs,
		}},
	}

	return r.Create(ctx, rb)
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

func (r *EphemeralEnvironmentReconciler) removeFinalizer(ctx context.Context, env *v1alpha1.EphemeralEnvironment) error {
	patch := client.MergeFrom(env.DeepCopy())
	controllerutil.RemoveFinalizer(env, finalizer)
	return r.Patch(ctx, env, patch)
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
