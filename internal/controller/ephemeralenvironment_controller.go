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
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/petri-dev/petri-operator/internal/deployer"
	"github.com/petri-dev/petri-operator/internal/graph"
	"github.com/petri-dev/petri-operator/internal/helpers"
	"github.com/petri-dev/petri-operator/internal/provisioner"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
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
	// defaultDeployTimeout bounds component readiness when the template does not set spec.deployTimeout.
	defaultDeployTimeout = 15 * time.Minute
	// deployConcurrency bounds how many deploy/undeploy Jobs are submitted or
	// observed at once, per level.
	deployConcurrency = 4

	nsPrefix        = "petri-env-"
	ownerUIDLabel   = "petri.run/environment-uid"
	namespaceBound  = "NamespaceBound"
	cleanupComplete = "CleanupComplete"

	deployerRoleBinding = "petri-deployer"
)

var ErrNamespaceNotManaged = errors.New("namespace not managed by Petri")

func resolveDeadline(created time.Time, envTTL, templateTTL string) (*time.Time, error) {
	value := envTTL
	if value == "" {
		value = templateTTL
	}
	if value == "" {
		return nil, nil
	}

	ttl, err := time.ParseDuration(value)
	if err != nil {
		return nil, err
	}
	if ttl < 0 {
		return nil, errors.New("duration must be non-negative")
	}
	if ttl == 0 {
		return nil, nil
	}

	return new(created.Add(ttl)), nil
}

func resolveDeployTimeout(templateTimeout string, fallback time.Duration) (time.Duration, error) {
	if templateTimeout == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(templateTimeout)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, errors.New("deployTimeout must be positive")
	}
	return d, nil
}

type checker interface {
	IsReady(ctx context.Context, namespace string, releaseName string, readiness *v1alpha1.ReadinessSpec) (bool, string, error)
}

// EphemeralEnvironmentReconciler reconciles a EphemeralEnvironment object.
type EphemeralEnvironmentReconciler struct {
	client.Client
	APIReader   client.Reader
	Scheme      *runtime.Scheme
	Recorder    events.EventRecorder
	Deployer    deployer.Deployer
	Provisioner provisioner.Provisioner
	Checker     checker

	// DefaultDeployTimeout is the readiness timeout used when a template doesnt set spec.deployTimeout. Zero falls back to defaultDeployTimeout.
	DefaultDeployTimeout time.Duration

	// DeployerServiceAccount is the SA name that deploy Jobs run as.
	// The controller creates this SA and its RoleBinding in each target namespace.
	// Empty falls back to the "petri-deployer" default.
	DeployerServiceAccount string
}

// +kubebuilder:rbac:groups=core.petri.run,resources=ephemeralenvironments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.petri.run,resources=ephemeralenvironments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.petri.run,resources=ephemeralenvironments/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.petri.run,resources=environmenttemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=core.petri.run,resources=sharedcomponentproviders,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;delete;patch
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="events.k8s.io",resources=events,verbs=create;patch
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
	var deadline *time.Time

	oldPhase := env.Status.Phase
	// Phases this reconcile moves through, in order. We record metrics/Events
	// for them only after the status PATCH succeeds (see below), so a failed
	// write never double-counts a transition on the next attempt.
	var passedPhases []v1alpha1.EnvironmentPhase

	original := env.DeepCopy()
	patcher := helpers.NewStatusPatcher(r.Client, env)
	defer func() {
		if err == nil && env.Status.Phase != v1alpha1.EnvironmentPhaseTerminating && deadline != nil {
			remaining := time.Until(*deadline)
			if remaining <= 0 {
				remaining = time.Nanosecond
			}
			if res.RequeueAfter == 0 || remaining < res.RequeueAfter {
				res.RequeueAfter = remaining
			}
		}
		passedPhases = append(passedPhases, env.Status.Phase)
		var patchErr error
		if original.Status.TargetNamespace != env.Status.TargetNamespace ||
			meta.IsStatusConditionTrue(original.Status.Conditions, namespaceBound) != meta.IsStatusConditionTrue(env.Status.Conditions, namespaceBound) {
			patchErr = r.Status().Patch(ctx, env, client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{}))
		} else {
			patchErr = patcher.Patch(ctx, env)
		}
		if patchErr != nil {
			err = errors.Join(err, patchErr)
			return
		}
		recordPhaseTransitions(r.Recorder, env, oldPhase, passedPhases)
	}()

	if env.Status.TargetNamespace == "" {
		return ctrl.Result{RequeueAfter: requeueImmediate}, r.allocateNamespace(ctx, env, 8)
	}
	targetNs, err := r.targetNamespace(env)
	if err != nil {
		return ctrl.Result{}, r.setFailed(env, "InvalidConfiguration", err.Error())
	}

	if env.Status.ObservedGeneration != env.Generation {
		log.Info("spec changed, resetting environment state",
			"observedGeneration", env.Status.ObservedGeneration, "generation", env.Generation)
		env.Status.Phase = ""
		env.Status.DeployStartedAt = nil
		oldPhase = ""

		// we rely on helm idempotency here, so each non-equal scenario will do an acceptable install-or-upgrade with helm
		env.Status.Components = nil
		env.Status.ObservedGeneration = env.Generation
	}

	template, err := r.getEnvironmentTemplate(ctx, env)
	if err != nil {
		if apierrors.IsNotFound(err) {
			env.Status.ExpiresAt = nil
			return ctrl.Result{}, r.setFailed(env, "TemplateNotFound", "template "+env.Spec.Template+" not found")
		}
		return ctrl.Result{}, err
	}
	now := time.Now()

	deadline, err = resolveDeadline(env.CreationTimestamp.Time, env.Spec.TTL, template.Spec.TTL)
	if err != nil {
		env.Status.ExpiresAt = nil
		return ctrl.Result{}, r.setFailed(env, "InvalidTTL", err.Error())
	}

	if deadline == nil {
		env.Status.ExpiresAt = nil
	} else {
		env.Status.ExpiresAt = new(metav1.NewTime(*deadline))
	}

	if deadline != nil && !now.Before(*deadline) {
		env.Status.Phase = v1alpha1.EnvironmentPhaseTerminating
		deadline = nil
		return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, env))
	}

	deployTimeout, err := resolveDeployTimeout(template.Spec.DeployTimeout, cmp.Or(r.DefaultDeployTimeout, defaultDeployTimeout))
	if err != nil {
		return ctrl.Result{}, r.setFailed(env, "InvalidConfiguration", "invalid deployTimeout: "+err.Error())
	}

	if err := r.createNamespace(ctx, env); err != nil {
		if errors.Is(err, ErrNamespaceNotManaged) {
			if !meta.IsStatusConditionTrue(env.Status.Conditions, namespaceBound) {
				return ctrl.Result{RequeueAfter: requeueImmediate}, r.allocateNamespace(ctx, env, len(targetNs)-len(nsPrefix)+4)
			}
			return ctrl.Result{}, r.setFailed(env, "NamespaceNotManaged", err.Error())
		}

		return ctrl.Result{}, err
	}
	if !meta.IsStatusConditionTrue(env.Status.Conditions, namespaceBound) {
		meta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{
			Type: namespaceBound, Status: metav1.ConditionTrue, Reason: "NamespaceOwned",
			Message: "Target namespace is bound to this environment UID", ObservedGeneration: env.Generation,
		})
		return ctrl.Result{RequeueAfter: requeueImmediate}, nil
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

	if env.Status.Phase == v1alpha1.EnvironmentPhaseFailed {
		return ctrl.Result{}, nil
	}

	phaseByName := make(map[string]v1alpha1.ComponentPhase, len(env.Status.Components))
	for _, cs := range env.Status.Components {
		phaseByName[cs.Name] = cs.Phase
	}

	firstPending := -1
	for i, level := range componentsByLevel {
		if !allReady(level, phaseByName) {
			firstPending = i
			break
		}
	}

	// enter Deploying if there is pending work, or the env isnt Ready yet. We check components directly rather than trusting the phase: a new component
	// in the template doesnt bump the env generation, so a Ready env can gain work with no signal on the env itself.
	if env.Status.Phase != v1alpha1.EnvironmentPhaseDeploying &&
		(firstPending >= 0 || env.Status.Phase != v1alpha1.EnvironmentPhaseReady) {
		// we only get here when starting (or restarting) a deploy, so record when it began.
		env.Status.DeployStartedAt = new(metav1.Now())
		env.Status.Phase = v1alpha1.EnvironmentPhaseDeploying
		// Remember the intermediate Deploying; the deferred block replays it
		// after the PATCH succeeds. The env may settle back to Ready below in
		// the same reconcile, but this keeps the Deploying leg visible.
		passedPhases = append(passedPhases, v1alpha1.EnvironmentPhaseDeploying)
	}

	if firstPending >= 0 {
		level := componentsByLevel[firstPending]
		log.V(1).Info("processing first pending level", "level", firstPending)
		return r.processLevel(ctx, env, targetNs, level, phaseByName, deployTimeout)
	}

	// TODO also update the status.URL field with domain
	if env.Status.Phase != v1alpha1.EnvironmentPhaseReady {
		log.Info("environment ready", "components", len(env.Status.Components))
	}
	env.Status.Phase = v1alpha1.EnvironmentPhaseReady
	return ctrl.Result{}, nil
}

func (r *EphemeralEnvironmentReconciler) reconcileDelete(ctx context.Context, env *v1alpha1.EphemeralEnvironment) (res ctrl.Result, err error) {
	oldPhase := env.Status.Phase

	original := env.DeepCopy()
	patcher := helpers.NewStatusPatcher(r.Client, env)
	defer func() {
		if !controllerutil.ContainsFinalizer(env, finalizer) {
			return
		}
		var patchErr error
		if !meta.IsStatusConditionTrue(original.Status.Conditions, cleanupComplete) && meta.IsStatusConditionTrue(env.Status.Conditions, cleanupComplete) {
			patchErr = r.Status().Patch(ctx, env, client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{}))
		} else {
			patchErr = patcher.Patch(ctx, env)
		}
		if patchErr != nil {
			err = errors.Join(err, patchErr)
			return
		}
		// Record only after the PATCH lands, so a failed write doesn't count a
		// transition the next attempt would count again.
		recordPhaseTransition(r.Recorder, env, oldPhase)
	}()

	log := logf.FromContext(ctx)

	if env.Status.TargetNamespace == "" {
		return ctrl.Result{}, r.removeFinalizer(ctx, env)
	}
	targetNs, err := r.targetNamespace(env)
	if err != nil {
		return ctrl.Result{}, r.setFailed(env, "InvalidConfiguration", err.Error())
	}
	ns := new(corev1.Namespace)
	if err := r.namespaceReader().Get(ctx, client.ObjectKey{Name: targetNs}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			if meta.IsStatusConditionTrue(env.Status.Conditions, cleanupComplete) {
				return ctrl.Result{}, r.removeFinalizer(ctx, env)
			}
			if meta.IsStatusConditionTrue(env.Status.Conditions, namespaceBound) {
				return ctrl.Result{}, r.setFailed(env, "NamespaceNotManaged", "bound namespace is missing; admin cleanup of external allocations is required")
			}
			return ctrl.Result{}, r.removeFinalizer(ctx, env)
		}
		return ctrl.Result{}, err
	}
	if !ownsNamespace(env, ns) {
		log.Info("namespace not owned by environment, skipping all cleanup", "namespace", targetNs)
		if meta.IsStatusConditionTrue(env.Status.Conditions, namespaceBound) {
			return ctrl.Result{}, r.setFailed(env, "NamespaceNotManaged", "bound namespace ownership changed; admin cleanup is required")
		}
		return ctrl.Result{}, r.removeFinalizer(ctx, env)
	}

	if meta.IsStatusConditionTrue(env.Status.Conditions, cleanupComplete) {
		if err := client.IgnoreNotFound(r.Delete(ctx, ns, client.Preconditions{
			UID: &ns.UID, ResourceVersion: &ns.ResourceVersion,
		})); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.removeFinalizer(ctx, env)
	}

	template, err := r.getEnvironmentTemplate(ctx, env)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	if env.Status.Phase != v1alpha1.EnvironmentPhaseTerminating {
		log.Info("tearing down environment", "namespace", targetNs)
	}
	env.Status.Phase = v1alpha1.EnvironmentPhaseTerminating

	if template != nil && meta.IsStatusConditionTrue(env.Status.Conditions, namespaceBound) {
		if err := r.ensureDeployerRoleBinding(ctx, targetNs); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
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

	meta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{
		Type: cleanupComplete, Status: metav1.ConditionTrue, Reason: "CleanupFinished",
		Message: "Environment cleanup finished; target namespace can be deleted", ObservedGeneration: env.Generation,
	})
	return ctrl.Result{RequeueAfter: requeueImmediate}, nil
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

		provName := provisioner.ProvisionJobName(env.Name, component.Name) + "-credentials"
		bindingName := env.Name + "-" + component.Name + "-binding"
		provJob := new(batchv1.Job)
		if err := r.namespaceReader().Get(ctx, client.ObjectKey{Name: provisioner.ProvisionJobName(env.Name, component.Name), Namespace: sharedNamespace}, provJob); err == nil {
			if !provJob.DeletionTimestamp.IsZero() || deployer.TerminalPhase(provJob) == deployer.RunningJobPhase {
				return false, ctrl.Result{RequeueAfter: requeueAfter}, nil
			}
		} else if !apierrors.IsNotFound(err) {
			return false, ctrl.Result{}, err
		}

		if scp.Spec.Deprovision == nil {
			if err := r.deleteJob(ctx, provisioner.ProvisionJobName(env.Name, component.Name), sharedNamespace); err != nil {
				return false, ctrl.Result{}, err
			}
			if err := r.deleteSecret(ctx, provName, sharedNamespace); err != nil {
				return false, ctrl.Result{}, err
			}
			if err := r.deleteSecret(ctx, bindingName, targetNs); err != nil {
				return false, ctrl.Result{}, err
			}
			continue
		}

		binding := new(corev1.Secret)
		if err := r.Get(ctx, client.ObjectKey{Name: bindingName, Namespace: targetNs}, binding); err != nil {
			// already cleaned up, or this component was never provisioned.
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, ctrl.Result{}, err
		}
		var genSecret string
		if binding.Data != nil {
			genSecret = string(binding.Data[generatedSecretKey])
		}

		if err := r.ensureProvisionSecret(ctx, provName, env.Name, genSecret); err != nil {
			return false, ctrl.Result{}, err
		}

		instance := map[string]string{}
		if scp.Spec.InstanceSecret != nil {
			secret := new(corev1.Secret)
			if err := r.Get(ctx, client.ObjectKey{Name: scp.Spec.InstanceSecret.Name, Namespace: sharedNamespace}, secret); err != nil {
				return false, ctrl.Result{}, err
			}
			for k, v := range secret.Data {
				instance[k] = string(v)
			}
		}

		opts, err := renderProvisionOptions(env, component, sc, scp.Spec.Deprovision, genSecret, instance)
		if err != nil {
			return false, ctrl.Result{}, err
		}
		state, err := r.Provisioner.ObserveDeprovision(ctx, opts)
		if err != nil {
			return false, ctrl.Result{}, err
		}

		switch state.Phase {
		case deployer.SucceededJobPhase:
			if err := r.deleteJob(ctx, provisioner.ProvisionJobName(env.Name, component.Name), sharedNamespace); err != nil {
				return false, ctrl.Result{}, err
			}
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
			if err := r.Provisioner.SubmitDeprovision(ctx, opts); err != nil {
				return false, ctrl.Result{}, err
			}
			return false, ctrl.Result{RequeueAfter: requeueAfter}, nil

		case deployer.RunningJobPhase:
			return false, ctrl.Result{RequeueAfter: requeueAfter}, nil
		}
	}

	return true, ctrl.Result{}, nil
}

func (r *EphemeralEnvironmentReconciler) ensureDeployerRoleBinding(ctx context.Context, targetNs string) error {
	name := cmp.Or(r.DeployerServiceAccount, deployerRoleBinding)
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: targetNs},
	}
	if err := r.Create(ctx, sa); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}

	rb := &rbacv1.RoleBinding{}
	err := r.Get(ctx, client.ObjectKey{Namespace: targetNs, Name: name}, rb)
	if err == nil {
		return nil
	}

	if !apierrors.IsNotFound(err) {
		return err
	}

	rb = &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: targetNs},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     "admin",
		},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      name,
			Namespace: targetNs,
		}},
	}

	return r.Create(ctx, rb)
}

func (r *EphemeralEnvironmentReconciler) createNamespace(ctx context.Context, env *v1alpha1.EphemeralEnvironment) error {
	targetNs := env.Status.TargetNamespace
	ns := &corev1.Namespace{}
	err := r.namespaceReader().Get(ctx, client.ObjectKey{Name: targetNs}, ns)
	if err == nil {
		if !ownsNamespace(env, ns) {
			return fmt.Errorf("%w: %q", ErrNamespaceNotManaged, targetNs)
		}
		if !ns.DeletionTimestamp.IsZero() {
			return fmt.Errorf("namespace %q is terminating", targetNs)
		}
		return r.ensureDeployerRoleBinding(ctx, targetNs)
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	if meta.IsStatusConditionTrue(env.Status.Conditions, namespaceBound) {
		return fmt.Errorf("bound namespace %q disappeared; refusing to recreate it and lose cleanup evidence", targetNs)
	}
	ns = &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: targetNs,
			Labels: map[string]string{
				managedLabel:  "true",
				ownerUIDLabel: string(env.UID),
			},
		},
	}

	err = r.Create(ctx, ns)
	if apierrors.IsAlreadyExists(err) {
		if err := r.namespaceReader().Get(ctx, client.ObjectKey{Name: targetNs}, ns); err != nil {
			return err
		}
		if !ownsNamespace(env, ns) {
			return fmt.Errorf("%w: %q", ErrNamespaceNotManaged, targetNs)
		}
		if !ns.DeletionTimestamp.IsZero() {
			return fmt.Errorf("namespace %q is terminating", targetNs)
		}
		err = nil
	}
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
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(env.UID)))
	for n := 8; n <= 52; n += 4 {
		if env.UID != "" && env.Status.TargetNamespace == nsPrefix+digest[:n] {
			return env.Status.TargetNamespace, nil
		}
	}
	return "", fmt.Errorf("invalid targetNamespace %q for environment UID %q", env.Status.TargetNamespace, env.UID)
}

func (r *EphemeralEnvironmentReconciler) namespaceReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func ownsNamespace(env *v1alpha1.EphemeralEnvironment, ns *corev1.Namespace) bool {
	return env.UID != "" && ns.Labels[managedLabel] == "true" && ns.Labels[ownerUIDLabel] == string(env.UID)
}

func (r *EphemeralEnvironmentReconciler) allocateNamespace(ctx context.Context, env *v1alpha1.EphemeralEnvironment, start int) error {
	if env.UID == "" {
		return errors.New("cannot allocate namespace without environment UID")
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(env.UID)))
	// magic numbers explanation: check if we exceed the length that was set by DNS-1035.
	for n := start; n <= 52; n += 4 {
		name := nsPrefix + digest[:n]
		ns := new(corev1.Namespace)
		err := r.Get(ctx, client.ObjectKey{Name: name}, ns)
		if apierrors.IsNotFound(err) || (err == nil && ownsNamespace(env, ns)) {
			env.Status.TargetNamespace = name
			return nil
		}
		if err != nil {
			return err
		}
	}

	return errors.New("all deterministic namespace prefixes through 52 hex characters are occupied")
}

func (r *EphemeralEnvironmentReconciler) removeFinalizer(ctx context.Context, env *v1alpha1.EphemeralEnvironment) error {
	patch := client.MergeFrom(env.DeepCopy())
	controllerutil.RemoveFinalizer(env, finalizer)
	return r.Patch(ctx, env, patch)
}

func (r *EphemeralEnvironmentReconciler) SetupWithManager(mgr ctrl.Manager, rl RateLimitOptions) error {
	r.APIReader = mgr.GetAPIReader()
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.EphemeralEnvironment{}).
		Named("ephemeralenvironment").
		WithOptions(rl.controllerOptions()).
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
	if env.Status.Phase != v1alpha1.EnvironmentPhaseFailed {
		logf.Log.WithName("ephemeralenvironment").Info("environment failed",
			"name", env.Name, "reason", reason, "message", message)
	}
	env.Status.Phase = v1alpha1.EnvironmentPhaseFailed
	return nil
}
