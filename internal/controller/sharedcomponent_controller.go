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
	"strconv"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/petri-dev/petri-operator/internal/deployer"
	"github.com/petri-dev/petri-operator/internal/helpers"
	"github.com/petri-dev/petri-operator/internal/secretgen"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	sharedFinalizer = "petri.run/shared-cleanup"
	sharedNamespace = "petri-shared"
)

// SharedComponentReconciler reconciles a SharedComponent object.
type SharedComponentReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Deployer deployer.Deployer
}

// +kubebuilder:rbac:groups=core.petri.run,resources=sharedcomponents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.petri.run,resources=sharedcomponents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.petri.run,resources=sharedcomponents/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.petri.run,resources=sharedcomponentproviders,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,verbs=bind,resourceNames=admin
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *SharedComponentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	sc := new(v1alpha1.SharedComponent)

	if err := r.Get(ctx, req.NamespacedName, sc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !sc.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, sc)
	}

	if !controllerutil.ContainsFinalizer(sc, sharedFinalizer) {
		patch := client.MergeFrom(sc.DeepCopy())
		controllerutil.AddFinalizer(sc, sharedFinalizer)
		return ctrl.Result{}, r.Patch(ctx, sc, patch)
	}

	return r.reconcile(ctx, sc)
}

func (r *SharedComponentReconciler) reconcile(ctx context.Context, sc *v1alpha1.SharedComponent) (res ctrl.Result, err error) {
	log := logf.FromContext(ctx)

	patcher := helpers.NewStatusPatcher(r.Client, sc)
	defer func() {
		err = errors.Join(err, patcher.Patch(ctx, sc))
	}()

	scp := new(v1alpha1.SharedComponentProvider)
	err = r.Get(ctx, client.ObjectKey{Name: sc.Spec.Provider, Namespace: sc.Namespace}, scp)
	if apierrors.IsNotFound(err) {
		sc.Status.Ready = false
		meta.SetStatusCondition(&sc.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "ProviderNotFound",
			Message:            "provider" + sc.Spec.Provider + " not found",
			ObservedGeneration: sc.Generation,
		})
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if scp.Spec.Helm == nil {
		sc.Status.Ready = false
		meta.SetStatusCondition(&sc.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "ProviderNoHelm",
			Message:            "no helm provided for " + sc.Spec.Provider,
			ObservedGeneration: sc.Generation,
		})
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	if err := r.createSharedNamespace(ctx, sharedNamespace); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.ensureInstanceSecret(ctx, scp.Spec.InstanceSecret); err != nil {
		return ctrl.Result{}, err
	}

	opts := deployer.DeployOptions{
		Namespace:   sharedNamespace,
		ReleaseName: "shared-" + sc.Name,
		Component:   v1alpha1.ComponentSpec{Name: sc.Name, Helm: scp.Spec.Helm},
	}

	state, err := r.Deployer.Observe(ctx, opts)
	if err != nil {
		return ctrl.Result{}, err
	}

	switch state.Phase {
	case deployer.PendingJobPhase:
		if err := r.Deployer.Submit(ctx, opts); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("shared instance deploy submitted", "release", opts.ReleaseName)
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	case deployer.RunningJobPhase:
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	case deployer.FailedJobPhase:
		sc.Status.Ready = false
		meta.SetStatusCondition(&sc.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "DeployFailed",
			Message:            state.Reason,
			ObservedGeneration: sc.Generation,
		})
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	case deployer.SucceededJobPhase:
		deployJobName := deployer.TruncateName("petri-deploy-shared-" + sc.Name)
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: deployJobName, Namespace: sharedNamespace}}
		if err := client.IgnoreNotFound(r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))); err != nil {
			log.Error(err, "failed to delete deploy job", "job", deployJobName)
		}
		sc.Status.Ready = true
		meta.SetStatusCondition(&sc.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "InstanceReady",
			Message:            "shared instance deployed",
			ObservedGeneration: sc.Generation,
		})
	}

	sc.Status.Consumers = r.countConsumers(ctx, sc.Name)
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// ensureInstanceSecret creates or updates the instance-level Secret in petri-shared.
// in Generate keys are stable (never rotated once written). In Value keys always reflect the provider spec.
func (r *SharedComponentReconciler) ensureInstanceSecret(ctx context.Context, spec *v1alpha1.InstanceSecret) error {
	if spec == nil {
		return nil
	}

	existing := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Namespace: sharedNamespace, Name: spec.Name}, existing)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	found := err == nil

	data := map[string][]byte{}
	for key, k := range spec.Keys {
		if k.Generate != nil {
			if found {
				if v, ok := existing.Data[key]; ok {
					data[key] = v
					continue
				}
			}
			v, err := secretgen.Random(k.Generate.Length, k.Generate.Charset)
			if err != nil {
				return err
			}
			data[key] = []byte(v)
			continue
		}
		data[key] = []byte(k.Value)
	}

	if found {
		existing.Data = data
		return r.Update(ctx, existing)
	}
	return r.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: spec.Name, Namespace: sharedNamespace},
		Data:       data,
	})
}

func (r *SharedComponentReconciler) countConsumers(ctx context.Context, name string) int {
	nsList := &corev1.NamespaceList{}
	if err := r.List(ctx, nsList, client.MatchingLabels{sharedLabel(name): "true"}); err != nil {
		return 0
	}
	return len(nsList.Items)
}

const uninstallRetriesAnnotation = "petri.run/uninstall-retries"

func (r *SharedComponentReconciler) reconcileDelete(ctx context.Context, sc *v1alpha1.SharedComponent) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(sc, sharedFinalizer) {
		return ctrl.Result{}, nil
	}

	if consumers := r.countConsumers(ctx, sc.Name); consumers > 0 {
		log.Info("shared component still in use, deferring teardown", "consumers", consumers)
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	opts := deployer.DeployOptions{
		Namespace:   sharedNamespace,
		ReleaseName: "shared-" + sc.Name,
	}

	state, err := r.Deployer.ObserveUndeploy(ctx, opts)
	if err != nil {
		return ctrl.Result{}, err
	}

	switch state.Phase {
	case deployer.PendingJobPhase:
		if err := r.Deployer.SubmitUndeploy(ctx, opts); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	case deployer.FailedJobPhase:
		retries, _ := strconv.Atoi(sc.Annotations[uninstallRetriesAnnotation])
		retries++
		log.Info("shared component uninstall job failed", "retries", retries, "maxRetries", maxDeployRetries)
		if retries >= maxDeployRetries {
			log.Error(errors.New(state.Reason), "uninstall exhausted retries, forcing finalizer removal", "component", sc.Name)
			patch := client.MergeFrom(sc.DeepCopy())
			controllerutil.RemoveFinalizer(sc, sharedFinalizer)
			return ctrl.Result{}, r.Patch(ctx, sc, patch)
		}
		patch := client.MergeFrom(sc.DeepCopy())
		if sc.Annotations == nil {
			sc.Annotations = map[string]string{}
		}
		sc.Annotations[uninstallRetriesAnnotation] = strconv.Itoa(retries)
		if err := r.Patch(ctx, sc, patch); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Deployer.SubmitUndeploy(ctx, opts); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	case deployer.RunningJobPhase:
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	case deployer.SucceededJobPhase:
		patch := client.MergeFrom(sc.DeepCopy())
		controllerutil.RemoveFinalizer(sc, sharedFinalizer)
		return ctrl.Result{}, r.Patch(ctx, sc, patch)
	}

	return ctrl.Result{}, nil
}

func (r *SharedComponentReconciler) createSharedNamespace(ctx context.Context, targetNs string) error {
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
			Name:   targetNs,
			Labels: map[string]string{managedLabel: "true"},
		},
	}
	if err := r.Create(ctx, ns); err != nil {
		return fmt.Errorf("failed to create shared namespace: %w", err)
	}
	return r.ensureDeployerRoleBinding(ctx, targetNs)
}

func (r *SharedComponentReconciler) ensureDeployerRoleBinding(ctx context.Context, targetNs string) error {
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

	return r.Create(ctx, &rbacv1.RoleBinding{
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
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *SharedComponentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.SharedComponent{}).
		Named("sharedcomponent").
		Complete(r)
}
