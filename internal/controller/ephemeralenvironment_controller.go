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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/nuromirg/petri/api/v1alpha1"
	"github.com/nuromirg/petri/internal/deployer"
	"github.com/nuromirg/petri/internal/helpers"
)

const (
	finalizer    = "petri.run/cleanup"
	managedLabel = "petri.run/managed"

	helmMaxReleaseNameLength = 53

	nsPrefix = "petri-"
)

var ErrNamespaceNotManaged = errors.New("namespace not managed by Petri")

// EphemeralEnvironmentReconciler reconciles a EphemeralEnvironment object
type EphemeralEnvironmentReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Deployer deployer.Deployer
}

// +kubebuilder:rbac:groups=core.petri.run,resources=ephemeralenvironments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.petri.run,resources=ephemeralenvironments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.petri.run,resources=ephemeralenvironments/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.petri.run,resources=environmenttemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;create;delete
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
	patcher := helpers.NewStatusPatcher(r.Client, env)
	defer func() {
		err = errors.Join(err, patcher.Patch(ctx, env))
	}()

	template, err := r.getEnvironmentTemplate(ctx, env)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return r.setFailed(ctx, env, "TemplateNotFound", "template "+env.Spec.Template+" not found")
		}
		return ctrl.Result{}, err
	}

	targetNs, err := r.targetNamespace(env)
	if err != nil {
		return r.setFailed(ctx, env, "InvalidConfiguration", err.Error())
	}

	if err := r.createNamespace(ctx, targetNs); err != nil {
		if errors.Is(err, ErrNamespaceNotManaged) {
			return r.setFailed(ctx, env, "NamespaceNotManaged", err.Error())
		}

		return ctrl.Result{}, err
	}

	env.Status.Phase = v1alpha1.PhaseDeploying
	// if err := r.Status().Update(ctx, env); err != nil {
	// 	return ctrl.Result{}, err
	// }

	for _, component := range template.Spec.Components {
		releaseName := env.Name + "-" + component.Name
		if len(releaseName) > helmMaxReleaseNameLength {
			return r.setFailed(ctx, env, "InvalidConfiguration", fmt.Sprintf("release name %q exceeds 53 characters", releaseName))
		}

		// TODO: design a dependsOn deploy ordering
		if err := r.Deployer.Deploy(ctx, deployer.DeployOptions{
			Namespace:   targetNs,
			ReleaseName: env.Name + "-" + component.Name,
			Component:   component,
		}); err != nil {
			return r.setFailed(ctx, env, "DeployFailed", err.Error())
		}
	}

	// TODO also update the status.URL field with domain
	env.Status.Phase = v1alpha1.PhaseReady
	// return ctrl.Result{}, r.Status().Update(ctx, env)
	return ctrl.Result{}, nil
}

func (r *EphemeralEnvironmentReconciler) createNamespace(ctx context.Context, targetNs string) error {
	ns := &corev1.Namespace{}
	err := r.Get(ctx, client.ObjectKey{Name: targetNs}, ns)
	if err == nil {
		if _, managed := ns.Labels[managedLabel]; !managed {
			return fmt.Errorf("%w: %q", ErrNamespaceNotManaged, targetNs)
		}
		return nil
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

	return r.Create(ctx, ns)
}

func (r *EphemeralEnvironmentReconciler) reconcileDelete(ctx context.Context, env *v1alpha1.EphemeralEnvironment) (ctrl.Result, error) {
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

	if template != nil {
		undeployErrs := make([]error, 0)
		for _, component := range template.Spec.Components {
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

func (r *EphemeralEnvironmentReconciler) setFailed(ctx context.Context, env *v1alpha1.EphemeralEnvironment, reason, message string) (ctrl.Result, error) {
	_ = ctx
	meta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: env.Generation,
	})
	env.Status.Phase = v1alpha1.PhaseFailed
	// return ctrl.Result{}, r.Status().Update(ctx, env)
	return ctrl.Result{}, nil
}
