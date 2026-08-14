package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/nuromirg/petri/api/v1alpha1"
	"github.com/nuromirg/petri/internal/deployer"
	"github.com/nuromirg/petri/internal/provisioner"
	"github.com/nuromirg/petri/internal/renderer"
	"github.com/nuromirg/petri/internal/secretgen"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const generatedSecretKey = "petri.generated-secret"

var (
	errSharedNotReady = errors.New("shared instance not ready")
	errAtCapacity     = errors.New("shared component at capacity")
)

func (r *EphemeralEnvironmentReconciler) acquireProvisionLease(ctx context.Context, scName, scNamespace, holderEnvName string) (acquired bool, err error) {
	leaseDuration := int32(30)
	now := metav1.NewMicroTime(metav1.Now().Time)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "petri-provision-" + scName,
			Namespace: scNamespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holderEnvName,
			LeaseDurationSeconds: &leaseDuration,
			AcquireTime:          &now,
			RenewTime:            &now,
		},
	}

	if err := r.Create(ctx, lease); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (r *EphemeralEnvironmentReconciler) releaseProvisionLease(ctx context.Context, scName, scNamespace string) {
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "petri-provision-" + scName,
			Namespace: scNamespace,
		},
	}
	if err := client.IgnoreNotFound(r.Delete(ctx, lease)); err != nil {
		logf.FromContext(ctx).Error(err, "failed to release provision lease, TTL will expire it", "lease", "petri-provision-"+scName)
	}
}

func (r *EphemeralEnvironmentReconciler) submitShared(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, component v1alpha1.ComponentSpec) error {
	sc := new(v1alpha1.SharedComponent)
	if err := r.Get(ctx, client.ObjectKey{Name: component.SharedComponentRef, Namespace: env.Namespace}, sc); err != nil {
		return fmt.Errorf("get shared component %q: %w", component.SharedComponentRef, err)
	}

	scp := new(v1alpha1.SharedComponentProvider)
	if err := r.Get(ctx, client.ObjectKey{Name: sc.Spec.Provider, Namespace: env.Namespace}, scp); err != nil {
		return fmt.Errorf("got provider %q: %w", sc.Spec.Provider, err)
	}

	if !sc.Status.Ready {
		return errSharedNotReady
	}

	if sc.Spec.MaxConsumers > 0 {
		already, err := r.isConsumer(ctx, targetNs, sc.Name)
		if err != nil {
			return err
		}
		if !already {
			acquired, err := r.acquireProvisionLease(ctx, sc.Name, sc.Namespace, env.Name)
			if err != nil {
				return err
			}
			if !acquired {
				return errSharedNotReady
			}
			defer r.releaseProvisionLease(ctx, sc.Name, sc.Namespace)

			nsList := &corev1.NamespaceList{}
			if err := r.List(ctx, nsList, client.MatchingLabels{sharedLabel(sc.Name): "true"}); err != nil {
				return err
			}
			if len(nsList.Items) >= int(sc.Spec.MaxConsumers) {
				return errAtCapacity
			}
		}
	}

	bindingName := env.Name + "-" + component.Name + "-binding"

	binding := new(corev1.Secret)
	err := r.Get(ctx, client.ObjectKey{Name: bindingName, Namespace: targetNs}, binding)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get binding secret: %w", err)
	}

	var genSecret string
	if err == nil {
		genSecret = string(binding.Data[generatedSecretKey])
	} else {
		genSecret, err = secretgen.Random(24, "alphanumeric")
		if err != nil {
			return err
		}

		instance := map[string]string{}
		if scp.Spec.InstanceSecret != nil {
			is := new(corev1.Secret)
			if err := r.Get(ctx, client.ObjectKey{Name: scp.Spec.InstanceSecret.Name, Namespace: sharedNamespace}, is); err == nil {
				for k, v := range is.Data {
					instance[k] = string(v)
				}
			}
		}

		vars := renderer.Vars{Env: renderer.EnvVarsFor(env.Name, genSecret), Instance: instance}
		data := map[string][]byte{generatedSecretKey: []byte(genSecret)}
		if scp.Spec.Binding != nil {
			rendered, err := renderer.RenderMap(scp.Spec.Binding.SecretKeys, vars)
			if err != nil {
				return fmt.Errorf("render binding keys: %w", err)
			}
			for k, v := range rendered {
				data[k] = []byte(v)
			}
		}

		if err := r.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: bindingName, Namespace: targetNs},
			Data:       data,
		}); err != nil {
			return fmt.Errorf("create binding secret: %w", err)
		}
	}

	if err := r.labelConsumer(ctx, targetNs, sc.Name); err != nil {
		return err
	}

	if scp.Spec.Provision == nil {
		setComponentPhase(env, component.Name, v1alpha1.PhaseReady)
		setComponentShared(env, component.Name)
		return nil
	}

	provName := "shared-" + sc.Name + "-provision-" + env.Name
	if err := r.ensureProvisionSecret(ctx, provName, env.Name, genSecret); err != nil {
		return err
	}

	script := *scp.Spec.Provision
	renderVars := renderer.Vars{Env: renderer.EnvVarsFor(env.Name, genSecret)}
	if script.Script != "" {
		rendered, err := renderer.Render(script.Script, renderVars)
		if err != nil {
			return fmt.Errorf("render provision script: %w", err)
		}
		script.Script = rendered
	}
	for i, c := range script.Command {
		rendered, err := renderer.Render(c, renderVars)
		if err != nil {
			return fmt.Errorf("render provision command[%d]: %w", i, err)
		}
		script.Command[i] = rendered
	}

	if err := r.Provisioner.SubmitProvision(ctx, provisioner.ProvisionOptions{
		EnvName:              env.Name,
		ComponentName:        component.Name,
		SharedName:           sc.Name,
		Script:               script,
		ProvisionerSecretRef: provName,
	}); err != nil {
		return err
	}

	setComponentPhase(env, component.Name, v1alpha1.PhaseSubmitting)
	setComponentShared(env, component.Name)
	return nil
}

func (r *EphemeralEnvironmentReconciler) isConsumer(ctx context.Context, targetNs, sharedName string) (bool, error) {
	ns := new(corev1.Namespace)
	if err := r.Get(ctx, client.ObjectKey{Name: targetNs}, ns); err != nil {
		return false, err
	}

	_, ok := ns.Labels[sharedLabel(sharedName)]
	return ok, nil
}

func (r *EphemeralEnvironmentReconciler) labelConsumer(ctx context.Context, targetNs, sharedName string) error {
	ns := new(corev1.Namespace)
	if err := r.Get(ctx, client.ObjectKey{Name: targetNs}, ns); err != nil {
		return err
	}
	label := sharedLabel(sharedName)
	if ns.Labels[label] == "true" {
		return nil
	}
	patch := client.MergeFrom(ns.DeepCopy())
	if ns.Labels == nil {
		ns.Labels = map[string]string{}
	}
	ns.Labels[label] = "true"
	return r.Patch(ctx, ns, patch)
}

func (r *EphemeralEnvironmentReconciler) ensureProvisionSecret(ctx context.Context, name, envName, genSecret string) error {
	data := map[string][]byte{
		"PETRI_ENV_NAME":         []byte(envName),
		"PETRI_GENERATED_SECRET": []byte(genSecret),
	}
	existing := new(corev1.Secret)
	err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: sharedNamespace}, existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: sharedNamespace},
			Data:       data,
		})
	}
	if err != nil {
		return err
	}

	existing.Data = data
	return r.Update(ctx, existing)
}

func (r *EphemeralEnvironmentReconciler) bindingComponents(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, template *v1alpha1.EnvironmentTemplate) (map[string]map[string]string, error) {
	out := map[string]map[string]string{}
	for _, c := range template.Spec.Components {
		if c.SharedComponentRef == "" {
			continue
		}

		secret := new(corev1.Secret)
		err := r.Get(ctx, client.ObjectKey{Name: env.Name + "-" + c.Name + "-binding", Namespace: targetNs}, secret)
		if apierrors.IsNotFound(err) {
			continue // not provisioned yet, consumer will wait with dependsOn
		}
		if err != nil {
			return nil, err
		}

		m := map[string]string{}
		for k, v := range secret.Data {
			if k == generatedSecretKey {
				continue
			}
			m[k] = string(v)
		}
		out[c.Name] = m
	}

	return out, nil
}

func renderConsumerValues(env *v1alpha1.EphemeralEnvironment, c v1alpha1.ComponentSpec, components map[string]map[string]string) (v1alpha1.ComponentSpec, error) {
	if c.Helm == nil {
		return c, nil
	}

	out := c.DeepCopy()
	if out.Helm.Values == nil {
		out.Helm.Values = map[string]string{}
	}
	vars := renderer.Vars{
		Env:        renderer.EnvVarsFor(env.Name, ""),
		Components: components,
	}

	rendered, err := renderer.RenderMap(out.Helm.Values, vars)
	if err != nil {
		return c, fmt.Errorf("render helm values for %s: %w", c.Name, err)
	}
	out.Helm.Values = rendered

	for _, ev := range c.Env {
		if ev.SecretKeyRef != nil {
			bindingName := env.Name + "-" + ev.SecretKeyRef.Component + "-binding"
			out.Helm.Values["extraEnvVarsSecret"] = bindingName
		}
	}

	return *out, nil
}

func (r *EphemeralEnvironmentReconciler) observeShared(ctx context.Context, env *v1alpha1.EphemeralEnvironment, component v1alpha1.ComponentSpec) (done bool, err error) {
	sc := new(v1alpha1.SharedComponent)
	if err := r.Get(ctx, client.ObjectKey{Name: component.SharedComponentRef, Namespace: env.Namespace}, sc); err != nil {
		return false, err
	}

	state, err := r.Provisioner.ObserveProvision(ctx, provisioner.ProvisionOptions{
		EnvName:       env.Name,
		ComponentName: component.Name,
		SharedName:    sc.Name,
	})
	if err != nil {
		return false, err
	}

	switch state.Phase {
	case deployer.SucceededJobPhase:
		provName := "shared-" + sc.Name + "-provision-" + env.Name
		provJobName := provisioner.ProvisionJobName(env.Name, component.Name)
		if err := r.deleteJob(ctx, provJobName, sharedNamespace); err != nil {
			return false, err
		}
		if err := r.deleteSecret(ctx, provName, sharedNamespace); err != nil {
			return false, err
		}
		setComponentPhase(env, component.Name, v1alpha1.PhaseReady)
		resetComponentFailure(env, component.Name)
		return true, nil

	case deployer.FailedJobPhase:
		if recordRuntimeFailure(env, component.Name, "provision: "+state.Reason) {
			setComponentPhase(env, component.Name, v1alpha1.PhaseFailed)
			return false, r.setFailed(env, "ProvisionFailed", component.Name+": "+state.Reason)
		}
		return false, nil

	default:
		return false, nil
	}
}

func (r *EphemeralEnvironmentReconciler) deleteSecret(ctx context.Context, name, ns string) error {
	err := r.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
	return client.IgnoreNotFound(err)
}

func (r *EphemeralEnvironmentReconciler) deleteJob(ctx context.Context, name, ns string) error {
	err := r.Delete(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}},
		client.PropagationPolicy(metav1.DeletePropagationBackground))
	return client.IgnoreNotFound(err)
}

func sharedLabel(name string) string {
	return "petri.run/shared-" + name
}
