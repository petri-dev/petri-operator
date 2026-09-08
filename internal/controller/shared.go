package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/petri-dev/petri-operator/internal/deployer"
	"github.com/petri-dev/petri-operator/internal/provisioner"
	"github.com/petri-dev/petri-operator/internal/renderer"
	"github.com/petri-dev/petri-operator/internal/secretgen"
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

func acquireProvisionLease(ctx context.Context, c client.Client, reader client.Reader, scName, scNamespace, holder string) (*coordinationv1.Lease, error) {
	leaseDuration := int32(30)
	now := metav1.NewMicroTime(metav1.Now().Time)
	leaseKey := client.ObjectKey{Name: "petri-provision-" + scName, Namespace: scNamespace}

	existing := &coordinationv1.Lease{}
	if err := reader.Get(ctx, leaseKey, existing); err == nil {
		if existing.Spec.LeaseDurationSeconds != nil && existing.Spec.RenewTime != nil {
			expiry := existing.Spec.RenewTime.Add(time.Duration(*existing.Spec.LeaseDurationSeconds) * time.Second)
			if metav1.Now().After(expiry) {
				if delErr := c.Delete(ctx, existing, client.Preconditions{UID: &existing.UID, ResourceVersion: &existing.ResourceVersion}); delErr != nil && !apierrors.IsNotFound(delErr) {
					return nil, delErr
				}
			} else {
				return nil, nil
			}
		} else {
			return nil, nil
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "petri-provision-" + scName,
			Namespace: scNamespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &leaseDuration,
			AcquireTime:          &now,
			RenewTime:            &now,
		},
	}

	if err := c.Create(ctx, lease); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, nil
		}
		return nil, err
	}

	return lease, nil
}

func releaseProvisionLease(ctx context.Context, c client.Client, lease *coordinationv1.Lease) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := client.IgnoreNotFound(c.Delete(ctx, lease, client.Preconditions{UID: &lease.UID, ResourceVersion: &lease.ResourceVersion})); err != nil {
		logf.FromContext(ctx).Error(err, "failed to release provision lease, TTL will expire it", "lease", lease.Name)
	}
}

func (r *EphemeralEnvironmentReconciler) registerConsumer(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, sc *v1alpha1.SharedComponent) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	reader := r.namespaceReader()
	key := client.ObjectKeyFromObject(sc)
	lease, err := acquireProvisionLease(ctx, r.Client, reader, sc.Name, sc.Namespace, env.Name)
	if err != nil {
		return err
	}

	if lease == nil {
		return errSharedNotReady
	}
	defer releaseProvisionLease(ctx, r.Client, lease)

	if err := reader.Get(ctx, key, sc); err != nil {
		return err
	}
	if !sc.DeletionTimestamp.IsZero() || !sc.Status.Ready {
		return errSharedNotReady
	}
	if sc.Spec.MaxConsumers > 0 {
		already, err := r.isConsumer(ctx, targetNs, sc.Name)
		if err != nil {
			return err
		}
		if !already {
			nsList := &corev1.NamespaceList{}
			if err := reader.List(ctx, nsList, client.MatchingLabels{sharedLabel(sc.Name): "true"}); err != nil {
				return err
			}
			if len(nsList.Items) >= int(sc.Spec.MaxConsumers) {
				return errAtCapacity
			}
		}
	}
	uid := sc.UID
	if err := r.labelConsumer(ctx, targetNs, sc.Name); err != nil {
		return err
	}

	if err := reader.Get(ctx, key, sc); err != nil {
		return err
	}
	if sc.UID != uid || !sc.DeletionTimestamp.IsZero() || !sc.Status.Ready {
		return errSharedNotReady
	}
	return ctx.Err()
}

func (r *EphemeralEnvironmentReconciler) submitShared(ctx context.Context, env *v1alpha1.EphemeralEnvironment, targetNs string, component v1alpha1.ComponentSpec) (v1alpha1.ComponentPhase, error) {
	sc := &v1alpha1.SharedComponent{ObjectMeta: metav1.ObjectMeta{Name: component.SharedComponentRef, Namespace: env.Namespace}}
	if err := r.registerConsumer(ctx, env, targetNs, sc); err != nil {
		return "", err
	}
	scp := new(v1alpha1.SharedComponentProvider)
	if err := r.Get(ctx, client.ObjectKey{Name: sc.Spec.Provider, Namespace: env.Namespace}, scp); err != nil {
		return "", fmt.Errorf("get provider %q: %w", sc.Spec.Provider, err)
	}

	bindingName := env.Name + "-" + component.Name + "-binding"

	binding := new(corev1.Secret)
	err := r.Get(ctx, client.ObjectKey{Name: bindingName, Namespace: targetNs}, binding)
	if err != nil && !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("get binding secret: %w", err)
	}

	instance := map[string]string{}
	if scp.Spec.InstanceSecret != nil {
		is := new(corev1.Secret)
		if err := r.Get(ctx, client.ObjectKey{Name: scp.Spec.InstanceSecret.Name, Namespace: sharedNamespace}, is); err != nil {
			return "", err
		}
		for k, v := range is.Data {
			instance[k] = string(v)
		}
	}

	var genSecret string
	if err == nil {
		genSecret = string(binding.Data[generatedSecretKey])
	} else {
		genSecret, err = secretgen.Random(24, "alphanumeric")
		if err != nil {
			return "", err
		}

		vars := renderer.Vars{Env: renderer.EnvVarsFor(env.Name, genSecret), Instance: instance}
		data := map[string][]byte{generatedSecretKey: []byte(genSecret)}
		if scp.Spec.Binding != nil {
			rendered, err := renderer.RenderMap(scp.Spec.Binding.SecretKeys, vars)
			if err != nil {
				return "", fmt.Errorf("render binding keys: %w", err)
			}
			for k, v := range rendered {
				data[k] = []byte(v)
			}
		}

		if err := r.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: bindingName, Namespace: targetNs},
			Data:       data,
		}); err != nil {
			return "", fmt.Errorf("create binding secret: %w", err)
		}
	}

	if scp.Spec.Provision == nil {
		return v1alpha1.ComponentPhaseReady, nil
	}

	if err := scp.Spec.Provision.Validate(); err != nil {
		return "", fmt.Errorf("invalid provision script: %w", err)
	}

	provName := provisioner.ProvisionJobName(env.Name, component.Name) + "-credentials"
	if err := r.ensureProvisionSecret(ctx, provName, env.Name, genSecret); err != nil {
		return "", err
	}

	opts, err := renderProvisionOptions(env, component, sc, scp.Spec.Provision, genSecret, instance)
	if err != nil {
		return "", err
	}
	if err := r.Provisioner.SubmitProvision(ctx, opts); err != nil {
		return "", err
	}

	return v1alpha1.ComponentPhaseSubmitting, nil
}

func renderProvisionOptions(env *v1alpha1.EphemeralEnvironment, component v1alpha1.ComponentSpec, sc *v1alpha1.SharedComponent, script *v1alpha1.JobScript, genSecret string, instance map[string]string) (provisioner.ProvisionOptions, error) {
	opts := provisioner.ProvisionOptions{
		EnvUID: env.UID, EnvName: env.Name, ComponentName: component.Name, SharedName: sc.Name,
		ProvisionerSecretRef: provisioner.ProvisionJobName(env.Name, component.Name) + "-credentials",
	}

	if err := script.Validate(); err != nil {
		return opts, err
	}

	opts.Script = *script.DeepCopy()
	vars := renderer.Vars{Env: renderer.EnvVarsFor(env.Name, genSecret), Instance: instance}

	var err error
	opts.Script.Script, err = renderer.Render(script.Script, vars)
	if err != nil {
		return opts, err
	}

	for i, command := range script.Command {
		opts.Script.Command[i], err = renderer.Render(command, vars)
		if err != nil {
			return opts, err
		}
	}

	return opts, nil
}

func (r *EphemeralEnvironmentReconciler) isConsumer(ctx context.Context, targetNs, sharedName string) (bool, error) {
	ns := new(corev1.Namespace)
	if err := r.namespaceReader().Get(ctx, client.ObjectKey{Name: targetNs}, ns); err != nil {
		return false, err
	}

	return ns.Labels[sharedLabel(sharedName)] == "true", nil
}

func (r *EphemeralEnvironmentReconciler) labelConsumer(ctx context.Context, targetNs, sharedName string) error {
	ns := new(corev1.Namespace)
	if err := r.namespaceReader().Get(ctx, client.ObjectKey{Name: targetNs}, ns); err != nil {
		return err
	}
	label := sharedLabel(sharedName)
	if ns.Labels[label] == "true" {
		return nil
	}
	patch := client.MergeFromWithOptions(ns.DeepCopy(), client.MergeFromWithOptimisticLock{})
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

	effectiveEnv := make(map[string]v1alpha1.EnvValue, len(c.Env)+len(env.Spec.Env))
	maps.Copy(effectiveEnv, c.Env)
	maps.Copy(effectiveEnv, env.Spec.Env)

	for _, ev := range effectiveEnv {
		if ev.SecretKeyRef != nil {
			if _, ok := components[ev.SecretKeyRef.Component]; !ok {
				return c, errSharedNotReady
			}
		}
	}

	out := c.DeepCopy()
	out.Env = effectiveEnv

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

	maps.Copy(rendered, env.Spec.Values)
	out.Helm.Values = rendered

	for _, ev := range out.Env {
		if ev.SecretKeyRef != nil {
			bindingName := env.Name + "-" + ev.SecretKeyRef.Component + "-binding"
			if existing, ok := out.Helm.Values["extraEnvVarsSecret"]; ok && existing != bindingName {
				return c, fmt.Errorf("component %s: multiple SecretKeyRef entries require different binding secrets (%q, %q); extraEnvVarsSecret only supports one", c.Name, existing, bindingName)
			}
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
	scp := new(v1alpha1.SharedComponentProvider)
	if err := r.Get(ctx, client.ObjectKey{Name: sc.Spec.Provider, Namespace: env.Namespace}, scp); err != nil {
		return false, err
	}

	if scp.Spec.Provision == nil {
		setComponentPhase(env, component.Name, v1alpha1.ComponentPhasePending)
		return false, nil
	}

	binding := new(corev1.Secret)
	if err := r.Get(ctx, client.ObjectKey{Name: env.Name + "-" + component.Name + "-binding", Namespace: env.Status.TargetNamespace}, binding); err != nil {
		return false, err
	}

	instance := map[string]string{}
	if scp.Spec.InstanceSecret != nil {
		secret := new(corev1.Secret)
		if err := r.Get(ctx, client.ObjectKey{Name: scp.Spec.InstanceSecret.Name, Namespace: sharedNamespace}, secret); err != nil {
			return false, err
		}
		for k, v := range secret.Data {
			instance[k] = string(v)
		}
	}

	opts, err := renderProvisionOptions(env, component, sc, scp.Spec.Provision, string(binding.Data[generatedSecretKey]), instance)
	if err != nil {
		return false, err
	}

	state, err := r.Provisioner.ObserveProvision(ctx, opts)
	if err != nil {
		return false, err
	}

	switch state.Phase {
	case deployer.PendingJobPhase:
		setComponentPhase(env, component.Name, v1alpha1.ComponentPhasePending)
		return false, nil
	case deployer.SucceededJobPhase:
		if err := r.deleteSecret(ctx, opts.ProvisionerSecretRef, sharedNamespace); err != nil {
			return false, err
		}
		setComponentPhase(env, component.Name, v1alpha1.ComponentPhaseReady)
		resetComponentFailure(env, component.Name)
		return true, nil

	case deployer.FailedJobPhase:
		if recordRuntimeFailure(env, component.Name, "provision: "+state.Reason) {
			setComponentPhase(env, component.Name, v1alpha1.ComponentPhaseFailed)
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
	job := new(batchv1.Job)
	if err := r.namespaceReader().Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, job); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !job.DeletionTimestamp.IsZero() {
		return fmt.Errorf("job %s is still deleting", name)
	}
	if deployer.TerminalPhase(job) != deployer.RunningJobPhase {
		return client.IgnoreNotFound(r.Delete(ctx, job, client.Preconditions{UID: &job.UID},
			client.PropagationPolicy(metav1.DeletePropagationForeground)))
	}

	return fmt.Errorf("refusing to delete active job %s", name)
}

func sharedLabel(name string) string {
	return "petri.run/shared-" + name
}
