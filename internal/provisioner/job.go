package provisioner

import (
	"context"
	"time"

	"github.com/petri-dev/petri-operator/internal/deployer"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	sharedNamespace = "petri-shared"
	OpProvision     = "provision"
	OpDeprovision   = "deprovision"
)

func jobName(op, envName, componentName string) string {
	return deployer.TruncateName("petri-" + op + "-" + envName + "-" + componentName)
}

func ProvisionJobName(envName, componentName string) string {
	return jobName(OpProvision, envName, componentName)
}

func DeprovisionJobName(envName, componentName string) string {
	return jobName(OpDeprovision, envName, componentName)
}

func labels(opts ProvisionOptions, op string) map[string]string {
	return map[string]string{
		"petri.run/managed": "true",
		"petri.run/env":     opts.EnvName,
		"petri.run/shared":  opts.SharedName,
		"petri.run/op":      op,
	}
}

type JobProvisioner struct {
	Client   client.Client
	Reader   client.Reader
	Deadline time.Duration
}

func (p *JobProvisioner) SubmitProvision(ctx context.Context, opts ProvisionOptions) error {
	return p.submit(ctx, opts, OpProvision)
}
func (p *JobProvisioner) SubmitDeprovision(ctx context.Context, opts ProvisionOptions) error {
	return p.submit(ctx, opts, OpDeprovision)
}
func (p *JobProvisioner) ObserveProvision(ctx context.Context, opts ProvisionOptions) (deployer.JobState, error) {
	return p.observe(ctx, opts, OpProvision)
}
func (p *JobProvisioner) ObserveDeprovision(ctx context.Context, opts ProvisionOptions) (deployer.JobState, error) {
	return p.observe(ctx, opts, OpDeprovision)
}

func (p *JobProvisioner) submit(ctx context.Context, opts ProvisionOptions, op string) error {
	name := jobName(op, opts.EnvName, opts.ComponentName)

	existing := &batchv1.Job{}
	failed := false
	err := p.Client.Get(ctx, client.ObjectKey{Namespace: sharedNamespace, Name: name}, existing)
	if err == nil {
		for _, c := range existing.Status.Conditions {
			if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
				failed = true
				if delErr := p.Client.Delete(ctx, existing, client.PropagationPolicy(metav1.DeletePropagationBackground)); delErr != nil && !apierrors.IsNotFound(delErr) {
					return delErr
				}
				break
			}
		}

		if !failed {
			return nil
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	return p.Client.Create(ctx, p.buildJob(opts, op, name))
}

func (p *JobProvisioner) observe(ctx context.Context, opts ProvisionOptions, op string) (deployer.JobState, error) {
	name := jobName(op, opts.EnvName, opts.ComponentName)

	job := &batchv1.Job{}
	err := p.Client.Get(ctx, client.ObjectKey{Namespace: sharedNamespace, Name: name}, job)
	if apierrors.IsNotFound(err) {
		return deployer.JobState{Phase: deployer.PendingJobPhase}, nil
	}
	if err != nil {
		return deployer.JobState{}, err
	}

	if job.Status.Succeeded > 0 {
		return deployer.JobState{Phase: deployer.SucceededJobPhase}, nil
	}
	if isFailed(job) {
		return deployer.JobState{Phase: deployer.FailedJobPhase, Reason: p.failureReason(ctx, name)}, nil
	}
	return deployer.JobState{Phase: deployer.RunningJobPhase}, nil
}

func isFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (p *JobProvisioner) failureReason(ctx context.Context, name string) string {
	pods := &corev1.PodList{}
	if err := p.Reader.List(ctx, pods, client.InNamespace(sharedNamespace), client.MatchingLabels{"job-name": name}); err != nil {
		return "provision job failed (could not read pod: " + err.Error() + ")"
	}
	for _, pod := range pods.Items {
		for _, cs := range pod.Status.ContainerStatuses {
			if t := cs.State.Terminated; t != nil && t.Message != "" {
				return t.Message
			}
		}
	}
	return "provision job failed"
}

func (p *JobProvisioner) buildJob(opts ProvisionOptions, op, name string) *batchv1.Job {
	backoff := int32(0)
	l := labels(opts, op)

	envFrom := append([]corev1.EnvFromSource{}, opts.Script.EnvFrom...)
	envFrom = append(envFrom, corev1.EnvFromSource{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: opts.ProvisionerSecretRef},
		},
	})

	command := opts.Script.Command
	if len(command) == 0 {
		command = []string{"/bin/sh", "-c", opts.Script.Script}
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: sharedNamespace, Labels: l},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: l},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Volumes:       opts.Script.Volumes,
					Containers: []corev1.Container{{
						Name:         "provisioner",
						Image:        opts.Script.Image,
						Command:      command,
						Env:          opts.Script.Env,
						EnvFrom:      envFrom,
						VolumeMounts: opts.Script.VolumeMounts,
					}},
				},
			},
		},
	}

	if p.Deadline > 0 {
		secs := int64(p.Deadline.Seconds())
		job.Spec.ActiveDeadlineSeconds = &secs
	}

	return job
}
