package deployer

import (
	"context"
	"encoding/json"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func jobName(op, release string) string {
	return TruncateName("petri-" + op + "-" + release)
}

func jobLabels(opts DeployOptions, op string) map[string]string {
	return map[string]string{
		"petri.run/managed": "true",
		"petri.run/release": opts.ReleaseName,
		"petri.run/op":      op,
	}
}

type JobDeployer struct {
	Client         client.Client
	Reader         client.Reader
	Image          string
	ServiceAccount string
	Deadline       time.Duration
}

func (j *JobDeployer) submit(ctx context.Context, opts DeployOptions, op string) error {
	specJSON, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	existing := &batchv1.Job{}
	failed := false
	err = j.Client.Get(ctx, client.ObjectKey{Namespace: opts.Namespace, Name: jobName(op, opts.ReleaseName)}, existing)
	if err == nil {
		for _, c := range existing.Status.Conditions {
			if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
				failed = true
				if err := j.Client.Delete(ctx, existing, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
					return err
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

	job := j.buildJob(opts, op, string(specJSON))
	err = j.Client.Create(ctx, job)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}

	return err
}

func (j *JobDeployer) Submit(ctx context.Context, opts DeployOptions) error {
	return j.submit(ctx, opts, OpDeploy)
}

func (j *JobDeployer) SubmitUndeploy(ctx context.Context, opts DeployOptions) error {
	return j.submit(ctx, opts, OpUndeploy)
}

func (j *JobDeployer) Observe(ctx context.Context, opts DeployOptions) (JobState, error) {
	return j.observe(ctx, opts, OpDeploy)
}

func (j *JobDeployer) ObserveUndeploy(ctx context.Context, opts DeployOptions) (JobState, error) {
	return j.observe(ctx, opts, OpUndeploy)
}

func (j *JobDeployer) observe(ctx context.Context, opts DeployOptions, op string) (JobState, error) {
	job := &batchv1.Job{}

	err := j.Client.Get(ctx, client.ObjectKey{Namespace: opts.Namespace, Name: jobName(op, opts.ReleaseName)}, job)
	if apierrors.IsNotFound(err) {
		return JobState{Phase: PendingJobPhase}, nil
	}
	if err != nil {
		return JobState{}, err
	}

	if job.Status.Succeeded > 0 {
		return JobState{Phase: SucceededJobPhase}, nil
	}

	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			reason := j.failureReason(ctx, opts, op)
			return JobState{Phase: FailedJobPhase, Reason: reason}, nil
		}
	}

	return JobState{Phase: RunningJobPhase}, nil
}

func (j *JobDeployer) failureReason(ctx context.Context, opts DeployOptions, op string) string {
	pods := &corev1.PodList{}

	// the pod's termination-message is written moments before the Job flips to Failed, so the cache may not have it yet,
	// and this cold path (only on Job failure) must not spin up a cluster-wide Pod informer, since it takes too much of memory.
	if err := j.Reader.List(ctx, pods, client.InNamespace(opts.Namespace), client.MatchingLabels{"job-name": jobName(op, opts.ReleaseName)}); err != nil {
		return op + " job failed (could not read pod: " + err.Error() + ")"
	}
	for _, p := range pods.Items {
		for _, cs := range p.Status.ContainerStatuses {
			if t := cs.State.Terminated; t != nil && t.Message != "" {
				return t.Message
			}
		}
	}

	return op + " job failed"
}

func (j *JobDeployer) buildJob(opts DeployOptions, op, specJSON string) *batchv1.Job {
	backoff := int32(0)
	labels := jobLabels(opts, op)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName(op, opts.ReleaseName),
			Namespace: opts.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: j.ServiceAccount,
					Containers: []corev1.Container{{
						Name:            "deployer",
						Image:           j.Image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Env: []corev1.EnvVar{
							{Name: EnvOp, Value: op},
							{Name: EnvSpec, Value: specJSON},
							// NOTE: all HELM_* env variables should be passed here. distrolles helm cache writes in /tmp
							{Name: "HELM_CACHE_HOME", Value: "/tmp/.helm/cache"},
							{Name: "HELM_CONFIG_HOME", Value: "/tmp/.helm/config"},
							{Name: "HELM_DATA_HOME", Value: "/tmp/.helm/data"},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "tmp", MountPath: "/tmp"}},
					}},
					Volumes: []corev1.Volume{{
						Name:         "tmp",
						VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
					}},
				},
			},
		},
	}

	if j.Deadline > 0 {
		secs := int64(j.Deadline.Seconds())
		job.Spec.ActiveDeadlineSeconds = &secs
	}

	return job
}
