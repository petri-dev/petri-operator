package deployer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// k8s object names must be a DNS-1123 label: at most 63 chars.
	maxJobNameLen = 63

	// hash suffix length when a name is too long; 8 hex chars = 32 bits,
	// enough to avoid collisions between truncated release names.
	jobNameHashLen = 8
)

func jobName(op, release string) string {
	name := "petri-" + op + release
	if len(name) <= maxJobNameLen {
		return name
	}

	sum := sha256.Sum256([]byte(name))
	suffix := hex.EncodeToString(sum[:])[:jobNameHashLen]

	prefix := name[:maxJobNameLen-1-jobNameHashLen] // reserve one char for dash
	return prefix + "-" + suffix
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
	Image          string
	ServiceAccount string
	Deadline       time.Duration
}

func (j *JobDeployer) submit(ctx context.Context, opts DeployOptions, op string) error {
	specJSON, err := json.Marshal(opts)
	if err != nil {
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

func (d *JobDeployer) Observe(ctx context.Context, opts DeployOptions) (JobState, error) {
	return d.observe(ctx, opts, OpDeploy)
}
func (d *JobDeployer) ObserveUndeploy(ctx context.Context, opts DeployOptions) (JobState, error) {
	return d.observe(ctx, opts, OpUndeploy)
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
	if err := j.Client.List(ctx, pods, client.InNamespace(opts.Namespace), client.MatchingLabels{"job-name": jobName(op, opts.ReleaseName)}); err != nil {
		return "deploy job failed (could not read pod: " + err.Error() + ")"
	}
	for _, p := range pods.Items {
		for _, cs := range p.Status.ContainerStatuses {
			if t := cs.State.Terminated; t != nil && t.Message != "" {
				return t.Message
			}
		}
	}

	return "deploy job failed"
}

func (j *JobDeployer) buildJob(opts DeployOptions, op, specJSON string) *batchv1.Job {
	backoff := int32(0)
	deadline := int64(j.Deadline)
	labels := jobLabels(opts, op)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName(op, opts.ReleaseName),
			Namespace: opts.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &backoff,
			ActiveDeadlineSeconds: &deadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: j.ServiceAccount,
					Containers: []corev1.Container{{
						Name:  "deployer",
						Image: j.Image,
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
}
