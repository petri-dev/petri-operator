package deployer

import (
	"context"
	"encoding/json"
	"fmt"
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

func DeployJobName(release string) string {
	return jobName(OpDeploy, release)
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
	err = j.Client.Get(ctx, client.ObjectKey{Namespace: opts.Namespace, Name: jobName(op, opts.ReleaseName)}, existing)
	if err == nil {
		phase := TerminalPhase(existing)
		if !existing.DeletionTimestamp.IsZero() || phase == RunningJobPhase ||
			(phase == SucceededJobPhase && matchesPayload(existing, string(specJSON))) {
			return nil
		}
		live := new(batchv1.Job)
		if err := j.reader().Get(ctx, client.ObjectKeyFromObject(existing), live); err != nil {
			return client.IgnoreNotFound(err)
		}
		if live.UID != existing.UID || !live.DeletionTimestamp.IsZero() || TerminalPhase(live) == RunningJobPhase ||
			(TerminalPhase(live) == SucceededJobPhase && matchesPayload(live, string(specJSON))) {
			return nil
		}
		err := j.Client.Delete(ctx, live,
			client.Preconditions{UID: &existing.UID},
			client.PropagationPolicy(metav1.DeletePropagationForeground))
		if apierrors.IsConflict(err) {
			return nil
		}
		return client.IgnoreNotFound(err)
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	job := j.buildJob(opts, op, string(specJSON))
	err = j.Client.Create(ctx, job)
	if apierrors.IsAlreadyExists(err) {
		if err := j.reader().Get(ctx, client.ObjectKeyFromObject(job), existing); err != nil {
			return client.IgnoreNotFound(err)
		}
		if existing.Labels["petri.run/managed"] != "true" {
			return fmt.Errorf("job %s/%s already exists and is not managed by Petri", job.Namespace, job.Name)
		}
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

	if !job.DeletionTimestamp.IsZero() {
		return JobState{Phase: PendingJobPhase}, nil
	}
	phase := TerminalPhase(job)
	if phase == RunningJobPhase {
		return JobState{Phase: phase}, nil
	}
	specJSON, err := json.Marshal(opts)
	if err != nil {
		return JobState{}, err
	}
	if !matchesPayload(job, string(specJSON)) {
		return JobState{Phase: PendingJobPhase}, nil
	}
	live := new(batchv1.Job)
	if err := j.reader().Get(ctx, client.ObjectKeyFromObject(job), live); err != nil {
		return JobState{Phase: PendingJobPhase}, client.IgnoreNotFound(err)
	}
	if live.UID != job.UID || !live.DeletionTimestamp.IsZero() || TerminalPhase(live) != phase || !matchesPayload(live, string(specJSON)) {
		return JobState{Phase: PendingJobPhase}, nil
	}
	if phase == FailedJobPhase {
		return JobState{Phase: phase, Reason: j.failureReason(ctx, opts, op)}, nil
	}
	return JobState{Phase: phase}, nil
}

func (j *JobDeployer) reader() client.Reader {
	if j.Reader != nil {
		return j.Reader
	}
	return j.Client
}

func TerminalPhase(job *batchv1.Job) JobPhase {
	for _, c := range job.Status.Conditions {
		if c.Status == corev1.ConditionTrue {
			switch c.Type {
			case batchv1.JobComplete:
				return SucceededJobPhase
			case batchv1.JobFailed:
				return FailedJobPhase
			}
		}
	}
	return RunningJobPhase
}

func matchesPayload(job *batchv1.Job, payload string) bool {
	for _, container := range job.Spec.Template.Spec.Containers {
		if container.Name == "deployer" {
			for _, env := range container.Env {
				if env.Name == EnvSpec {
					return env.Value == payload
				}
			}
		}
	}
	return false
}

func (j *JobDeployer) failureReason(ctx context.Context, opts DeployOptions, op string) string {
	pods := &corev1.PodList{}

	// the pod's termination-message is written moments before the Job flips to Failed, so the cache may not have it yet,
	// and this cold path (only on Job failure) must not spin up a cluster-wide Pod informer, since it takes too much of memory.
	if err := j.reader().List(ctx, pods, client.InNamespace(opts.Namespace), client.MatchingLabels{"job-name": jobName(op, opts.ReleaseName)}); err != nil {
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
		job.Spec.ActiveDeadlineSeconds = new(int64(j.Deadline.Seconds()))
	}

	return job
}
