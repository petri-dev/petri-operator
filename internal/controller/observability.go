package controller

import (
	"time"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	phaseTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "petri_environment_phase_transitions_total",
		Help: "Number of times EphemeralEnvironments entered each phase.",
	}, []string{"phase"})
	deployDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "petri_environment_deploy_duration_seconds",
		Help:    "Time from deployment start to Ready for an EphemeralEnvironment.",
		Buckets: prometheus.ExponentialBuckets(5, 2, 10),
	})
	deployFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "petri_environment_deploy_failures_total",
		Help: "Total number of EphemeralEnvironments that entered the Failed phase.",
	})
)

func init() {
	metrics.Registry.MustRegister(phaseTransitions, deployDuration, deployFailures)
}

func eventReason(p v1alpha1.EnvironmentPhase) (reason, eventType string) {
	switch p {
	case v1alpha1.EnvironmentPhaseReady:
		return "Ready", "Normal"
	case v1alpha1.EnvironmentPhaseFailed:
		return "Failed", "Warning"
	case v1alpha1.EnvironmentPhaseDeploying:
		return "Deploying", "Normal"
	case v1alpha1.EnvironmentPhaseTerminating:
		return "Terminating", "Normal"
	default:
		return string(p), "Normal"
	}
}

func recordPhaseTransition(rec events.EventRecorder, env *v1alpha1.EphemeralEnvironment, oldPhase v1alpha1.EnvironmentPhase) {
	newPhase := env.Status.Phase
	if newPhase == oldPhase || newPhase == "" {
		return
	}

	phaseTransitions.WithLabelValues(string(newPhase)).Inc()

	reason, eventType := eventReason(newPhase)
	if rec != nil {
		rec.Eventf(env, nil, eventType, reason, "PhaseChange", "environment "+string(newPhase))
	}

	switch newPhase {
	case v1alpha1.EnvironmentPhaseFailed:
		deployFailures.Inc()
	case v1alpha1.EnvironmentPhaseReady:
		if env.Status.DeployStartedAt != nil {
			deployDuration.Observe(time.Since(env.Status.DeployStartedAt.Time).Seconds())
		}
	}
}

func recordPhaseTransitions(rec events.EventRecorder, env *v1alpha1.EphemeralEnvironment, oldPhase v1alpha1.EnvironmentPhase, passed []v1alpha1.EnvironmentPhase) {
	prev := oldPhase
	for _, p := range passed {
		env.Status.Phase = p
		recordPhaseTransition(rec, env, prev)
		prev = p
	}
}
