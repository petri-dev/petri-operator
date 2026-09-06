package controller

import (
	"testing"
	"time"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func envInPhase(phase v1alpha1.Phase) *v1alpha1.EphemeralEnvironment {
	return &v1alpha1.EphemeralEnvironment{
		ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "default"},
		Status:     v1alpha1.EphemeralEnvironmentStatus{Phase: phase},
	}
}

func TestRecordPhaseTransition_EmitsEventOnChange(t *testing.T) {
	rec := events.NewFakeRecorder(4)
	recordPhaseTransition(rec, envInPhase(v1alpha1.PhaseReady), v1alpha1.PhaseDeploying)

	select {
	case ev := <-rec.Events:
		if want := "Normal Ready environment Ready"; ev != want {
			t.Fatalf("got %q, want %q", ev, want)
		}
	default:
		t.Fatal("expected an event, got none")
	}
}

func TestRecordPhaseTransition_WarningOnFailed(t *testing.T) {
	rec := events.NewFakeRecorder(4)
	recordPhaseTransition(rec, envInPhase(v1alpha1.PhaseFailed), v1alpha1.PhaseDeploying)

	ev := <-rec.Events
	if want := "Warning Failed environment Failed"; ev != want {
		t.Fatalf("got %q, want %q", ev, want)
	}
}

func TestRecordPhaseTransition_NoEventWhenUnchanged(t *testing.T) {
	rec := events.NewFakeRecorder(4)
	recordPhaseTransition(rec, envInPhase(v1alpha1.PhaseReady), v1alpha1.PhaseReady)

	select {
	case ev := <-rec.Events:
		t.Fatalf("expected no event, got %q", ev)
	default:
	}
}

func TestRecordPhaseTransition_NilRecorderSafe(t *testing.T) {
	recordPhaseTransition(nil, envInPhase(v1alpha1.PhaseReady), v1alpha1.PhaseDeploying)
}

func TestRecordPhaseTransition_SkipsEmptyPhase(t *testing.T) {
	rec := events.NewFakeRecorder(4)
	recordPhaseTransition(rec, envInPhase(""), v1alpha1.PhaseReady)

	select {
	case ev := <-rec.Events:
		t.Fatalf("expected no event for empty phase, got %q", ev)
	default:
	}
}

func TestRecordPhaseTransition_CountsAndFailures(t *testing.T) {
	beforeReady := testutil.ToFloat64(phaseTransitions.WithLabelValues(string(v1alpha1.PhaseReady)))
	beforeFail := testutil.ToFloat64(deployFailures)

	recordPhaseTransition(nil, envInPhase(v1alpha1.PhaseReady), v1alpha1.PhaseDeploying)
	recordPhaseTransition(nil, envInPhase(v1alpha1.PhaseFailed), v1alpha1.PhaseDeploying)

	if got := testutil.ToFloat64(phaseTransitions.WithLabelValues(string(v1alpha1.PhaseReady))); got != beforeReady+1 {
		t.Fatalf("Ready transitions: got %v, want %v", got, beforeReady+1)
	}
	if got := testutil.ToFloat64(deployFailures); got != beforeFail+1 {
		t.Fatalf("deploy failures: got %v, want %v", got, beforeFail+1)
	}
}

func TestDeployDurationUsesDeploymentStart(t *testing.T) {
	before := &dto.Metric{}
	if err := deployDuration.Write(before); err != nil {
		t.Fatal(err)
	}
	env := envInPhase(v1alpha1.PhaseReady)
	env.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	env.Status.DeployStartedAt = new(metav1.NewTime(time.Now().Add(-10 * time.Second)))
	recordPhaseTransition(nil, env, v1alpha1.PhaseDeploying)
	// An object without a recorded start must not contribute its creation age.
	env.Status.DeployStartedAt = nil
	recordPhaseTransition(nil, env, v1alpha1.PhaseDeploying)
	after := &dto.Metric{}
	if err := deployDuration.Write(after); err != nil {
		t.Fatal(err)
	}
	if after.Histogram.GetSampleCount() != before.Histogram.GetSampleCount()+1 {
		t.Fatal("expected one duration sample")
	}
	if elapsed := after.Histogram.GetSampleSum() - before.Histogram.GetSampleSum(); elapsed < 10 || elapsed > 30 {
		t.Fatalf("expected deployment duration, got %v seconds", elapsed)
	}
}

func TestReconcileRecordsRedeployment(t *testing.T) {
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []v1alpha1.Phase{v1alpha1.PhaseReady, v1alpha1.PhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			env := envInPhase(phase)
			env.Generation = 2
			env.Status.ObservedGeneration = 1
			env.Status.DeployStartedAt = new(metav1.NewTime(time.Now().Add(-time.Hour)))
			env.Spec.Template = "empty"
			template := &v1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "empty", Namespace: env.Namespace}}
			c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).WithObjects(env, template).Build()
			r := &EphemeralEnvironmentReconciler{Client: c, Scheme: s}
			ready := phaseTransitions.WithLabelValues(string(v1alpha1.PhaseReady))
			deploying := phaseTransitions.WithLabelValues(string(v1alpha1.PhaseDeploying))
			beforeReady, beforeDeploying := testutil.ToFloat64(ready), testutil.ToFloat64(deploying)
			start := time.Now().Truncate(time.Second)
			for range 2 {
				if _, err := r.reconcile(t.Context(), env); err != nil {
					t.Fatal(err)
				}
				if err := c.Get(t.Context(), client.ObjectKeyFromObject(env), env); err != nil {
					t.Fatal(err)
				}
			}
			if env.Status.DeployStartedAt == nil || env.Status.DeployStartedAt.Before(new(metav1.NewTime(start))) {
				t.Fatal("deployment start was not reset and persisted")
			}
			if testutil.ToFloat64(ready) != beforeReady+1 || testutil.ToFloat64(deploying) != beforeDeploying+1 {
				t.Fatal("expected one Deploying and one Ready transition, with no duplicates on resync")
			}
		})
	}
}
