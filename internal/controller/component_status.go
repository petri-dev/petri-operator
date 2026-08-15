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
	"time"

	"github.com/petri-dev/petri-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func allReady(level []v1alpha1.ComponentSpec, phaseByName map[string]v1alpha1.Phase) bool {
	for _, component := range level {
		if phaseByName[component.Name] != v1alpha1.PhaseReady {
			return false
		}
	}
	return true
}

func setComponentPhase(env *v1alpha1.EphemeralEnvironment, name string, phase v1alpha1.Phase) {
	if cs := findComponent(env, name); cs != nil {
		cs.Phase = phase
		return
	}
	env.Status.Components = append(env.Status.Components, v1alpha1.ComponentStatus{
		Name:  name,
		Phase: phase,
	})
}

func setComponentShared(env *v1alpha1.EphemeralEnvironment, name string) {
	if cs := findComponent(env, name); cs != nil {
		cs.Shared = true
	}
}

func recordRuntimeFailure(env *v1alpha1.EphemeralEnvironment, name, reason string) (exhausted bool) {
	if cs := findComponent(env, name); cs != nil {
		cs.DeployRetries++
		cs.LastFailureReason = reason
		cs.Phase = v1alpha1.PhaseSubmitting
		cs.DeployingSince = nil
		return cs.DeployRetries >= maxDeployRetries
	}

	env.Status.Components = append(env.Status.Components, v1alpha1.ComponentStatus{
		Name:              name,
		Phase:             v1alpha1.PhaseSubmitting,
		DeployRetries:     1,
		LastFailureReason: reason,
	})
	return maxDeployRetries <= 1
}

func resetComponentFailure(env *v1alpha1.EphemeralEnvironment, name string) {
	if cs := findComponent(env, name); cs != nil {
		cs.DeployRetries = 0
		cs.LastFailureReason = ""
	}
}

func setComponentDeployingSince(env *v1alpha1.EphemeralEnvironment, name string, t metav1.Time) {
	if cs := findComponent(env, name); cs != nil && cs.DeployingSince == nil {
		cs.DeployingSince = &t
	}
}

func componentDeployingSince(env *v1alpha1.EphemeralEnvironment, name string) *metav1.Time {
	if cs := findComponent(env, name); cs != nil {
		return cs.DeployingSince
	}
	return nil
}

func deployBackoff(env *v1alpha1.EphemeralEnvironment, components []v1alpha1.ComponentSpec) time.Duration {
	var maxRetries int32
	names := make(map[string]struct{}, len(components))
	for _, c := range components {
		names[c.Name] = struct{}{}
	}
	for _, cs := range env.Status.Components {
		if _, ok := names[cs.Name]; ok && cs.DeployRetries > maxRetries {
			maxRetries = cs.DeployRetries
		}
	}

	backoff := requeueAfter << maxRetries
	if backoff > maxDeployBackoff || backoff <= 0 {
		return maxDeployBackoff
	}
	return backoff
}

func findComponent(env *v1alpha1.EphemeralEnvironment, name string) *v1alpha1.ComponentStatus {
	for i := range env.Status.Components {
		if env.Status.Components[i].Name == name {
			return &env.Status.Components[i]
		}
	}
	return nil
}
