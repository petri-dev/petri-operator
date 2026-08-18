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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type Phase string

const (
	PhasePending     Phase = "Pending"
	PhaseSubmitting  Phase = "Submitting"
	PhaseDeploying   Phase = "Deploying"
	PhaseReady       Phase = "Ready"
	PhaseFailed      Phase = "Failed"
	PhaseTerminating Phase = "Terminating"
)

// EphemeralEnvironmentSpec defines the desired state of EphemeralEnvironment.
type EphemeralEnvironmentSpec struct {
	Template string `json:"template"`
	// +optional
	Source SourceSpec          `json:"source,omitempty"`
	Values map[string]string   `json:"values,omitempty"`
	Env    map[string]EnvValue `json:"env,omitempty"`
	TTL    string              `json:"ttl,omitempty"`
}

type SourceSpec struct {
	// +optional
	Repo string `json:"repo,omitempty"`
	// +optional
	Branch string `json:"branch,omitempty"`
	// +optional
	SHA string `json:"sha,omitempty"`
}

type ComponentStatus struct {
	Name   string `json:"name"`
	Shared bool   `json:"shared"`
	Phase  Phase  `json:"phase"`
	// +optional
	DeployRetries int32 `json:"deployRetries,omitempty"`
	// +optional
	DeployingSince *metav1.Time `json:"deployingSince,omitempty"`
	// +optional
	LastFailureReason string `json:"lastFailureReason,omitempty"`
}

// EphemeralEnvironmentStatus defines the observed state of EphemeralEnvironment.
type EphemeralEnvironmentStatus struct {
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +kubebuilder:validation:Enum=Pending;Deploying;Ready;Failed;Terminating
	Phase Phase `json:"phase,omitempty"`

	URL       string       `json:"url,omitempty"`
	ExpiresAt *metav1.Time `json:"expiresAt,omitempty"`

	// +listType=map
	// +listMapKey=name
	// +optional
	Components []ComponentStatus `json:"components,omitempty"`

	// observedGeneration is the generation of the spec that produced this status.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="URL",type=string,JSONPath=`.status.url`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// EphemeralEnvironment is the Schema for the ephemeralenvironments API.
type EphemeralEnvironment struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of EphemeralEnvironment
	// +required
	Spec EphemeralEnvironmentSpec `json:"spec"`

	// status defines the observed state of EphemeralEnvironment
	// +optional
	Status EphemeralEnvironmentStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// EphemeralEnvironmentList contains a list of EphemeralEnvironment.
type EphemeralEnvironmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []EphemeralEnvironment `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &EphemeralEnvironment{}, &EphemeralEnvironmentList{})
		return nil
	})
}
