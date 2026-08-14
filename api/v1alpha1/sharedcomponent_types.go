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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// SharedComponentSpec defines the desired state of SharedComponent.
type SharedComponentSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	Provider     string `json:"provider"`
	MaxConsumers int32  `json:"maxConsumers,omitempty"`
	IdleCleanup  string `json:"idleCleanup,omitempty"`
}

// SharedComponentStatus defines the observed state of SharedComponent.
type SharedComponentStatus struct {
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	Consumers int32 `json:"consumers,omitempty"`
	Ready     bool  `json:"ready,omitempty"`
	Phase     Phase `json:"phase,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// SharedComponent is the Schema for the sharedcomponents API.
type SharedComponent struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of SharedComponent
	// +required
	Spec SharedComponentSpec `json:"spec"`

	// status defines the observed state of SharedComponent
	// +optional
	Status SharedComponentStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// SharedComponentList contains a list of SharedComponent.
type SharedComponentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []SharedComponent `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &SharedComponent{}, &SharedComponentList{})
		return nil
	})
}
