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

type HelmSpec struct {
	Path       string            `json:"path,omitempty"`
	Git        *HelmGitRef       `json:"git,omitempty"`
	Repo       string            `json:"repo,omitempty"`
	Chart      string            `json:"chart,omitempty"`
	Version    string            `json:"version,omitempty"`
	Values     map[string]string `json:"values,omitempty"`
	ValuesFile string            `json:"valuesFile,omitempty"`
}

type HelmGitRef struct {
	Repo        string `json:"repo"`
	Path        string `json:"path"`
	Ref         string `json:"ref,omitempty"`         // default: main
	FallbackRef string `json:"fallbackRef,omitempty"` // default: main
}

type ComponentSpec struct {
	Name string    `json:"name"`
	Helm *HelmSpec `json:"helm,omitempty"`
}

type IngressSpec struct {
	Domain string `json:"domain"`
}

// EnvironmentTemplateSpec defines the desired state of EnvironmentTemplate.
type EnvironmentTemplateSpec struct {
	// +optional
	Ingress IngressSpec `json:"ingress,omitempty"`

	// +kubebuilder:validation:MinItems=1
	Components []ComponentSpec `json:"components"`
}

// EnvironmentTemplateStatus defines the observed state of EnvironmentTemplate.
type EnvironmentTemplateStatus struct {
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// EnvironmentTemplate is the Schema for the environmenttemplates API.
type EnvironmentTemplate struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of EnvironmentTemplate
	// +required
	Spec EnvironmentTemplateSpec `json:"spec"`

	// status defines the observed state of EnvironmentTemplate
	// +optional
	Status EnvironmentTemplateStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// EnvironmentTemplateList contains a list of EnvironmentTemplate.
type EnvironmentTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []EnvironmentTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &EnvironmentTemplate{}, &EnvironmentTemplateList{})
		return nil
	})
}
