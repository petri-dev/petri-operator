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
	"errors"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// SharedComponentProviderSpec defines the desired state of SharedComponentProvider.
type SharedComponentProviderSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	Helm           *HelmSpec       `json:"helm,omitempty"`
	InstanceSecret *InstanceSecret `json:"instanceSecret,omitempty"`
	Provision      *JobScript      `json:"provision,omitempty"`
	Deprovision    *JobScript      `json:"deprovision,omitempty"`
	Binding        *Binding        `json:"binding,omitempty"`
}

type InstanceSecret struct {
	Name string                 `json:"name"`
	Keys map[string]InstanceKey `json:"keys"`
}

// InstanceKey is exactly one of Value (static) or Generate (random).
// Value is always written from the provider spec on reconcile. Generate is written once and preserved, the random value never rotates under a live instance.
type InstanceKey struct {
	Value    string        `json:"value,omitempty"`
	Generate *GenerateSpec `json:"generate,omitempty"`
}

type GenerateSpec struct {
	// +kubebuilder:default=24
	Length int `json:"length,omitempty"`
	// alphanumeric avoids symbols that break DSN URLs; use it by default.
	// +kubebuilder:validation:Enum=alphanumeric;hex
	// +kubebuilder:default=alphanumeric
	Charset string `json:"charset,omitempty"`
}

type JobScript struct {
	Image string `json:"image"`

	// Exactly one of Script or Command. Script is sugar wrapped as ["/bin/sh","-c",Script] for the common multi-line shell case.
	// Command is the raw entrypoint for images without a shell (distroless). Validated at reconcile.
	Script string `json:"script,omitempty"`

	Command []string `json:"command,omitempty"`

	Env     []corev1.EnvVar        `json:"env,omitempty"`
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`
	Volumes []corev1.Volume        `json:"volumes,omitempty"`
}

func (j *JobScript) Validate() error {
	hasScript := j.Script != ""
	hasCommand := len(j.Command) > 0
	switch {
	case j.Image == "":
		return errors.New("jobScript: image is required")
	case hasScript && hasCommand:
		return errors.New("jobScript: script and command are mutually exclusive")
	case !hasScript && !hasCommand:
		return errors.New("jobScript: one of script or command is required")
	}
	return nil
}

type Binding struct {
	SecretKeys map[string]string `json:"secretKeys"`
}

// SharedComponentProviderStatus defines the observed state of SharedComponentProvider.
type SharedComponentProviderStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the SharedComponentProvider resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// SharedComponentProvider is the Schema for the sharedcomponentproviders API.
type SharedComponentProvider struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of SharedComponentProvider
	// +required
	Spec SharedComponentProviderSpec `json:"spec"`

	// status defines the observed state of SharedComponentProvider
	// +optional
	Status SharedComponentProviderStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// SharedComponentProviderList contains a list of SharedComponentProvider.
type SharedComponentProviderList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []SharedComponentProvider `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &SharedComponentProvider{}, &SharedComponentProviderList{})
		return nil
	})
}
