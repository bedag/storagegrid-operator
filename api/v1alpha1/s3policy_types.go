/*
Copyright 2025.

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
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// S3PolicySpec defines the desired state of S3Policy.
type S3PolicySpec struct {
	// embedding common policy spec
	CommonPolicySpec `json:",inline"`
}

// S3PolicyStatus defines the observed state of S3Policy.
type S3PolicyStatus struct {
	// Track s3Tenant conditions.
	// Conditions is an array of conditions.
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// embedding common policy status
	CommonPolicyStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName={"s3pols","s3pol"}
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name=`AGE`,type=date
// +kubebuilder:printcolumn:name="Ready",type="boolean",JSONPath=".status.ready",description="Whether the policy has been successfully rendered"
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".spec.version",description="Policy version"

// S3Policy is the Schema for the s3policies API.
type S3Policy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   S3PolicySpec   `json:"spec,omitempty"`
	Status S3PolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// S3PolicyList contains a list of S3Policy.
type S3PolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []S3Policy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&S3Policy{}, &S3PolicyList{})
}
