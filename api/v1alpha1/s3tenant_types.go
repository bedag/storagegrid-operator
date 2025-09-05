/*
Copyright 2024.

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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// S3TenantSpec defines the desired state of S3Tenant
type S3TenantSpec struct {
	// embed the common tenant spec
	CommonTenantSpec `json:",inline"`
}

// S3TenantStatus defines the observed state of S3Tenant
type S3TenantStatus struct {
	// keep track of linked buckets in this tenant
	// all buckets are within the same namespace as the tenant
	// +optional
	LinkedBuckets []string `json:"linkedBuckets,omitempty"`

	// track a reference to the tenantAccount binding this tenant
	// +optional
	S3TenantAccountRef *corev1.ObjectReference `json:"s3TenantAccountRef,omitempty"`

	// embed common tenant status
	CommonTenantStatus `json:",inline"`

	// Track s3Tenant conditions
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName={"s3tnts","s3tnt"}
// +kubebuilder:printcolumn:name="StorageGrid",type="string",JSONPath=".spec.storageGridRef.name",description="The StorageGrid this tenant account belongs to"
// +kubebuilder:printcolumn:name="TenantClass",type="string",JSONPath=".status.s3ApiEndpoint.s3TenantClassName",description="The class of the tenant"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Current phase of the tenant"
// +kubebuilder:printcolumn:name="Account",type="string",JSONPath=".status.s3TenantAccountRef.name",description="Backing S3 Tenant Account"
// +kubebuilder:printcolumn:name="Capacity",type="string",JSONPath=".status.quota.limit",description="Configured capacity of the tenant"
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name=`AGE`,type=date

// S3Tenant is the Schema for the s3tenants API
type S3Tenant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec S3TenantSpec `json:"spec,omitempty"`
	// +kubebuilder:default={}
	Status S3TenantStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// S3TenantList contains a list of S3Tenant
type S3TenantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []S3Tenant `json:"items"`
}

func init() {
	SchemeBuilder.Register(&S3Tenant{}, &S3TenantList{})
}
