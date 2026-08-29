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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!.
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// S3TenantSpec defines the desired state of S3Tenant.
type S3TenantSpec struct {
	// embed the common tenant spec.
	CommonTenantSpec `json:",inline"`

	// S3TenantAccountRef is an optional reference to an existing S3TenantAccount to claim.
	// When specified, the S3Tenant will claim the referenced S3TenantAccount instead of creating a new one.
	// This field is immutable once set.
	// Similar to PersistentVolumeClaim's volumeName, this allows claiming existing accounts.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="s3TenantAccountRef is immutable"
	S3TenantAccountRef *corev1.LocalObjectReference `json:"s3TenantAccountRef,omitempty"`

	// DeletionPollInterval overrides how often this tenant re-checks whether its blocked
	// deletion can proceed (for example while waiting for linked S3Buckets to be deleted).
	// Falls back to the StorageGrid spec.operations.deletion.pollInterval, then to 30s.
	// This is only a backstop: the tenant is also woken directly by its watch on S3Bucket.
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('5s')",message="deletionPollInterval must be at least 5s"
	// +optional
	DeletionPollInterval *metav1.Duration `json:"deletionPollInterval,omitempty"`
}

// S3TenantStatus defines the observed state of S3Tenant.
type S3TenantStatus struct {
	// keep track of linked buckets in this tenant.
	// These are only the buckets managed by the operator itself.
	// Manually created buckets are not visible here
	// +optional
	LinkedBuckets []string `json:"linkedBuckets,omitempty"`

	// track a reference to the tenantAccount binding this tenant.
	// +optional
	S3TenantAccountRef *corev1.ObjectReference `json:"s3TenantAccountRef,omitempty"`

	// embed common tenant status.
	CommonTenantStatus `json:",inline"`

	// ObservedGeneration is the most recent generation observed by the controller.
	// It is used to track whether the controller has processed the latest spec changes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Track s3Tenant conditions.
	// Conditions is an array of conditions.
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName={"s3tnts","s3tnt"}
// +kubebuilder:printcolumn:name="StorageGrid",type="string",JSONPath=".spec.storageGridRef.name",description="The StorageGrid this tenant account belongs to"
// +kubebuilder:printcolumn:name="Default Address",type="string",JSONPath=".status.s3EndpointConfig.defaultAddress",description="Default S3 address"
// +kubebuilder:printcolumn:name="TenantClass",type="string",JSONPath=".status.s3EndpointConfig.s3TenantClassName",description="The class of the tenant"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Current phase of the tenant"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status",description="Ready status"
// +kubebuilder:printcolumn:name="Account",type="string",JSONPath=".status.s3TenantAccountRef.name",description="Backing S3 Tenant Account"
// +kubebuilder:printcolumn:name="Capacity",type="string",JSONPath=".status.quota.limit",description="Configured capacity of the tenant"
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name=`AGE`,type=date

// S3Tenant is the Schema for the s3tenants API.
type S3Tenant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec S3TenantSpec `json:"spec,omitempty"`
	// +kubebuilder:default={}
	Status S3TenantStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// S3TenantList contains a list of S3Tenant.
type S3TenantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []S3Tenant `json:"items"`
}

// CanServeBackendOperations reports whether a dependent resource (S3Bucket, S3Access) may
// use this tenant to reach the StorageGrid backend. Pass dependentTerminating=true when the
// caller is itself being deleted.
//
// A terminating tenant still serves cleanup. Its backend tenant and admin credentials live
// until the S3TenantAccount finalizes, and the tenant blocks its own deletion on exactly
// these dependents - so refusing them while it terminates would deadlock the pair: the
// bucket could never finalize, and the tenant would wait on it forever. A dependent that is
// not being deleted is still refused, since there is no point starting new work against a
// tenant that is going away.
func (t *S3Tenant) CanServeBackendOperations(dependentTerminating bool) bool {
	if t.Status.Phase == PhaseBound {
		return true
	}

	return dependentTerminating && t.Status.Phase == PhaseDeleting
}

func init() {
	SchemeBuilder.Register(&S3Tenant{}, &S3TenantList{})
}
