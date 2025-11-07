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

const (
	DefaultRetentionDuration       = "168h"                     // 7 days
	DefaultTenantDeletionProcedure = TenantDeletionPolicyRetain // Default policy for tenant deletion
)

type TenantDeletionPolicyType string
type TenantPrefix string

// TenantDeletionPolicyType defines the type of deletion policy for tenants.
const (
	TenantDeletionPolicyDelete           TenantDeletionPolicyType = "Delete"
	TenantDeletionPolicyRetain           TenantDeletionPolicyType = "Retain"
	TenantDeletionPolicyRetainThenDelete TenantDeletionPolicyType = "RetainThenDelete"
)

// TenantPrefix defines the type of prefixing for tenant names.
const (
	TenantPrefixDisabled  TenantPrefix = "Disabled"
	TenantPrefixNamespace TenantPrefix = "Namespace"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!.
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// StorageGridSpec defines the desired state of StorageGrid.
type StorageGridSpec struct {
	// The secret containing the credentials for the StorageGrid.
	SecretRef corev1.ObjectReference `json:"secretRef"`

	// The endpoint of the StorageGrid.
	Endpoint string `json:"endpoint,omitempty"`

	// Default region for buckets created in the StorageGrid, defaults to the first region in the list.
	// +optional
	DefaultBucketRegion string `json:"defaultBucketRegion,omitempty"`

	// Controls whether tenant names should be prefixed in the backend.
	// Possible values:
	// - "Disabled": (default) No prefixing
	// - "Namespace": Namespace is added as a prefix (e.g., "namespace-tenant")
	// +kubebuilder:validation:Enum=Disabled;Namespace
	// +kubebuilder:default="Disabled"
	TenantPrefix TenantPrefix `json:"tenantPrefix,omitempty"`

	// DefaultDeletionPolicy specifies how associated resources (e.g., Tenants) should be handled.
	// when the Tenant resource is deleted.
	// A tenant will always be able to override this policy if specified in the Tenant resource.
	// By default this is set to Retain with a retention duration of 7 days.
	// The maximum retention duration is 30 days.
	// +optional
	// +kubebuilder:default={retentionDuration: "168h"}
	DefaultTenantDeletionPolicy *TenantDeletionPolicy `json:"defaultTenantDeletionPolicy,omitempty"`

	// Amount of nodes (default: 1) that can be unavailable before the StorageGrid is considered not ready.
	// This is a safeguard to prevent operations when the StorageGrid is not fully available.
	// +kubebuilder:default=1
	// +optional
	MaxUnavailableNodes int `json:"maxUnavailableNodes,omitempty"`
}

type TenantDeletionPolicy struct {
	// RetentionDuration specifies the duration to retain resources after deletion is requested.
	// Accepts values like "60m", "24h", etc., using the standard Kubernetes duration format.
	// set this to 0 to disable retention.
	// Defaults to "168h" (7 days).
	// +optional
	// +kubebuilder:default="168h"
	// +kubebuilder:validation:XValidation:rule="duration(self) <= duration('720h')",message="retentionDuration must not exceed 30 days"
	RetentionDuration *metav1.Duration `json:"retentionDuration,omitempty"`

	// Policy specifies the deletion policy for tenants.
	// Possible values:
	// - "Delete": Tenants are deleted on the backend immediately.
	// - "Retain": (default) Tenants are retained and need to be manually deleted later.
	// - "RetainThenDelete": Tenants are retained for the duration specified in RetentionDuration, then deleted.
	// +kubebuilder:validation:Enum=Delete;Retain;RetainThenDelete
	// +kubebuilder:default="Retain"
	// +optional
	Policy TenantDeletionPolicyType `json:"policy,omitempty"`
}

// StorageGridStatus defines the observed state of StorageGrid.
type StorageGridStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster.
	// Important: Run "make" to regenerate code after modifying this file.

	// list of available regions for tenants.
	Regions []string `json:"regions,omitempty"`

	// DefaultRegion for buckets created in the StorageGrid.
	// +optional
	DefaultBucketRegion string `json:"defaultBucketRegion,omitempty"`

	// Currently active tenant deletion policy.
	// +optional
	// +kubebuilder:default={}
	DefaultTenantDeletionPolicy *TenantDeletionPolicy `json:"defaultTenantDeletionPolicy,omitempty"`

	// Usage of the StorageGrid summarizes the usage of all tenants.
	// +optional
	StorageGridUsage StorageGridUsage `json:"usage,omitempty"`

	// Displays whether the StorageGrid is ready or not.
	// +kubebuilder:default=false
	Ready bool `json:"ready,omitempty"`

	// Track StorageGrid conditions.
	// Track s3Tenant conditions.
	// Conditions is an array of conditions.
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

type StorageGridUsage struct {
	// currently removed anything but the tenantCound because it's too hard to deal with due to potential duplicates.

	// total objects stored by the StorageGrid from the linked tenants.
	// +optional
	// ObjectCount int64 `json:"objectCount,omitempty"`.

	// total buckets created by the StorageGrid from the linked tenants.
	// +optional
	// BucketCount int `json:"bucketCount,omitempty"`.

	// total bytes stored by the StorageGrid from the linked tenants.
	// +optional
	// UsedBytes *resource.Quantity `json:"bytes,omitempty"`.

	// total tenants linked to the StorageGrid.
	// +optional
	TenantCount int `json:"tenantCount,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName={"sg","sg"}
// +kubebuilder:printcolumn:name="Ready",type="boolean",JSONPath=".status.ready",description="The current readiness state of the StorageGrid"
// +kubebuilder:printcolumn:name="DeletionPolicy",type="string",JSONPath=".status.defaultTenantDeletionPolicy.policy",description="Deletion policy by default for tenants"
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name=`AGE`,type=date

// StorageGrid is the Schema for the storagegrids API.
type StorageGrid struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StorageGridSpec   `json:"spec,omitempty"`
	Status StorageGridStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// StorageGridList contains a list of StorageGrid.
type StorageGridList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StorageGrid `json:"items"`
}

func init() {
	SchemeBuilder.Register(&StorageGrid{}, &StorageGridList{})
}
