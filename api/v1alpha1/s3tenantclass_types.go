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

// S3TenantClassSpec defines the desired state of S3TenantClass.
type S3TenantClassSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster.
	// Important: Run "make" to regenerate code after modifying this file.

	// The backing ID on the StorageGrid.
	// Need to point to an existing gateway in the StorageGrid.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storageGridRef is immutable"
	BackingID string `json:"backingID,omitempty"`

	// pathstyle access means the bucketname is part of the path.
	// +kubebuilder:default=false
	// +optional
	UsePathStyleAccess bool `json:"usePathStyleAccess,omitempty"`

	// StorageGrid Reference for the TenantClass.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storageGridRef is immutable"
	StorageGridRef corev1.LocalObjectReference `json:"storageGridRef,omitempty"`

	// Enforce make sure that only the tenants in this class can.
	// use the backing gateway in the StorageGrid.
	// +kubebuilder:default=false
	// +optional
	Enforce bool `json:"enforce,omitempty"`

	// Frequency to check for changes in the StorageGrid in minutes.
	// +kubebuilder:default="60m"
	// +optional
	RefreshInterval *metav1.Duration `json:"refreshInterval,omitempty"`

	// PreferredEndpoints controls which gateway endpoints are exposed to tenants.
	// If unset, all discovered endpoints from the gateway certificate are exposed
	// with the first as default. If set, only specified endpoints are exposed.
	// +optional
	PreferredEndpoints *PreferredEndpointsSpec `json:"preferredEndpoints,omitempty"`
}

// PreferredEndpointsSpec defines which endpoints from the StorageGrid gateway
// should be exposed to tenants. If unset, all discovered endpoints are exposed.
type PreferredEndpointsSpec struct {
	// DefaultEndpoint is the primary S3 endpoint to expose. This endpoint will
	// be listed first and marked as the default. It will be included in the
	// endpoint list even if not found in the gateway certificate SANs (with a warning).
	// +kubebuilder:validation:MinLength=1
	DefaultEndpoint string `json:"defaultEndpoint"`

	// AdditionalEndpoints are extra S3 endpoints to expose alongside the default.
	// If unset (nil), all discovered endpoints from the gateway certificate are included.
	// If set to an empty list ([]), only the DefaultEndpoint is exposed.
	// Endpoints not found in the gateway certificate SANs will be kept in status but
	// a warning event will be emitted (admin knows best).
	// +optional
	AdditionalEndpoints []string `json:"additionalEndpoints,omitempty"`
}

// S3TenantClassStatus defines the observed state of S3TenantClass.
type S3TenantClassStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster.
	// Important: Run "make" to regenerate code after modifying this file.

	// The displayname of the S3TenantClass.
	// +optional
	DisplayName string `json:"displayName,omitempty"`

	// Port for the S3TenantClass.
	// +optional
	Port int32 `json:"port,omitempty"`

	// Secure defines whether TLS is enabled on the endpoint or not.
	// +optional
	Secure bool `json:"secure,omitempty"`

	// Define whether ipv4 access is enabled.
	// +kubebuilder:default=true
	// +optional
	IPv4AccessEnabled bool `json:"ipv4AccessEnabled,omitempty"`

	// Define whether ipv6 access is enabled.
	// +kubebuilder:default=false
	// +optional
	IPv6AccessEnabled bool `json:"ipv6AccessEnabled,omitempty"`

	// drop requests on untrusted client networks.
	// +kubebuilder:default=false
	// +optional
	UntrustedNetworksDropped bool `json:"untrustedNetworksDropped,omitempty"`

	// track a list of tenants that use this class.
	// needed to update the tenantclass when a tenant is created or deleted.
	S3TenantIDs []string `json:"tenants,omitempty"`

	// S3 endpoint configuration for tenant access.
	// Contains the list of addresses and default address configuration.
	// +optional
	S3EndpointConfig *S3EndpointConfig `json:"s3EndpointConfig,omitempty"`

	// Track S3TenantClass conditions.
	// Track s3Tenant conditions.
	// Conditions is an array of conditions.
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// Track the last time the S3TenantClass was updated.
	LastUpdated metav1.Time `json:"lastUpdated,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="StorageGrid",type="string",JSONPath=".spec.storageGridRef.name",description="The StorageGrid this tenant account belongs to"
// +kubebuilder:printcolumn:name="DisplayName",type="string",JSONPath=".status.displayName",description="Displayname within StorageGrid"
// +kubebuilder:printcolumn:name="Default Address",type="string",JSONPath=".status.s3EndpointConfig.defaultAddress",description="Default S3 address"
// +kubebuilder:printcolumn:name="Enforce",type="string",JSONPath=".spec.enforce",description="Where it will be enforced, so only tenants in this class can use the backing gateway"
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name=`AGE`,type=date
// +kubebuilder:resource:scope=Cluster,shortName={"s3class"}

// S3TenantClass is the Schema for the s3tenantclasses API.
type S3TenantClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   S3TenantClassSpec   `json:"spec,omitempty"`
	Status S3TenantClassStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// S3TenantClassList contains a list of S3TenantClass.
type S3TenantClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []S3TenantClass `json:"items"`
}

func init() {
	SchemeBuilder.Register(&S3TenantClass{}, &S3TenantClassList{})
}
