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
	"k8s.io/apimachinery/pkg/api/resource"
)

type Phase string

const (
	PhasePending          Phase = "Pending"
	PhaseReady            Phase = "Ready"
	PhaseFailed           Phase = "Failed"
	PhaseDeleting         Phase = "Deleting"
	PhaseInProgress       Phase = "InProgress"
	PhaseRetaining        Phase = "Retaining"
	PhaseRetainThenDelete Phase = "RetainThenDelete"
	PhaseBound            Phase = "Bound"
)

// CommonTenantSpec defines the common fields for S3Tenant and S3TenantAccount.
type CommonTenantSpec struct {
	// Name to identify the S3 Tenant in the backned.
	// This name will be prefixed with predefined values.
	// to ensure uniqueness across the backend.
	Name string `json:"name,omitempty"`

	// User facing description of the S3 Tenant in Kubernetes.
	// This will be reflected in the backend.
	// +optional
	Description *string `json:"description,omitempty"`

	// As there is no metadata field in the backend, you can use this to add <field>:<value> pairs to the description.
	// This will always include the specified owner and description as default fields.
	// +optional
	AdditionalTenantMetadata map[string]string `json:"additionalTenantMetadata,omitempty"`

	// define tenantclassname for s3 endpoint accessibility.
	// if not specified the default tenant class will be used.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	S3TenantClassName string `json:"s3TenantClassName,omitempty"`

	// Configure which namespaces are allowed to create buckets in this tenant (supports wildcards).
	// if unset only the namespace of the tenant is allowed.
	// +optional
	AllowedNamespaces []string `json:"allowedNamespaces,omitempty"`

	// default bucket region to use.
	// if not set, the default region of the StorageGrid will be used.
	// +optional
	DefaultBucketRegion *string `json:"defaultBucketRegion,omitempty"`

	// SecretRef for the admin tenant user.
	// secret will be created and overridden by the operator, defaults to <S3 Tenant>-admin-credentials.
	// the secret must contain the following keys:
	//   data:
	//     username: <base64 encoded username>
	//     password: <base64 encoded password>
	// the admin user is used to manage the tenant and perform administrative tasks.
	// +optional
	// +kubebuilder:default:={}
	AdminSecretRef *corev1.ObjectReference `json:"adminSecretRef"`

	// SecretRef for the admin s3 keys.
	// secret will be created and overridden by the operator, defaults to <S3 Tenant>-s3-admin-keypair.
	// the secret must contain the following keys:
	//   data:
	//     accessKeyId: <base64 encoded access key id>
	//     secretAccessKey: <base64 encoded secret access key>
	// this is the administrative keypair to manage buckets and objects in the tenant.
	// +optional
	// +kubebuilder:default:={}
	S3AdminKeysSecretRef *corev1.ObjectReference `json:"s3AdminKeysSecretRef"`

	// the storage quota for the S3 Tenant.
	// should be specified using a quantity string (e.g., "512Mi", "1Gi").
	// Example:
	//   storageQuota: "1Gi"
	StorageQuota *resource.Quantity `json:"storageQuota,omitempty"`

	// StorageGrid reference.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storageGridRef is immutable"
	StorageGridRef corev1.LocalObjectReference `json:"storageGridRef,omitempty"`
}

type CommonTenantStatus struct {
	// Quota of the S3 Tenant storage.
	Quota QuotaStatus `json:"quota,omitempty"`

	// Available Regions for the S3 Tenant.
	Regions []string `json:"regions,omitempty"`

	// Default region for the buckets of the tenant inherited from grid if not specified by the tenant.
	DefaultBucketRegion string `json:"defaultBucketRegion,omitempty"`

	// Phase displays the current state of the S3 Tenant.
	// Possible values:.
	// - "Pending": The tenant is being created.
	// - "Ready": The tenant is ready to use.
	// - "Failed": The tenant creation failed.
	// - "Deleting": The tenant is being deleted.
	// - "InProgress": The tenant is being updated.
	// - "Retaining": The tenant is being retained after deletion.
	// - "RetainWait": The tenant is retained until deletion timestamp is reached.
	// - "Bound": The S3Tenant is bound to the S3TenantAccount and is ready to use.
	// +kubebuilder:default="Pending"
	// +kubebuilder:validation:Enum=Pending;Ready;Failed;Deleting;InProgress;Retaining;RetainWait;Bound
	Phase Phase `json:"phase,omitempty"`

	// Usage of the S3 Tenant.
	TenantUsage TenantUsage `json:"usage,omitempty"`

	// TenantID of the S3 Tenant.
	TenantID string `json:"tenantID,omitempty"`

	// grid endpoint of the backend.
	// this is the endpoint to access the StorageGrid management API.
	// +optional
	GridEndpoint string `json:"gridEndpoint,omitempty"`

	// TenantManagerURL of the S3 Tenant.
	// Use this URL to manage the tenant using the Web UI.
	TenantManagerURL string `json:"tenantManagerURL,omitempty"`

	// SecretRef for the admin tenant user.
	// secret will be created and overridden by the operator, defaults to <S3 Tenant>-admin-credentials.
	// the secret must contain the following keys:
	//   data:
	//     username: <base64 encoded username>
	//     password: <base64 encoded password>
	// the admin user is used to manage the tenant and perform administrative tasks.
	// +optional
	// +kubebuilder:default:={}
	AdminSecretRef *corev1.ObjectReference `json:"adminSecretRef"`

	// S3AdminAccessKeyId is the ID for the administrative S3 access key.
	// this key is used to manage buckets and objects in the tenant.
	// +optional
	S3AdminAccessKeyId string `json:"s3AdminAccessKeyId,omitempty"`

	// SecretRef for the admin s3 keys.
	// secret will be created and overridden by the operator, defaults to <S3 Tenant>-s3-admin-keypair.
	// the secret must contain the following keys:
	//   data:
	//     accessKeyId: <base64 encoded access key id>
	//     secretAccessKey: <base64 encoded secret access key>
	// this is the administrative keypair to manage buckets and objects in the tenant.
	// +optional
	S3AdminKeysSecretRef *corev1.ObjectReference `json:"s3AdminKeysSecretRef"`

	// S3 endpoint to use for tenant users.
	// Includes URLs and VIPs to access your buckets and objects.
	// +kubebuilder:default:={}
	S3ApiEndpoint *S3ApiEndpoint `json:"s3ApiEndpoint,omitempty"`
}

type QuotaStatus struct {
	// Total storage used by the S3 Tenant.
	Used *resource.Quantity `json:"used,omitempty"`

	// Total storage limit for the S3 Tenant configured on the backend.
	Limit *resource.Quantity `json:"limit,omitempty"`
}

type S3ApiEndpoint struct {
	// TenantClassName which this S3ApiEndpoint belongs to.
	// +optional
	// +kubebuilder:default=""
	S3TenantClassName string `json:"s3TenantClassName,omitempty"`

	// urls of the s3 api endpoint.
	// +optional
	S3Urls []string `json:"s3Urls,omitempty"`

	// S3 VIPs to use for potential fw requests.
	// +optional
	S3VIPs []string `json:"s3VIPs,omitempty"`

	// Port for the S3 API endpoint.
	// +optional
	Port int32 `json:"port,omitempty"`

	// Use pathstyle access for the s3 api endpoint.
	// Defaults to false if not set.
	// +optional
	// +kubebuilder:default=false
	PathStyleAccess *bool `json:"pathStyleAccess,omitempty"`
}

type TenantUsage struct {
	// total objects stored by the S3 Tenant.
	ObjectCount int64 `json:"objectCount,omitempty"`

	// total buckets created by the S3 Tenant.
	BucketCount int `json:"bucketCount,omitempty"`
}
