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

// S3TenantAccountSpec defines the desired state of S3TenantAccount.
// There's no field for the S3 Tenant Account name as the name specified in metadata.name will be used as the account name in the backend.
type S3TenantAccountSpec struct {
	// Features of the S3 Tenant.
	// Currently only maintained by admins.
	// +optional
	Features *S3TenantFeatures `json:"features,omitempty"`

	// Reference to the S3Tenant this account should be bounded by.
	// +optional
	S3TenantRef *corev1.ObjectReference `json:"s3TenantRef,omitempty"`

	// SecretRef for the root tenant user.
	// secret will be created and overridden by the operator, defaults to <S3 Tenant>-admin-credentials.
	// the secret must contain the following keys:
	//   data:
	//     username: <base64 encoded username>
	//     password: <base64 encoded password>
	// the root user is used to manage the tenant and perform administrative tasks.
	// the root user is only for the platform team and should not be used by end users.
	// +optional
	// +kubebuilder:default:={}
	RootSecretRef *corev1.ObjectReference `json:"rootSecretRef"`

	// Override where to store the secrets created for this account.
	// If not set, the secrets will be created in the same namespace as the S3Tenant.
	// If no tenant is specified, the secrets will be created in the operator namespace.
	// This will just create the secrets in the specified namespace, but not go back and delete old ones.
	// +optional
	SecretNamespace string `json:"secretNamespace,omitempty"`

	// Set tenantdeletionpolicy for this account.
	// this overrides the policy set in the StorageGrid CRD.
	// +optional
	TenantDeletionPolicy *TenantDeletionPolicy `json:"tenantDeletionPolicy,omitempty"`

	// Include the common tenant spec.
	CommonTenantSpec `json:",inline"`
}

// currently only maintained by admins.
type S3TenantFeatures struct {
	// Allow users to use CustomIdentitySource.
	// +optional
	CustomIdentitySource *bool `json:"customIdentitySource,omitempty"`

	// Enable PlatformServices like CloudMirror.
	// +optional
	PlatformServices *bool `json:"platformServices,omitempty"`

	// Enable SelectObjectContent API to filter and retrieve object data.
	// +optional
	SelectObjectContent *bool `json:"selectObjectContent,omitempty"`
}

// S3TenantAccountStatus defines the observed state of S3TenantAccount.
type S3TenantAccountStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster.
	// Important: Run "make" to regenerate code after modifying this file.

	// embed common tenant status.
	CommonTenantStatus `json:",inline"`

	// Compiled description with all metadata.
	// This will include the owner, description and additional metadata.
	// Used to track changes in the tenant description.
	// +optional
	Description string `json:"description,omitempty"`

	// Reference to the S3Tenant this account is bounded by.
	// +optional
	S3TenantRef *corev1.ObjectReference `json:"s3TenantRef,omitempty"`

	// ObservedTenantBackendName is the actual name in the backend.
	// +optional
	// +kubebuilder:default=""
	ObservedTenantBackendName *string `json:"observedTenantBackendName,omitempty"`

	// DesiredTenantBackendName is the name of the desired state in the backend.
	// +optional
	DesiredTenantBackendName *string `json:"desiredTenantBackendName,omitempty"`

	// DeletionTimestamp is set when the tenant account is due for deletion.
	// if this timestamp is reached, the tenant account will be deleted both in Kubernetes and in the backend.
	// +optional
	DeletionTimestamp *metav1.Time `json:"deletionTimestamp,omitempty"`

	// SecretRef for the root tenant user.
	// secret will be created and overridden by the operator, defaults to <S3 Tenant>-admin-credentials.
	// the secret must contain the following keys:
	//   data:
	//     username: <base64 encoded username>
	//     password: <base64 encoded password>
	// the root user is used to manage the tenant and perform administrative tasks.
	// the root user is only for the platform team and should not be used by end users.
	// +optional
	// +kubebuilder:default:={}
	RootSecretRef *corev1.ObjectReference `json:"rootSecretRef"`

	// This is the applied tenant deletion policy.
	// this is either the policy set in the StorageGrid CRD or the one overridden in the S3TenantAccount.
	// this is needed to track changes in the deletion policy.
	// +optional
	TenantDeletionPolicy *TenantDeletionPolicy `json:"tenantDeletionPolicy,omitempty"`

	// Track s3Tenant conditions.
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="S3Tenant",type="string",JSONPath=".status.s3TenantRef.name",description="The name of the tenant in the backend"
// +kubebuilder:printcolumn:name="TenantBackendName",type="string",JSONPath=".status.observedTenantBackendName",description="The name of the tenant in the backend"
// +kubebuilder:printcolumn:name="StorageGrid",type="string",JSONPath=".spec.storageGridRef.name",description="The StorageGrid this tenant account belongs to"
// +kubebuilder:printcolumn:name="Capacity",type="string",JSONPath=".status.quota.limit",description="Configured capacity of the tenant"
// +kubebuilder:printcolumn:name="TenantClass",type="string",JSONPath=".status.s3ApiEndpoint.s3TenantClassName",description="The class of the tenant account"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Current phase of the tenant account"
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name=`AGE`,type=date
// +kubebuilder:resource:scope=Cluster,shortName={"s3accs","s3acc"}

// S3TenantAccount is the Schema for the s3tenantaccounts API.
type S3TenantAccount struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec S3TenantAccountSpec `json:"spec,omitempty"`
	// +kubebuilder:default={}
	Status S3TenantAccountStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// S3TenantAccountList contains a list of S3TenantAccount.
type S3TenantAccountList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []S3TenantAccount `json:"items"`
}

func init() {
	SchemeBuilder.Register(&S3TenantAccount{}, &S3TenantAccountList{})
}
