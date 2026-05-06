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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// S3AccessSpec defines the desired state of S3Access.
type S3AccessSpec struct {
	// Reference to the S3Bucket this access belongs to.
	// This is immutable after creation, recreate the access if you want to change the bucket.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="s3TenantRef is immutable"
	// +kubebuilder:validation:Required
	S3BucketRef corev1.LocalObjectReference `json:"s3BucketRef,omitempty"`

	// List of GlobalS3Policies and namespaced S3Policies to apply to the access.
	// if kind is not specified, it will be assumed to be a GlobalS3Policy.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	PolicyRefs []PolicyRef `json:"policyRefs,omitempty"`

	// Optional list of subpaths to restrict access to, e.g. ["folder1/*", "folder2/subfolder/*"].
	// if empty, access will be granted to the whole bucket.
	// +optional
	SubPaths []string `json:"subPaths,omitempty"`

	// Optionally specify the secret name for the access credentials (raw keypair).
	// If not specified, a secret will be automatically generated and managed by the operator.
	// Changing this after creation will rename the secret (data is moved, old secret is deleted).
	// This is the operator's internal credential store, separate from the user-facing
	// connection-details Secret configured via spec.connectionDetails.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`

	// ConnectionDetails controls a separate operator-synthesized Kubernetes Secret
	// containing the data needed to connect to the bucket through this access
	// (access key id, secret access key, endpoint URL, region, bucket name) for direct
	// consumption by user workloads (mountable as `envFrom`). This is a projection of
	// the credentials Secret referenced by SecretRef plus the bucket's endpoint info;
	// the raw keypair Secret remains the operator's source of truth.
	// Defaults to {mode: All}.
	// +kubebuilder:default={mode: All}
	// +optional
	ConnectionDetails *ConnectionDetailsSpec `json:"connectionDetails,omitempty"`
}

// PolicyRef references an S3Policy or GlobalS3Policy by name and kind.
type PolicyRef struct {
	// Kind of the policy. Must be "S3Policy" or "GlobalS3Policy".
	// Defaults to "GlobalS3Policy" if not specified.
	// +optional
	// +kubebuilder:validation:Enum=S3Policy;GlobalS3Policy;""
	Kind string `json:"kind,omitempty"`

	// Name of the policy resource.
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

// AppliedPolicyRef describes a policy that has been applied to this S3Access.
type AppliedPolicyRef struct {
	// Kind of the policy, e.g. "S3Policy" or "GlobalS3Policy".
	Kind string `json:"kind"`

	// Name of the policy.
	Name string `json:"name"`

	// Namespace of the policy. Empty for cluster-scoped policies.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Version of the policy at the time it was applied. Empty if the policy has no version.
	// +optional
	Version string `json:"version,omitempty"`
}

// S3AccessPhase represents the lifecycle phase of an S3Access.
type S3AccessPhase string

const (
	// S3AccessPhasePending indicates the access is being set up or a policy update is in progress.
	S3AccessPhasePending S3AccessPhase = "Pending"
	// S3AccessPhaseReady indicates the access is fully reconciled with the most recent policy applied.
	S3AccessPhaseReady S3AccessPhase = "Ready"
	// S3AccessPhaseFailed indicates the access reconciliation failed. Check conditions for details.
	S3AccessPhaseFailed S3AccessPhase = "Failed"
)

// S3AccessStatus defines the observed state of S3Access.
type S3AccessStatus struct {
	// Track s3Access conditions.
	// Conditions is an array of conditions.
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// S3 Access Key ID for the access.
	AccessKeyId string `json:"accessKeyId,omitempty"`

	// SecretRef is the operator-managed credentials Secret holding the raw access keypair
	// for this access. This is the operator's source of truth for the credentials.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`

	// ConnectionDetailsSecretRef is set to the user-facing connection-details Secret
	// projected by the operator (when spec.connectionDetails.mode is All). Cleared when
	// mode is Disabled.
	// +optional
	ConnectionDetailsSecretRef *corev1.LocalObjectReference `json:"connectionDetailsSecretRef,omitempty"`

	// Applied policies for this access.
	// +optional
	AppliedPolicyRefs []AppliedPolicyRef `json:"appliedPolicyRefs,omitempty"`

	// Rendered policy document for this access, after applying the referenced policies and subpath restrictions.
	// +optional
	PolicyDocument string `json:"policyDocument,omitempty"`

	// Phase represents the current lifecycle phase of the S3Access.
	// +kubebuilder:default="Pending"
	Phase S3AccessPhase `json:"phase,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Bucket",type="string",JSONPath=".spec.s3BucketRef.name",description="The bucket this access belongs to"
// +kubebuilder:printcolumn:name="ConnectionDetails",type="string",JSONPath=".status.connectionDetailsSecretRef.name",description="Name of the user-facing connection-details secret"
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.phase",description="Current lifecycle phase"
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name=`AGE`,type=date

// S3Access is the Schema for the s3accesses API.
type S3Access struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   S3AccessSpec   `json:"spec,omitempty"`
	Status S3AccessStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// S3AccessList contains a list of S3Access.
type S3AccessList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []S3Access `json:"items"`
}

func init() {
	SchemeBuilder.Register(&S3Access{}, &S3AccessList{})
}
