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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// S3BucketSpec defines the desired state of S3Bucket
type S3BucketSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// region of the S3 Bucket
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="region is immutable"
	Region string `json:"region,omitempty"`

	// Optional retention in days for objects in the bucket, defaults to 0, meaning unlimited retention
	// +optional
	// +kubebuilder:default=0
	RetentionInDays *int32 `json:"retentionInDays,omitempty"`

	// tenant the bucket will be created in
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="s3TenantRef is immutable"
	S3TenantRef corev1.ObjectReference `json:"s3TenantRef,omitempty"`

	// specify a s3 bucketpolicy as json to apply to the bucket
	// check the s3 documentation for the policy json format
	// +optional
	BucketPolicyJson string `json:"bucketPolicyJson,omitempty"`
}

// S3BucketStatus defines the observed state of S3Bucket
type S3BucketStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// S3 Access Key ID for the bucket admin
	AccessKeyId string `json:"accessKeyId,omitempty"`

	// The region the bucket is created in
	Region string `json:"region,omitempty"`

	// The bucketname is generated automatically to prevent duplication
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="bucketName is immutable"
	BucketName string `json:"bucketName,omitempty"`

	// BucketUsage is the total uf bytes and objects in this bucket
	BucketUsage BucketUsage `json:"bucketUsage,omitempty"`

	// SecretRef for the bucket-admin s3 keys
	// secret will be created and overridden by the operator, defaults to <S3 Tenant>-s3-admin-keypair
	// the secret must contain the following keys:
	//   data:
	//     accessKeyId: <base64 encoded access key id>
	//     secretAccessKey: <base64 encoded secret access key>
	// this is the administrative keypair to manage this buckets and it's objects
	// +optional
	S3AdminKeysSecretRef *corev1.LocalObjectReference `json:"s3AdminKeysSecretRef"`

	// s3 api endpoint for the bucket
	S3ApiEndpoint *S3ApiEndpoint `json:"s3ApiEndpoint,omitempty"`

	// track last successfully applied policy
	LastAppliedPolicy string `json:"lastAppliedPolicy,omitempty"`

	// Track s3Bucket conditions
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

type BucketUsage struct {
	// total objects stored in the S3 Bucket
	ObjectCount int `json:"objectCount,omitempty"`

	// total resources used by the S3 Bucket
	Bytes *resource.Quantity `json:"Bytes,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName={"s3bus","s3bu"}
// +kubebuilder:printcolumn:name="Tenant",type="string",JSONPath=".spec.s3TenantRef.name",description="The tenant the bucket belongs to"
// +kubebuilder:printcolumn:name="BucketName",type="string",JSONPath=".status.bucketName",description="The name of the bucket in the backned"
// +kubebuilder:printcolumn:name="Region",type="string",JSONPath=".status.region",description="The region of the bucket"
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name=`AGE`,type=date

// S3Bucket is the Schema for the s3buckets API
type S3Bucket struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   S3BucketSpec   `json:"spec,omitempty"`
	Status S3BucketStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// S3BucketList contains a list of S3Bucket
type S3BucketList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []S3Bucket `json:"items"`
}

func init() {
	SchemeBuilder.Register(&S3Bucket{}, &S3BucketList{})
}
