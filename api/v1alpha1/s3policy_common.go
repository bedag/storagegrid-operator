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

type CommonPolicySpec struct {
	// Add a description to help others understand the purpose of this policy.
	// +optional
	Description *string `json:"description,omitempty"`

	// Specify the version of the policy as a string, e.g. "2024-01-01".
	// This can be used for tracking changes to the policy over time.
	// +optional
	// +kubebuilder:validation:Pattern=`^\d{4}-\d{2}-\d{2}$`
	Version *string `json:"version,omitempty"`

	// List of rules that should be applied to the s3access.
	// This is a simplified version of the actual S3 policy statement from AWS
	// See https://docs.aws.amazon.com/AmazonS3/latest/userguide/example-bucket-policies.html for more details and some examples.
	// You cannot set the resource, as we are automatically scoping the policy to the bucket referenced in the s3access.
	Rules []PolicyRule `json:"rules,omitempty"`
}

type PolicyRule struct {
	// Effect of the rule, either "Allow" or "Deny".
	// +kubebuilder:validation:Enum=Allow;Deny
	Effect string `json:"effect"`

	// List of actions that are allowed or denied by this rule.
	// These should be in the format of "s3:Action", e.g. "s3:GetObject" or "s3:ListBucket".
	// +kubebuilder:validation:MinItems=1
	Actions []string `json:"actions"`

	// Optional condition for the rule, e.g. only allow access if the request is coming from a specific IP range.
	// This is a simplified version of the actual S3 policy condition spec from AWS.
	// See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition.html for more details and some examples.
	// +optional
	Condition *PolicyCondition `json:"condition,omitempty"`

	// Scope of the rule, either "Bucket" or "Objects".
	// This is used to determine whether the rule applies to bucket-level actions (e.g. ListBucket) or object-level actions (e.g. GetObject).
	// +kubebuilder:validation:Enum=Bucket;Objects
	Scope string `json:"scope"`
}

type PolicyCondition struct {
	// Type of the condition, e.g. "IpAddress", "StringLike" or "StringEquals".
	// see https://docs.aws.amazon.com/AmazonS3/latest/userguide/example-bucket-policies.html#example-bucket-policies-global-condition-keys for some examples of conditions in S3 policies.
	Type string `json:"type"`

	// Key is the key of the condition, e.g. "aws:SourceIp" for an IpAddress condition.
	Key string `json:"key"`

	// Values is the list of values for the condition, e.g. ["10.0.0.1"] for an IpAddress condition with key "aws:SourceIp".
	Values []string `json:"values"`
}

type CommonPolicyStatus struct {
	// Rendered policy json document that is applied to the s3access.
	// Resource is omitted as this is automatically set to the bucket referenced in the s3access.
	// +optional
	RenderedPolicy string `json:"renderedPolicy,omitempty"`

	// Ready indicates whether the policy has been successfully rendered.
	// +optional
	Ready bool `json:"ready,omitempty"`
}
