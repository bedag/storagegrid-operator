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

// ConnectionDetailsMode controls whether the operator synthesizes a Kubernetes Secret
// containing the data needed to connect to the bucket using the access keys it manages.
// +kubebuilder:validation:Enum=All;Disabled
type ConnectionDetailsMode string

const (
	// ConnectionDetailsModeAll instructs the operator to create/update an owned Secret
	// containing all standard connection-detail keys (access key id, secret access key,
	// endpoint URL, region, bucket name).
	ConnectionDetailsModeAll ConnectionDetailsMode = "All"
	// ConnectionDetailsModeDisabled instructs the operator not to create such a Secret,
	// and to remove a previously-created one if present.
	ConnectionDetailsModeDisabled ConnectionDetailsMode = "Disabled"
)

// Standard keys written into the synthesized connection-details Secret. The names
// follow the AWS SDK environment-variable convention so the Secret can be mounted
// directly as `envFrom: - secretRef: ...` in user workloads.
const (
	ConnectionDetailKeyAccessKeyID     = "AWS_ACCESS_KEY_ID"
	ConnectionDetailKeySecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	ConnectionDetailKeyEndpointURL     = "AWS_ENDPOINT_URL"
	ConnectionDetailKeyRegion          = "AWS_REGION"
	ConnectionDetailKeyBucket          = "S3_BUCKET"
)

// ConnectionDetailsSpec configures the operator-synthesized connection-details Secret
// for an S3Bucket or S3Access.
//
// If a Secret with the target name already exists and is not controlled by the source
// CR, the operator refuses to overwrite it and emits an OwnershipConflict event; the
// user can resolve the conflict by either renaming the target via DestinationSecret or
// switching Mode to Disabled.
type ConnectionDetailsSpec struct {
	// Mode controls whether a connection-details Secret is synthesized.
	// Defaults to All
	// Available options:
	// - All: create/update a Secret containing all standard connection-detail keys.
	// - Disabled: do not create such a Secret, and remove a previously-created one if present.
	// +kubebuilder:default=All
	// +optional
	Mode ConnectionDetailsMode `json:"mode,omitempty"`

	// DestinationSecret optionally overrides the name of the synthesized Secret.
	// When empty, the operator uses `<resource-name>-connection-details`.
	// The Secret is always created in the same namespace as the source CR.
	// +optional
	DestinationSecret string `json:"destinationSecret,omitempty"`
}
