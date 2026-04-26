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

package controller

// Event reasons for S3TenantAccount controller.
// These events reflect platform-level operations on the backing StorageGrid tenant.
const (
	// Tenant Lifecycle Events.
	EventTenantCreating     = "TenantCreating"
	EventTenantCreated      = "TenantCreated"
	EventTenantCreateFailed = "TenantCreateFailed"
	EventTenantImported     = "TenantImported"
	EventTenantImportFailed = "TenantImportFailed"
	EventTenantUpdating     = "TenantUpdating"
	EventTenantUpdated      = "TenantUpdated"
	EventTenantUpdateFailed = "TenantUpdateFailed"
	EventTenantDeleting     = "TenantDeleting"
	EventTenantDeleted      = "TenantDeleted"
	EventTenantDeleteFailed = "TenantDeleteFailed"

	// Ownership Management Events.
	EventOwnershipConflict = "OwnershipConflict"

	// Backend Connection Events.
	EventBackendConnectionFailed   = "BackendConnectionFailed"
	EventBackendConnectionRestored = "BackendConnectionRestored"

	// Quota Events.
	EventQuotaWarning  = "QuotaWarning"  // Usage at 80% threshold
	EventQuotaExceeded = "QuotaExceeded" // Usage exceeded quota
	EventQuotaNormal   = "QuotaNormal"   // Usage back under threshold

	// Credential Events.
	EventCredentialsRotated        = "CredentialsRotated"
	EventCredentialsRotationFailed = "CredentialsRotationFailed"

	// Deletion Policy Events.
	EventDeletionPolicyApplied = "DeletionPolicyApplied"
	EventDeletionPolicyFailed  = "DeletionPolicyFailed"
)

// Event reasons for S3Tenant controller.
// These events reflect user-facing operations and account binding.
const (
	// Account Binding Events.
	EventAccountCreating      = "AccountCreating"
	EventAccountCreated       = "AccountCreated"
	EventAccountCreateFailed  = "AccountCreateFailed"
	EventAccountClaiming      = "AccountClaiming"
	EventAccountClaimed       = "AccountClaimed"
	EventAccountClaimFailed   = "AccountClaimFailed"
	EventAccountBound         = "AccountBound"
	EventAccountBindingFailed = "AccountBindingFailed"
	EventAccountUnbound       = "AccountUnbound"
	EventAccountNotReady      = "AccountNotReady"

	// Configuration Management Events.
	EventConfigurationChanged = "ConfigurationChanged"
	EventConfigurationPending = "ConfigurationPending"
	EventConfigurationApplied = "ConfigurationApplied"
	EventConfigurationFailed  = "ConfigurationFailed"

	// Tenant Status Events.
	EventTenantReady    = "TenantReady"
	EventTenantNotReady = "TenantNotReady"
)

// Event reasons for S3TenantClass controller.
// These events reflect gateway endpoint management and tenant allowlist operations.
const (
	// Gateway Management Events.
	EventGatewayFetchFailed       = "GatewayFetchFailed"
	EventGatewayStatusRefreshed   = "GatewayStatusRefreshed"
	EventEndpointNotInCertificate = "EndpointNotInCertificate"

	// Tenant Allowlist Management Events.
	EventTenantAddedToAllowlist     = "TenantAddedToAllowlist"
	EventTenantRemovedFromAllowlist = "TenantRemovedFromAllowlist"
	EventAllowlistUpdateFailed      = "AllowlistUpdateFailed"
)

// Event reasons for StorageGrid controller.
// These events reflect grid connectivity, health monitoring, and configuration discovery.
const (
	// Connection & Authentication Events.
	EventGridConnectionFailed      = "GridConnectionFailed"
	EventGridConnectionEstablished = "GridConnectionEstablished"
	EventGridCredentialsFailed     = "GridCredentialsFailed"

	// Health Monitoring Events.
	EventGridHealthCheckFailed = "GridHealthCheckFailed"
	EventGridUnhealthy         = "GridUnhealthy"
	EventGridHealthRecovered   = "GridHealthRecovered"

	// Configuration Events.
	EventRegionsUpdated     = "RegionsUpdated"
	EventRegionsFetchFailed = "RegionsFetchFailed"
	EventDefaultRegionSet   = "DefaultRegionSet"
)

// Event reasons for S3Bucket controller.
// These events reflect bucket lifecycle, policy management, and S3 endpoint access.
const (
	// Bucket Lifecycle Events.
	EventBucketCreating     = "BucketCreating"
	EventBucketCreated      = "BucketCreated"
	EventBucketCreateFailed = "BucketCreateFailed"
	EventBucketDeleting     = "BucketDeleting"
	EventBucketDeleted      = "BucketDeleted"
	EventBucketDeleteFailed = "BucketDeleteFailed"
	EventBucketNotEmpty     = "BucketNotEmpty"

	// Tenant Dependency Events.
	EventBucketTenantNotReady = "TenantNotReady"
	EventBucketTenantReady    = "TenantReady"

	// Bucket Credentials Events.
	EventBucketCredentialsCreated         = "CredentialsCreated"
	EventBucketCredentialsRotated         = "CredentialsRotated"
	EventBucketCredentialsRotationFailed  = "CredentialsRotationFailed"
	EventBucketCredentialsSecretMissing   = "CredentialsSecretMissing"
	EventBucketCredentialsSecretMismatch  = "CredentialsSecretMismatch"

	// Bucket Policy Events.
	EventBucketPolicyApplied      = "PolicyApplied"
	EventBucketPolicyRemoved      = "PolicyRemoved"
	EventBucketPolicyApplyFailed  = "PolicyApplyFailed"
	EventBucketPolicyRemoveFailed = "PolicyRemoveFailed"

	// Admin User Management Events.
	EventBucketAdminUserCreated      = "AdminUserCreated"
	EventBucketAdminUserCreateFailed = "AdminUserCreateFailed"

	// S3 Endpoint Access Events.
	EventS3EndpointConnectionEstablished = "S3EndpointConnectionEstablished"
	EventS3EndpointConnectionFailed      = "S3EndpointConnectionFailed"

	// Usage Monitoring Events.
	EventBucketUsageUpdated     = "UsageUpdated"
	EventBucketUsageFetchFailed = "UsageFetchFailed"

	// Region Events.
	EventBucketRegionValidationFailed = "RegionValidationFailed"
	EventBucketRegionSet              = "RegionSet"

	// Bucket Import Events.
	EventBucketImported     = "BucketImported"
	EventBucketImportFailed = "BucketImportFailed"

	// Bucket Ownership Events.
	EventBucketOwnershipCheckFailed   = "BucketOwnershipCheckFailed"
	EventBucketOwnershipTaggingFailed = "BucketOwnershipTaggingFailed"
	EventBucketNotOwnedByOperator     = "BucketNotOwnedByOperator"

	// Bucket Drain Events.
	EventBucketDrainingStarted  = "BucketDrainingStarted"
	EventBucketDrainingProgress = "BucketDrainingProgress"
	EventBucketDrainingComplete = "BucketDrainingComplete"
	EventBucketDrainingStuck    = "BucketDrainingStuck"
	EventBucketDrainingCanceled = "BucketDrainingCanceled"
	EventBucketDrainFailed      = "BucketDrainFailed"
	EventBucketOrphanedDrain    = "BucketOrphanedDrainDetected"
	EventBucketAlreadyEmpty     = "BucketAlreadyEmpty"
)
