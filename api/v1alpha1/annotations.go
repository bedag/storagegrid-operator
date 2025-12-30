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

import "fmt"

const (
	// Domain will be used for all annotations.
	Domain                 = "s3.bedag.ch"
	AnnotationPrefixTenant = "tenant" // will be forwarded from the s3tenant to the account
	AnnotationPrefixAdmin  = "admin"  // will not be passed through from tenant to the account
	AnnotationPrefixBucket = "bucket"
	AnnotationPrefixGrid   = "grid"
)

var (
	// recreate credentials.
	AnnotationRecreateBucketKeypairs   = fmt.Sprintf("%s.%s/recreate-s3-access-keys", AnnotationPrefixBucket, Domain)
	AnnotationRecreateTenantKeypairs   = fmt.Sprintf("%s.%s/recreate-s3-access-keys", AnnotationPrefixTenant, Domain)
	AnnotationResetTenantAdminPassword = fmt.Sprintf("%s.%s/reset-admin-password", AnnotationPrefixAdmin, Domain)

	// import existing s3 tenant into state.
	AnnotationImportTenant = fmt.Sprintf("%s.%s/import-tenant-id", AnnotationPrefixAdmin, Domain)

	// force a recreation of the tenant.
	AnnotationRecreateTenant = fmt.Sprintf("%s.%s/recreate-tenant", AnnotationPrefixTenant, Domain)

	// allow the changing of the S3TenantClassName in a tenant since this can change the api endpoints.
	AnnotationAllowTenantClassNameChange = fmt.Sprintf("%s.%s/allow-tenant-class-name-change", AnnotationPrefixTenant, Domain)

	// allow shrinking of the tenant quota.
	// TODO: implement this.
	AnnotationAllowTenantQuotaShrinking = fmt.Sprintf("%s.%s/allow-tenant-quota-shrink", AnnotationPrefixTenant, Domain)

	// allow force deletion of the tenant.
	// TODO: implement this.
	AnnotationForceDeleteTenant = fmt.Sprintf("%s.%s/force-tenant-delete", AnnotationPrefixTenant, Domain)

	// allow the deletion of the account.
	AnnotationAllowTenantDeletion = fmt.Sprintf("%s.%s/allow-tenant-deletion", AnnotationPrefixTenant, Domain)

	// Annotation to drain a bucket, deleting all objects stored in it before deleting the bucket itself.
	AnnotationDrainBucket = fmt.Sprintf("%s.%s/force-drain-bucket", AnnotationPrefixBucket, Domain)

	// Tenant annotation to ignore buckets on deletion
	// this annotation only applied to buckets that are not managed by the operator itself.
	// eg buckets created manually or by another operator.
	// buckets will be orphaned and the account is left intact.
	AnnotationIgnoreUnmanagedBuckets = fmt.Sprintf("%s.%s/ignore-unmanaged-buckets", AnnotationPrefixTenant, Domain)

	// TenantAnnotationsToKeep defines annotations that should remain on S3Tenant.
	// and NOT be automatically removed after propagation to S3TenantAccount.
	// These typically serve as user-visible safety gates or status indicators.
	// Each annotation in this list requires documented removal strategy:
	// - Automatic: removed when resource is deleted.
	// - Explicit: controller logic removes after operation completes.
	TenantAnnotationsToKeep = []string{
		AnnotationAllowTenantDeletion, // Stays until user removes or resource deleted
	}
)
