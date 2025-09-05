package controller

import "fmt"

const (
	// Domain will be used for all annotations
	Domain       = "s3.bedag.ch"
	TenantPrefix = "tenant" // will be forwarded from the s3tenant to the account
	AdminPrefix  = "admin"  // will not be passed through from tenant to the account
	BucketPrefix = "bucket"
	GridPrefix   = "grid"
)

var (
	// recreate credentials
	AnnotationRecreateBucketKeypairs   = fmt.Sprintf("%s.%s/recreate-s3-access-keys", BucketPrefix, Domain)
	AnnotationRecreateTenantKeypairs   = fmt.Sprintf("%s.%s/recreate-s3-access-keys", TenantPrefix, Domain)
	AnnotationResetTenantAdminPassword = fmt.Sprintf("%s.%s/reset-admin-password", AdminPrefix, Domain)

	// import existing s3 tenant into state
	// TODO: implement this
	AnnotationExistingTenant = fmt.Sprintf("%s.%s/existing-tenant-id", AdminPrefix, Domain)

	// force a recreation of the tenant
	AnnotationRecreateTenant = fmt.Sprintf("%s.%s/recreate-tenant", TenantPrefix, Domain)

	// allow the changing of the S3TenantClassName in a tenant since this can change the api endpoints
	AnnotationAllowTenantClassNameChange = fmt.Sprintf("%s.%s/allow-tenant-class-name-change", TenantPrefix, Domain)

	// allow shrinking of the tenant quota
	// TODO: implement this
	AnnotationAllowTenantQuotaShrinking = fmt.Sprintf("%s.%s/allow-tenant-quota-shrink", TenantPrefix, Domain)

	// allow force deletion of the tenant
	// TODO: implement this
	AnnotationForceDeleteTenant = fmt.Sprintf("%s.%s/force-tenant-delete", TenantPrefix, Domain)

	// allow the deletion of the account
	AnnotationAllowTenantDeletion = fmt.Sprintf("%s.%s/allow-tenant-deletetion", TenantPrefix, Domain)

	// Annotation to drain a bucket, deleting all objects stored in it before deleting the bucket itself
	AnnotationDrainBucket = fmt.Sprintf("%s.%s/force-drain-bucket", BucketPrefix, Domain)
)
