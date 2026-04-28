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

package grid

import (
	"context"
	"fmt"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"

	models "github.com/bedag/storagegrid-sdk-go/models"
)

type BucketUsage = models.BucketStats

// CreateBucket creates a new bucket with the specified name and region and returns admin credentials for s3 access.
// When objectLock is non-nil and Mode != Disabled, S3 Object Lock is enabled with the supplied default retention.
func CreateBucket(ctx context.Context, name string, region string, objectLock *s3v1alpha1.S3ObjectLockBucketSpec, tenantClient *TenantClient) (err error) {
	log := log.FromContext(ctx).WithValues("func", "CreateBucket")
	log.V(1).Info(fmt.Sprintf("Creating bucket: name=%s, region=%s, objectLock=%+v", name, region, objectLock))

	bucket := models.Bucket{
		Name:   name,
		Region: region,
	}

	if settings := objectLockToSDK(objectLock); settings != nil {
		bucket.S3ObjectLock = settings
	}

	// no need to keep the result.
	_, err = tenantClient.Bucket().Create(ctx, &bucket)
	if err != nil {
		log.Error(err, "Failed to create bucket")

		if strings.Contains(err.Error(), "409 Conflict") {
			conflictErr := fmt.Errorf("BucketAlreadyExists: A bucket with name %s already exists on the grid, please use a different name", bucket.Name)
			log.Error(conflictErr, "Bucket name conflict")
			return conflictErr
		}

		return err
	}

	log.V(1).Info("Bucket created successfully")
	return nil
}

func CreateBucketAdminIfNotExists(ctx context.Context, bucketName string, uniqueIdentifier string, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "CreateBucketAdmin")
	log.V(1).Info(fmt.Sprintf("Creating bucket admin for bucket %s", bucketName))

	bucketAdminUserName, bucketAdminGroupName := getBucketGroupAndAdminUserName(uniqueIdentifier)

	// create group.
	groupId, err := createBucketAdminGroup(ctx, bucketName, bucketAdminGroupName, tenantClient)
	if err != nil {
		log.Error(err, "Failed to create bucket admin group")
		return err
	}

	// create user.
	userId, err := createBucketAdminUser(ctx, bucketName, bucketAdminUserName, groupId, tenantClient)
	if err != nil {
		log.Error(err, "Failed to create bucket admin user")
		return err
	}

	log.V(1).Info(fmt.Sprintf("Bucket admin created: userId=%s, groupid=%s", userId, groupId))
	return nil
}

func createBucketAdminGroup(ctx context.Context, bucketName string, groupName string, tenantClient *TenantClient) (string, error) {
	log := log.FromContext(ctx).WithValues("func", "CreateBucketAdminGroup")
	log.V(1).Info(fmt.Sprintf("Creating bucket admin group for bucket %s", bucketName))

	groupid, err := createGroupIfNotExists(ctx, groupName, generateBucketAdminGroupPolicy(groupName, bucketName), tenantClient)
	if err != nil {
		log.Error(err, "Failed to create bucket admin group")
		return "", err
	}

	log.V(1).Info(fmt.Sprintf("Group created: id=%s", groupid))
	return groupid, nil
}

func generateBucketAdminGroupPolicy(policyName string, bucketName string) *models.TenantGroupPolicies {
	log := log.FromContext(context.Background()).WithValues("func", "generateBucketAdminGroupPolicy")
	log.V(1).Info(fmt.Sprintf("Generating bucket admin group policy: policyName=%s, bucketName=%s", policyName, bucketName))

	// allow all s3 actions.
	actions := []string{"s3:*"}
	// only on the bucket specified though.
	resource := []string{
		fmt.Sprintf("arn:aws:s3:::%s/*", bucketName),
		fmt.Sprintf("arn:aws:s3:::%s", bucketName),
	}

	return &models.TenantGroupPolicies{
		// these are tenant level permissios.
		// left empty.
		Management: &models.TenantGroupManagementPolicy{},
		S3: &models.S3Policy{
			ID: &policyName,
			Statement: []models.S3Statement{
				{
					Effect:   "Allow",
					Action:   &actions,
					Resource: resource,
				},
			},
		},
	}
}

func createBucketAdminUser(ctx context.Context, bucketName string, username string, groupID string, tenantClient *TenantClient) (string, error) {
	log := log.FromContext(ctx).WithValues("func", "CreateBucketAdminUser")
	log.V(1).Info(fmt.Sprintf("Creating bucket admin user for bucket %s", bucketName))

	createdUserId, err := createUserIfNotExists(ctx, username, groupID, tenantClient)
	if err != nil {
		log.Error(err, "Failed to create bucket admin user")
		return "", err
	}

	log.V(1).Info(fmt.Sprintf("Bucket admin user created: id=%s", createdUserId))
	return createdUserId, nil
}

func CreateS3Credentials(ctx context.Context, bucketName string, identifier string, tenantClient *TenantClient) (accesskeyId string, key string, secret string, err error) {
	log := log.FromContext(ctx).WithValues("func", "CreateS3Credentials")
	log.V(1).Info(fmt.Sprintf("Creating s3 credentials for bucket %s", bucketName))

	s3AccessKey := models.S3AccessKey{}

	_, bucketAdminUser := getBucketGroupAndAdminUserName(identifier)

	user, err := tenantClient.Users().GetByName(ctx, bucketAdminUser)
	if err != nil {
		log.Error(err, "Failed to get user by name")
		return "", "", "", err
	}

	createdS3AccessKey, err := tenantClient.S3AccessKeys().CreateForUser(ctx, *user.Id, &s3AccessKey)
	if err != nil {
		log.Error(err, "Failed to create s3 access key")
		return "", "", "", err
	}

	log.V(1).Info(fmt.Sprintf("S3 accessKey created: id=%s", *createdS3AccessKey.Id))
	return *createdS3AccessKey.Id, *createdS3AccessKey.AccessKey, *createdS3AccessKey.SecretAccessKey, nil
}

func RecreateS3Credentials(ctx context.Context, bucketName string, identifier string, accessKeyId string, tenantClient *TenantClient) (accesskeyId string, key string, secret string, err error) {
	log := log.FromContext(ctx).WithValues("func", "RecreateS3Credentials")
	log.V(1).Info(fmt.Sprintf("Recreating s3 credentials for bucket %s", bucketName))

	_, bucketAdminUser := getBucketGroupAndAdminUserName(identifier)

	user, err := tenantClient.Users().GetByName(ctx, bucketAdminUser)
	if err != nil {
		log.Error(err, "Failed to get user by name")
		return "", "", "", err
	}

	// delete the existing s3 credentials.
	err = tenantClient.S3AccessKeys().DeleteForUser(ctx, *user.Id, accessKeyId)
	if err != nil {
		log.Error(err, "Failed to delete s3 access key")
		return "", "", "", err
	}

	// create new s3 credentials.
	return CreateS3Credentials(ctx, bucketName, identifier, tenantClient)
}

// the operator makes sure this is only called as admin, else it will just create s3 keypairs for the user calling it.
func CreateAdminS3Credentials(ctx context.Context, tenantClient *TenantClient) (accesskeyId string, key string, secret string, err error) {
	log := log.FromContext(ctx).WithValues("func", "CreateS3Credentials")
	log.V(1).Info("Creating admin s3 credentials")

	adminUser, err := tenantClient.Users().GetByName(ctx, adminUserName)
	if err != nil {
		log.Error(err, "Failed to get admin user by name")
		return "", "", "", err
	}

	s3AccessKey := models.S3AccessKey{}
	createdS3AccessKey, err := tenantClient.S3AccessKeys().CreateForUser(ctx, *adminUser.Id, &s3AccessKey)
	if err != nil {
		log.Error(err, "Failed to create s3 access key")
		return "", "", "", err
	}

	log.V(1).Info(fmt.Sprintf("S3 accessKey created: id=%s", *createdS3AccessKey.Id))
	return *createdS3AccessKey.Id, *createdS3AccessKey.AccessKey, *createdS3AccessKey.SecretAccessKey, nil
}

func RecreateAdminS3Credentials(ctx context.Context, accessKeyId string, tenantClient *TenantClient) (accesskeyId string, key string, secret string, err error) {
	log := log.FromContext(ctx).WithValues("func", "RecreateS3Credentials")
	log.V(1).Info("Recreating admin s3 credentials")

	adminUser, err := tenantClient.Users().GetByName(ctx, adminUserName)
	if err != nil {
		log.Error(err, "Failed to get admin user by name")
		return "", "", "", err
	}

	// delete the existing s3 credentials.
	err = tenantClient.S3AccessKeys().DeleteForUser(ctx, *adminUser.Id, accessKeyId)
	if err != nil {
		log.Error(err, "Failed to delete s3 access key")
		return "", "", "", err
	}

	// create new s3 credentials.
	return CreateAdminS3Credentials(ctx, tenantClient)
}

// check the api if the bucket exists.
func BucketExists(ctx context.Context, bucketName string, tenantClient *TenantClient) (bool, error) {
	log := log.FromContext(ctx).WithValues("func", "BucketExists")
	log.V(1).Info(fmt.Sprintf("Checking if bucket %s exists", bucketName))

	// if the bucket was not found an error is returned thus the bucket does not exist.
	_, err := tenantClient.Bucket().GetByName(ctx, bucketName)
	if err != nil {
		if strings.HasSuffix(err.Error(), "not found") {
			log.V(1).Info(fmt.Sprintf("Bucket %s does not exist", bucketName))
			return false, nil
		}

		log.Error(err, "Failed to get bucket")
		return false, err
	}

	log.V(1).Info(fmt.Sprintf("Bucket %s exists", bucketName))
	return true, nil
}

func FetchBucketUsage(ctx context.Context, bucketName string, tenantClient *TenantClient) (*BucketUsage, error) {
	log := log.FromContext(ctx).WithValues("func", "FetchBucketUsage")
	log.V(1).Info(fmt.Sprintf("Fetching bucket usage for bucket %s", bucketName))

	bucketUsage, err := tenantClient.Bucket().GetUsage(ctx, bucketName)
	if err != nil {
		log.Error(err, "Failed to fetch bucket usage")
		return nil, err
	}

	log.V(1).Info("Bucket usage fetched")
	return bucketUsage, nil
}

func GetBucketObjectCount(bucketUsage *BucketUsage) int {
	log := log.FromContext(context.Background()).WithValues("func", "GetBucketObjectCount")
	log.V(1).Info("Getting bucket object count")

	if bucketUsage == nil {
		log.V(1).Info("Bucket usage not fetched, returning 0")
		return 0
	}

	count := *bucketUsage.ObjectCount
	log.V(1).Info(fmt.Sprintf("Bucket object count: %d", count))
	return count
}

func GetBucketUsedBytes(bucketUsage *BucketUsage) int64 {
	log := log.FromContext(context.Background()).WithValues("func", "GetBucketUsedBytes")
	log.V(1).Info("Getting bucket used bytes")

	if bucketUsage == nil {
		log.V(1).Info("Bucket usage not fetched, returning 0")
		return 0
	}

	bytes := *bucketUsage.DataBytes
	log.V(1).Info(fmt.Sprintf("Bucket used bytes: %d", bytes))
	return bytes
}

func DeleteBucket(ctx context.Context, bucketName string, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "DeleteBucket")
	log.V(1).Info(fmt.Sprintf("Deleting bucket %s", bucketName))

	return tenantClient.Bucket().Delete(ctx, bucketName)
}

func DeleteBucketAdmin(ctx context.Context, identifier string, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "DeleteUser")

	bucketAdminUserName, bucketAdminGroupName := getBucketGroupAndAdminUserName(identifier)

	log.V(1).Info(fmt.Sprintf("Deleting user %s", bucketAdminUserName))
	err := deleteUserByName(ctx, bucketAdminUserName, tenantClient)
	if err != nil {
		log.Error(err, "Failed to delete user")
		return err
	}

	log.V(1).Info(fmt.Sprintf("Deleting group %s", bucketAdminGroupName))
	return deleteGroupByName(ctx, bucketAdminGroupName, tenantClient)
}

func getBucketGroupAndAdminUserName(identifier string) (groupName string, userName string) {
	groupName = fmt.Sprintf("bucket-admin-%s", identifier)
	userName = fmt.Sprintf("bucket-admin-%s", identifier)
	return groupName, userName
}

// DrainBucket initiates an asynchronous bucket drain operation in StorageGrid.
// This tells StorageGrid to start deleting all objects in the bucket.
func DrainBucket(ctx context.Context, bucketName string, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "DrainBucket")
	log.V(1).Info(fmt.Sprintf("Initiating drain for bucket %s", bucketName))

	// Call SDK: POST /org/containers/{name}/delete-objects with deleteObjects=true
	status, err := tenantClient.Bucket().Drain(ctx, bucketName)
	if err != nil {
		log.Error(err, "Failed to initiate bucket drain")
		return fmt.Errorf("failed to initiate drain: %w", err)
	}

	log.V(1).Info("Bucket drain initiated successfully", "isDeletingObjects", *status.IsDeletingObjects)
	return nil
}

// DrainStatus represents the current state of a bucket drain operation.
// This is a grid-layer abstraction over the SDK's BucketDeleteObjectStatus.
type DrainStatus struct {
	// IsDeletingObjects indicates whether a drain operation is currently active.
	IsDeletingObjects bool
	// InitialObjectCount is the number of objects when drain started (0 if unknown).
	InitialObjectCount int32
	// InitialObjectBytes is the total bytes when drain started (0 if unknown).
	InitialObjectBytes int64
}

// GetBucketDrainStatus retrieves current drain operation status from StorageGrid.
// Returns the drain status including whether objects are being deleted and counts.
func GetBucketDrainStatus(ctx context.Context, bucketName string, tenantClient *TenantClient) (*DrainStatus, error) {
	log := log.FromContext(ctx).WithValues("func", "GetBucketDrainStatus")
	log.V(1).Info(fmt.Sprintf("Fetching drain status for bucket %s", bucketName))

	// Call SDK: GET /org/containers/{name}/delete-objects
	sdkStatus, err := tenantClient.Bucket().DrainStatus(ctx, bucketName)
	if err != nil {
		log.Error(err, "Failed to get bucket drain status")
		return nil, fmt.Errorf("failed to get drain status: %w", err)
	}

	// Convert SDK type to grid type
	status := &DrainStatus{
		IsDeletingObjects:  sdkStatus.IsDeletingObjects != nil && *sdkStatus.IsDeletingObjects,
		InitialObjectCount: 0,
		InitialObjectBytes: 0,
	}
	if sdkStatus.InitialObjectCount != nil {
		status.InitialObjectCount = *sdkStatus.InitialObjectCount
	}
	if sdkStatus.InitialObjectBytes != nil {
		status.InitialObjectBytes = *sdkStatus.InitialObjectBytes
	}

	log.V(1).Info(fmt.Sprintf("Drain status fetched: isDeletingObjects=%v, objectCount=%d",
		status.IsDeletingObjects, status.InitialObjectCount))
	return status, nil
}

// CancelBucketDrain cancels an active drain operation.
func CancelBucketDrain(ctx context.Context, bucketName string, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "CancelBucketDrain")
	log.V(1).Info(fmt.Sprintf("Canceling drain for bucket %s", bucketName))

	// Call SDK: POST /org/containers/{name}/delete-objects with deleteObjects=false
	status, err := tenantClient.Bucket().CancelDrain(ctx, bucketName)
	if err != nil {
		log.Error(err, "Failed to cancel bucket drain")
		return fmt.Errorf("failed to cancel drain: %w", err)
	}

	log.V(1).Info("Bucket drain canceled successfully", "isDeletingObjects", *status.IsDeletingObjects)
	return nil
}

// objectLockToSDK converts an S3ObjectLockBucketSpec into the SDK's BucketS3ObjectLockSettings.
// Returns nil when the spec is nil or Mode is Disabled (omits the field from the request body).
func objectLockToSDK(spec *s3v1alpha1.S3ObjectLockBucketSpec) *models.BucketS3ObjectLockSettings {
	if spec == nil || spec.Mode == "" || spec.Mode == s3v1alpha1.S3ObjectLockModeDisabled {
		return nil
	}

	enabled := true
	return &models.BucketS3ObjectLockSettings{
		Enabled: &enabled,
		DefaultRetentionSetting: &models.BucketS3ObjectLockDefaultRetentionSettings{
			Mode: strings.ToLower(string(spec.Mode)),
			Days: spec.RetentionInDays,
		},
	}
}

// GetBucketObjectLock fetches the current S3 Object Lock configuration for a bucket.
func GetBucketObjectLock(ctx context.Context, bucketName string, tenantClient *TenantClient) (*models.BucketS3ObjectLockSettings, error) {
	log := log.FromContext(ctx).WithValues("func", "GetBucketObjectLock")
	log.V(1).Info(fmt.Sprintf("Fetching object lock for bucket %s", bucketName))

	settings, err := tenantClient.Bucket().GetObjectLock(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch object lock for bucket %s: %w", bucketName, err)
	}
	return settings, nil
}

// UpdateBucketObjectLock updates the S3 Object Lock configuration for an existing bucket.
// Note: StorageGRID applies retention changes to NEW objects only; existing objects keep their prior settings.
// The caller is responsible for translating the desired spec into SDK settings via DesiredBucketObjectLock.
func UpdateBucketObjectLock(ctx context.Context, bucketName string, desired *models.BucketS3ObjectLockSettings, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "UpdateBucketObjectLock")
	log.V(1).Info(fmt.Sprintf("Updating object lock for bucket %s", bucketName))

	if _, err := tenantClient.Bucket().UpdateObjectLock(ctx, bucketName, desired); err != nil {
		return fmt.Errorf("failed to update object lock for bucket %s: %w", bucketName, err)
	}
	return nil
}

// DesiredBucketObjectLock builds the SDK settings struct from a bucket spec for use with UpdateBucketObjectLock.
// When spec is nil/Disabled it returns settings with Enabled=false and no default retention.
func DesiredBucketObjectLock(spec *s3v1alpha1.S3ObjectLockBucketSpec) *models.BucketS3ObjectLockSettings {
	if settings := objectLockToSDK(spec); settings != nil {
		return settings
	}
	disabled := false
	return &models.BucketS3ObjectLockSettings{Enabled: &disabled}
}

// ObjectLockSettingsEqual compares two SDK object-lock settings for drift detection.
// Treats nil and Disabled-with-no-retention as equivalent.
func ObjectLockSettingsEqual(a, b *models.BucketS3ObjectLockSettings) bool {
	enabled := func(s *models.BucketS3ObjectLockSettings) bool {
		if s == nil || s.Enabled == nil {
			return false
		}
		return *s.Enabled
	}
	if enabled(a) != enabled(b) {
		return false
	}
	// When neither is enabled, default retention is irrelevant.
	if !enabled(a) {
		return true
	}
	defA := a.DefaultRetentionSetting
	defB := b.DefaultRetentionSetting
	if defA == nil && defB == nil {
		return true
	}
	if defA == nil || defB == nil {
		return false
	}
	return defA.Mode == defB.Mode && defA.Days == defB.Days && defA.Years == defB.Years
}
