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

	models "github.com/yehlo/storagegrid-sdk-go/models"
)

type BucketUsage = models.BucketStats

// CreateBucket creates a new bucket with the specified name and region and returns admin credentials for s3 access.
func CreateBucket(ctx context.Context, name string, region string, retentionInDays int32, tenantClient *TenantClient) (err error) {
	log := log.FromContext(ctx).WithValues("func", "CreateBucket")
	log.V(1).Info(fmt.Sprintf("Creating bucket: name=%s, region=%s, retentionInDays=%d", name, region, retentionInDays))

	bucket := models.Bucket{
		Name:   name,
		Region: region,
	}

	// add retention settings if retentionInDays is configured.
	if retentionInDays > 0 {
		enabled := true
		bucket.S3ObjectLock = &models.BucketS3ObjectLockSettings{
			Enabled: &enabled,
			DefaultRetentionSetting: &models.BucketS3ObjectLockDefaultRetentionSettings{
				Mode: "compliance",
				Days: retentionInDays,
			},
		}
	}

	// no need to keep the result.
	_, err = tenantClient.Bucket.Create(ctx, &bucket)
	if err != nil {
		log.Error(err, "Failed to create bucket")
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

	user, err := tenantClient.Users.GetByName(ctx, bucketAdminUser)
	if err != nil {
		log.Error(err, "Failed to get user by name")
		return "", "", "", err
	}

	createdS3AccessKey, err := tenantClient.S3AccessKeys.CreateForUser(ctx, *user.Id, &s3AccessKey)
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

	user, err := tenantClient.Users.GetByName(ctx, bucketAdminUser)
	if err != nil {
		log.Error(err, "Failed to get user by name")
		return "", "", "", err
	}

	// delete the existing s3 credentials.
	err = tenantClient.S3AccessKeys.DeleteForUser(ctx, *user.Id, accessKeyId)
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

	adminUser, err := tenantClient.Users.GetByName(ctx, adminUserName)
	if err != nil {
		log.Error(err, "Failed to get admin user by name")
		return "", "", "", err
	}

	s3AccessKey := models.S3AccessKey{}
	createdS3AccessKey, err := tenantClient.S3AccessKeys.CreateForUser(ctx, *adminUser.Id, &s3AccessKey)
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

	adminUser, err := tenantClient.Users.GetByName(ctx, adminUserName)
	if err != nil {
		log.Error(err, "Failed to get admin user by name")
		return "", "", "", err
	}

	// delete the existing s3 credentials.
	err = tenantClient.S3AccessKeys.DeleteForUser(ctx, *adminUser.Id, accessKeyId)
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
	_, err := tenantClient.Bucket.GetByName(ctx, bucketName)
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

	bucketUsage, err := tenantClient.Bucket.GetUsage(ctx, bucketName)
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

	return tenantClient.Bucket.Delete(ctx, bucketName)
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
