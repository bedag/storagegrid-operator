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

	models "github.com/bedag/storagegrid-sdk-go/models"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// CreateAccessUserIfNotExists creates a group with a custom S3 policy and a user in that group.
// This is used for S3Access resources where the policy is rendered from referenced S3Policies.
// The identifier is used to build unique group/user names (e.g. <bucketName>-<s3accessUID short>).
func CreateAccessUserIfNotExists(ctx context.Context, identifier string, policyStatements []PolicyStatement, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "CreateAccessUserIfNotExists")
	log.V(1).Info(fmt.Sprintf("Creating access user for identifier %s", identifier))

	groupName, userName := identifier, identifier

	policies := &models.TenantGroupPolicies{
		Management: &models.TenantGroupManagementPolicy{},
		S3: &models.S3Policy{
			ID:        &groupName,
			Statement: policyStatementsToS3Statements(policyStatements),
		},
	}

	groupId, err := createGroupIfNotExists(ctx, groupName, policies, tenantClient)
	if err != nil {
		log.Error(err, "Failed to create access group")
		return err
	}

	userId, err := createUserIfNotExists(ctx, userName, groupId, tenantClient)
	if err != nil {
		log.Error(err, "Failed to create access user")
		return err
	}

	log.V(1).Info(fmt.Sprintf("Access user created: userId=%s, groupId=%s", userId, groupId))
	return nil
}

// UpdateAccessGroupPolicy updates the S3 policy on an existing access group.
func UpdateAccessGroupPolicy(ctx context.Context, groupName string, policyStatements []PolicyStatement, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "UpdateAccessGroupPolicy")

	group, err := tenantClient.Groups().GetByName(ctx, groupName)
	if err != nil {
		log.Error(err, "Failed to get group by name")
		return err
	}

	group.Policies = &models.TenantGroupPolicies{
		Management: &models.TenantGroupManagementPolicy{},
		S3: &models.S3Policy{
			ID:        &groupName,
			Statement: policyStatementsToS3Statements(policyStatements),
		},
	}

	_, err = tenantClient.Groups().Update(ctx, group)
	if err != nil {
		log.Error(err, "Failed to update group policy")
		return err
	}

	log.V(1).Info("Access group policy updated")
	return nil
}

// DeleteAccessUser deletes the user and group created for an S3Access resource.
func DeleteAccessUser(ctx context.Context, userName string, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "DeleteAccessUser")

	// Make username and group equal
	groupName := userName

	log.V(1).Info(fmt.Sprintf("Deleting access user %s", userName))
	err := deleteUserByName(ctx, userName, tenantClient)
	if err != nil {
		log.Error(err, "Failed to delete access user")
		return err
	}

	log.V(1).Info(fmt.Sprintf("Deleting access group %s", groupName))
	return deleteGroupByName(ctx, groupName, tenantClient)
}

// CreateAccessS3Credentials creates S3 credentials for the access user.
func CreateAccessS3Credentials(ctx context.Context, userName string, tenantClient *TenantClient) (accessKeyId string, key string, secret string, err error) {
	log := log.FromContext(ctx).WithValues("func", "CreateAccessS3Credentials")

	user, err := tenantClient.Users().GetByName(ctx, userName)
	if err != nil {
		log.Error(err, "Failed to get access user by name")
		return "", "", "", err
	}

	s3AccessKey := models.S3AccessKey{}
	createdKey, err := tenantClient.S3AccessKeys().CreateForUser(ctx, *user.Id, &s3AccessKey)
	if err != nil {
		log.Error(err, "Failed to create S3 access key")
		return "", "", "", err
	}

	log.V(1).Info(fmt.Sprintf("S3 access key created: id=%s", *createdKey.Id))
	return *createdKey.Id, *createdKey.AccessKey, *createdKey.SecretAccessKey, nil
}

// RecreateAccessS3Credentials deletes existing credentials and creates new ones.
func RecreateAccessS3Credentials(ctx context.Context, userName string, existingAccessKeyId string, tenantClient *TenantClient) (accessKeyId string, key string, secret string, err error) {
	log := log.FromContext(ctx).WithValues("func", "RecreateAccessS3Credentials")

	user, err := tenantClient.Users().GetByName(ctx, userName)
	if err != nil {
		log.Error(err, "Failed to get access user by name")
		return "", "", "", err
	}

	err = tenantClient.S3AccessKeys().DeleteForUser(ctx, *user.Id, existingAccessKeyId)
	if err != nil {
		log.Error(err, "Failed to delete existing S3 access key")
		return "", "", "", err
	}

	return CreateAccessS3Credentials(ctx, userName, tenantClient)
}
