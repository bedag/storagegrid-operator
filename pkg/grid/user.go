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

	models "github.com/yehlo/storagegrid-sdk-go/models"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func createUser(ctx context.Context, username string, groupID string, tenantClient *TenantClient) (string, error) {
	log := log.FromContext(ctx).WithValues("func", "createUser")
	log.V(1).Info(fmt.Sprintf("Creating user %s", username))

	user := models.User{
		UniqueName: username,
		FullName:   &username,
		MemberOf:   []string{groupID},
	}

	createdUser, err := tenantClient.Users.Create(ctx, &user)
	if err != nil {
		log.Error(err, "Failed to create user")
		return "", err
	}

	log.V(1).Info(fmt.Sprintf("User created: id=%s", *createdUser.Id))
	return *createdUser.Id, nil
}

func userExists(ctx context.Context, username string, tenantClient *TenantClient) (bool, string) {
	log := log.FromContext(ctx).WithValues("func", "userExists")
	log.V(1).Info(fmt.Sprintf("Checking if user %s exists", username))

	user, err := tenantClient.Users.GetByName(ctx, username)
	if err == nil {
		log.V(1).Info(fmt.Sprintf("User %s exists", username))
		return true, *user.Id
	}

	log.V(1).Info(fmt.Sprintf("User %s does not exist", username))
	return false, ""
}

func createUserIfNotExists(ctx context.Context, username string, groupID string, tenantClient *TenantClient) (string, error) {
	log := log.FromContext(ctx).WithValues("func", "createUserIfNotExists")

	// check if the user already exists.
	exists, userID := userExists(ctx, username, tenantClient)
	if exists {
		log.V(1).Info(fmt.Sprintf("User %s already exists, validating membership", username))
		if err := ensureGroupMembership(ctx, username, groupID, tenantClient); err != nil {
			log.Error(err, "Failed to ensure group membership for existing user")
			return "", err
		}
		return userID, nil
	}

	// create the user.
	userID, err := createUser(ctx, username, groupID, tenantClient)
	if err != nil {
		log.Error(err, "Failed to create user")
		return "", err
	}

	return userID, nil
}

func ensureGroupMembership(ctx context.Context, username string, groupID string, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "ensureGroupMembership")
	log.V(1).Info(fmt.Sprintf("Ensuring user %s is member of group with id %s", username, groupID))

	// get user by name.
	user, err := tenantClient.Users.GetByName(ctx, username)
	if err != nil {
		log.Error(err, "Failed to get user by name")
		return err
	}

	// get group by id.
	group, err := tenantClient.Groups.GetById(ctx, groupID)
	if err != nil {
		log.Error(err, "Failed to get group by id")
		return err
	}

	// check if the user is already a member of the group.
	for _, gid := range user.MemberOf {
		if gid == *group.Id {
			log.V(1).Info(fmt.Sprintf("User %s is already member of group %s", username, group.DisplayName))
			return nil
		}
	}

	// add group to the memberof list.
	user.MemberOf = append(user.MemberOf, *group.Id)
	_, err = tenantClient.Users.Update(ctx, user)
	if err != nil {
		log.Error(err, "Failed to update user")
	}

	log.V(1).Info(fmt.Sprintf("User %s added to group %s", username, group.DisplayName))
	return nil
}

func deleteUserByName(ctx context.Context, username string, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "DeleteUserByName")
	log.V(1).Info(fmt.Sprintf("Deleting user %s", username))

	user, err := tenantClient.Users.GetByName(ctx, username)
	if err != nil {
		log.Error(err, "Failed to get user by name")
		return err
	}

	return tenantClient.Users.Delete(ctx, *user.Id)
}

func createGroup(ctx context.Context, groupName string, policies *models.TenantGroupPolicies, tenantClient *TenantClient) (string, error) {
	log := log.FromContext(ctx).WithValues("func", "CreateBucketAdminGroup")
	log.V(1).Info(fmt.Sprintf("Creating group %s", groupName))

	// groupname can't be longer than 32 characters.
	if len(groupName) > 32 {
		log.V(1).Info(fmt.Sprintf("Group name %s is longer than 32 characters, truncating", groupName))
		groupName = groupName[:32]
	}

	group := models.TenantGroup{
		UniqueName:  groupName,
		DisplayName: groupName,
		Policies:    policies,
	}

	createdGroup, err := tenantClient.Groups.Create(ctx, &group)
	if err != nil {
		log.Error(err, "Failed to create group")
		return "", err
	}

	log.V(1).Info(fmt.Sprintf("Group %s created: id=%s", groupName, *createdGroup.Id))
	return *createdGroup.Id, nil
}

func groupExists(ctx context.Context, groupName string, tenantClient *TenantClient) (bool, string) {
	log := log.FromContext(ctx).WithValues("func", "groupExists")
	log.V(1).Info(fmt.Sprintf("Checking if group %s exists", groupName))

	group, err := tenantClient.Groups.GetByName(ctx, groupName)
	if err == nil {
		log.V(1).Info(fmt.Sprintf("Group %s exists", groupName))
		return true, *group.Id
	}

	log.V(1).Info(fmt.Sprintf("Group %s does not exist", groupName))
	return false, ""
}

// createGroupIfNotExists checks if a group with the given name exists, and creates it if it does not.
// It returns the group ID of the existing or newly created group.
func createGroupIfNotExists(ctx context.Context, groupName string, policies *models.TenantGroupPolicies, tenantClient *TenantClient) (string, error) {
	log := log.FromContext(ctx).WithValues("func", "createGroupIfNotExists")

	// check if the group already exists.
	exists, groupID := groupExists(ctx, groupName, tenantClient)
	if exists {
		log.V(1).Info(fmt.Sprintf("Group %s already exists, skipping creation", groupName))
		return groupID, nil
	}

	// create the group.
	groupID, err := createGroup(ctx, groupName, policies, tenantClient)
	if err != nil {
		log.Error(err, "Failed to create group")
		return "", err
	}

	return groupID, nil
}

func deleteGroupByName(ctx context.Context, groupName string, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "DeleteGroupByName")
	log.V(1).Info(fmt.Sprintf("Deleting group %s", groupName))

	group, err := tenantClient.Groups.GetByName(ctx, groupName)
	if err != nil {
		log.Error(err, "Failed to get group by name")
		return err
	}

	return tenantClient.Groups.Delete(ctx, *group.Id)
}
