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
	client "github.com/yehlo/storagegrid-sdk-go/client"
	models "github.com/yehlo/storagegrid-sdk-go/models"
)

type GridClient = client.GridClient
type TenantClient = client.TenantClient

func InitGridClient(username string, password string, endpoint string) (*GridClient, error) {
	creds := models.Credentials{Username: username, Password: password}
	opts := []client.ClientOption{
		client.WithCredentials(&creds),
		client.WithEndpoint(endpoint),
		client.WithSkipSSL(),
	}

	gridClient, err := client.NewGridClient(opts...)
	if err != nil {
		return nil, err
	}

	return gridClient, nil
}

func InitTenantClient(username string, password string, endpoint string, tenantId string) (*TenantClient, error) {
	// no once, because this needs to be called for each tenant.
	creds := models.Credentials{Username: username, Password: password, AccountId: &tenantId}
	opts := []client.ClientOption{
		client.WithCredentials(&creds),
		client.WithEndpoint(endpoint),
		client.WithSkipSSL(),
	}

	tenantClient, err := client.NewTenantClient(opts...)
	if err != nil {
		return nil, err
	}

	return tenantClient, nil
}
