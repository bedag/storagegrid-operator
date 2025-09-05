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
	// no once, because this needs to be called for each tenant
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
