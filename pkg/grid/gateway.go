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
	"slices"
	"strings"

	models "github.com/bedag/storagegrid-sdk-go/models"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	allowlistMode string = "allowList"
)

type GatewayConfig = models.GatewayConfig
type HAGroup = models.HAGroup
type GWServerConfig = models.GWServerConfig

type Gateway = struct {
	Gateway      *GatewayConfig
	HAGroups     []HAGroup
	ServerConfig *GWServerConfig
}

// Currently not needed - would be for a whole list of gateways.
// func GetGateways(ctx context.Context) (, error) {.
// 	log := log.FromContext(ctx).WithValues("func", "GetGateways").
// 	log.V(1).Info("Fetching list of gateway").

// 	gridClient, err := GetGridClient().
// 	if err != nil {.
// 		log.Error(err, "Failed to get grid client").
// 		return "", err.
// 	}
// 	return gridClient.Gateway().List(ctx).
// }

func FetchGateway(ctx context.Context, gatewayID string, gridClient *GridClient) (*Gateway, error) {
	log := log.FromContext(ctx).WithValues("func", "FetchGateway")
	log.V(1).Info(fmt.Sprintf("Fetching gateway with id %s", gatewayID))

	gatewayConfig := &Gateway{
		Gateway:      nil,
		HAGroups:     []models.HAGroup{},
		ServerConfig: nil,
	}

	gw, err := gridClient.Gateway().GetGatewayConfigById(ctx, gatewayID)
	if err != nil {
		log.Error(err, "Failed to get gateway")
		return nil, err
	}

	gatewayConfig.Gateway = gw

	srvConfig, err := fetchGWServerConfig(ctx, gw, gridClient)
	if err != nil {
		log.Error(err, "Failed to get server configuration")
		return nil, err
	}
	gatewayConfig.ServerConfig = srvConfig

	HAGroups, err := fetchHAGroups(ctx, gw, gridClient)
	if err != nil {
		log.Error(err, "Failed to get backing HA groups")
		return nil, err
	}

	gatewayConfig.HAGroups = HAGroups

	return gatewayConfig, nil
}

func fetchGWServerConfig(ctx context.Context, gw *GatewayConfig, gridClient *GridClient) (*GWServerConfig, error) {
	log := log.FromContext(ctx).WithValues("func", "fetchGWServerConfig")
	log.V(1).Info("Fetching gateway server configuration")

	srvConfig, err := gridClient.Gateway().GetGatewayServerConfig(ctx, gw.Id)
	if err != nil {
		log.Error(err, "Failed to get server configuration")
		return nil, err
	}

	return srvConfig, nil
}

func fetchHAGroups(ctx context.Context, gw *GatewayConfig, gridClient *GridClient) ([]HAGroup, error) {
	log := log.FromContext(ctx).WithValues("func", "fetchHAGroups")
	log.V(1).Info("Fetching HA groups")

	// get all hagroups mapped to the gateway.hagroups.
	hagroups := []HAGroup{}
	for _, hagroup := range *gw.PinTargets.HaGroups {
		hagroup, err := gridClient.HAGroup().GetById(ctx, hagroup)
		if err != nil {
			log.Error(err, "Failed to get HA group")
			return nil, err
		}
		hagroups = append(hagroups, *hagroup)
	}

	return hagroups, nil
}

func GetDisplayname(gw *Gateway) string {
	return *gw.Gateway.DisplayName
}

//nolint:all
func GetPort(gw *Gateway) int32 {
	return int32(*gw.Gateway.Port)
}

func GetVIPs(gw *Gateway) []string {
	vips := []string{}
	for _, hagroup := range gw.HAGroups {
		vips = append(vips, *hagroup.VirtualIps...)
	}
	return vips
}

func IsTLSEnabled(gw *Gateway) bool {
	return *gw.Gateway.Secure
}

func IsIPv4AccessEnabled(gw *Gateway) bool {
	return *gw.Gateway.EnableIPv4
}

func IsIPv6AccessEnabled(gw *Gateway) bool {
	return *gw.Gateway.EnableIPv6
}

func DropUntrustedNetworks(gw *Gateway) bool {
	return *gw.Gateway.ClosedOnUntrustedClientNetwork
}

func GetS3Endpoints(gw *Gateway) []string {
	// endpoints are the subject alt name from the cert.
	// they are always formatted as DNS: <endpoint>.
	endpoints := []string{}
	for _, endpoint := range *gw.ServerConfig.PlaintextCertData.Metadata.ServerCertificateDetails.SubjectAltNames {
		endpoints = append(endpoints, strings.ReplaceAll(endpoint, "DNS:", ""))
	}

	return endpoints
}

// Currently not used as it could be too flaky.
func IsPathStyleAccessEnabled(gw *Gateway) bool {
	// the evaluation of this is somewhat tricky.
	// according to storagegrid docs the default is virtualhost style access (so false).
	// but this only works if the subject altname uses a wildcard cert.
	// eg. we're checking if the endpoint is a wildcard cert.
	// if it is, we can assume that path style access is not used.
	// https://docs.netapp.com/us-en/storagegrid-116/admin/configuring-s3-api-endpoint-domain-names.html.

	// we can use GetS3Endpoints because this would not remove the wildcard.
	for _, endpoint := range GetS3Endpoints(gw) {
		// maybe need to adjust in the future, since this is a very basic check.
		if strings.HasPrefix(endpoint, "*") {
			return false
		}
	}

	return true
}

func AddTenantToAllowlist(ctx context.Context, tenantID string, gw *Gateway, gridClient *GridClient) error {
	log := log.FromContext(ctx).WithValues("func", "AddTenantToAllowlist")
	log.V(1).Info(fmt.Sprintf("Adding tenant %s to allowlist", tenantID))

	if isTenantInAllowlist(tenantID, gw) {
		// tenant was probably added to the allowlist on another reconciliation or through other means.
		log.V(1).Info(fmt.Sprintf("Tenant %s is already in the allowlist - skipping", tenantID))
		return nil
	}

	// prepare serverConfig obj.
	*gw.ServerConfig.AccountRestrictions = append(*gw.ServerConfig.AccountRestrictions, tenantID)
	gw.ServerConfig.AccountRestrictionMode = &allowlistMode

	// update server config.
	srvConfig, err := gridClient.Gateway().UpdateGatewayServerConfig(ctx, gw.Gateway.Id, gw.ServerConfig)
	if err != nil {
		log.Error(err, "Failed to update server configuration")
		return err
	}

	gw.ServerConfig = srvConfig

	return nil
}

func RemoveTenantFromAllowlist(ctx context.Context, tenantID string, gw *Gateway, gridClient *GridClient) error {
	log := log.FromContext(ctx).WithValues("func", "RemoveTenantFromAllowlist")
	log.V(1).Info(fmt.Sprintf("Removing tenant %s from allowlist", tenantID))

	if !isTenantInAllowlist(tenantID, gw) {
		// tenant was probably removed from the allowlist on another reconciliation or through other means.
		log.V(1).Info(fmt.Sprintf("Tenant %s is already not in the allowlist - skipping", tenantID))
		return nil
	}

	// prepare serverConfig obj.
	for i, tenant := range *gw.ServerConfig.AccountRestrictions {
		if tenant == tenantID {
			// remove this element from the slice by copying the elements before and after it.
			*gw.ServerConfig.AccountRestrictions = append((*gw.ServerConfig.AccountRestrictions)[:i], (*gw.ServerConfig.AccountRestrictions)[i+1:]...)
			break
		}
	}
	gw.ServerConfig.AccountRestrictionMode = &allowlistMode

	// update server config.
	srvConfig, err := gridClient.Gateway().UpdateGatewayServerConfig(ctx, gw.Gateway.Id, gw.ServerConfig)
	if err != nil {
		log.Error(err, "Failed to update server configuration")
		return err
	}

	gw.ServerConfig = srvConfig

	return nil
}

func isTenantInAllowlist(tenantID string, gw *Gateway) bool {
	return slices.Contains(*gw.ServerConfig.AccountRestrictions, tenantID)
}
