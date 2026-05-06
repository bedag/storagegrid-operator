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

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// GetS3ObjectLockAvailable returns whether S3 Object Lock (compliance-global) is enabled
// grid-wide. Wraps GET /grid/compliance-global. A nil ComplianceEnabled in the response is
// treated as false.
func GetS3ObjectLockAvailable(ctx context.Context, gridClient *GridClient) (bool, error) {
	log := log.FromContext(ctx).WithValues("func", "GetS3ObjectLockAvailable")
	log.V(1).Info("Fetching grid-wide S3 Object Lock capability")

	settings, err := gridClient.S3ObjectLock().Get(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to fetch grid S3 Object Lock settings: %w", err)
	}
	if settings == nil || settings.ComplianceEnabled == nil {
		return false, nil
	}
	return *settings.ComplianceEnabled, nil
}
