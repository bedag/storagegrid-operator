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

// Package metrics holds the operator's Prometheus collectors.
package metrics

// ReservedLabels are label names the operator sets itself and that users may not
// request through StorageGrid.spec.metrics.grid.allowedLabels.
var ReservedLabels = []string{
	"namespace",
	"storagegrid",
	"s3tenantclass",
	"s3tenantaccount",
	"s3tenant",
	"s3bucket",
	"s3access",
	"tenant_id",
	"step",
}
