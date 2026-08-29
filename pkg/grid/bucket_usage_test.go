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

import "testing"

// The SDK models these counts as pointers with omitempty, so StorageGrid omitting the
// field yields nil. These accessors gate whether a bucket or tenant is considered empty
// and therefore safe to delete, so a nil must read as "unknown, treated as 0" rather
// than panicking the reconcile.
func TestGetBucketObjectCountHandlesNilFields(t *testing.T) {
	if got := GetBucketObjectCount(nil); got != 0 {
		t.Errorf("nil usage: got %d, want 0", got)
	}

	if got := GetBucketObjectCount(&BucketUsage{}); got != 0 {
		t.Errorf("nil ObjectCount: got %d, want 0", got)
	}

	count := 7
	if got := GetBucketObjectCount(&BucketUsage{ObjectCount: &count}); got != 7 {
		t.Errorf("populated: got %d, want 7", got)
	}
}

func TestGetBucketUsedBytesHandlesNilFields(t *testing.T) {
	if got := GetBucketUsedBytes(nil); got != 0 {
		t.Errorf("nil usage: got %d, want 0", got)
	}

	if got := GetBucketUsedBytes(&BucketUsage{}); got != 0 {
		t.Errorf("nil DataBytes: got %d, want 0", got)
	}
}

func TestTenantUsageAccessorsHandleNil(t *testing.T) {
	if got := GetTenantObjectCount(nil); got != 0 {
		t.Errorf("nil usage: got %d, want 0", got)
	}

	if got := GetTenantObjectCount(&TenantUsage{}); got != 0 {
		t.Errorf("nil ObjectCount: got %d, want 0", got)
	}

	if got := GetTenantUsedBytes(&TenantUsage{}); got != 0 {
		t.Errorf("nil DataBytes: got %d, want 0", got)
	}

	if got := GetBucketCount(nil); got != 0 {
		t.Errorf("nil usage: got %d, want 0", got)
	}
}
