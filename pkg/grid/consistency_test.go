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
	"testing"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

// The API enum is PascalCase while StorageGrid expects hyphenated lowercase tokens. A wrong
// mapping does not fail loudly: the PUT is rejected with a 422, or worse, the drift check
// never converges because the value read back never equals the value sent.
func TestConsistencyFromSpec(t *testing.T) {
	cases := []struct {
		spec s3v1alpha1.BucketConsistency
		want Consistency
	}{
		{s3v1alpha1.BucketConsistencyAll, "all"},
		{s3v1alpha1.BucketConsistencyStrongGlobal, "strong-global"},
		{s3v1alpha1.BucketConsistencyStrongSite, "strong-site"},
		{s3v1alpha1.BucketConsistencyReadAfterNewWrite, "read-after-new-write"},
		{s3v1alpha1.BucketConsistencyAvailable, "available"},
	}

	for _, tc := range cases {
		got, err := ConsistencyFromSpec(tc.spec)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.spec, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.spec, got, tc.want)
		}
	}
}

func TestConsistencyFromSpecRejectsUnknown(t *testing.T) {
	// An empty spec means "unmanaged" and must never be translated into a wire value:
	// doing so would make the operator write to buckets nobody asked it to touch.
	if _, err := ConsistencyFromSpec(""); err == nil {
		t.Error("empty consistency: expected an error, got nil")
	}

	if _, err := ConsistencyFromSpec(s3v1alpha1.BucketConsistency("strong-site")); err == nil {
		t.Error("wire value passed as spec value: expected an error, got nil")
	}
}

// The CRD enum and the wire mapping are declared in two different packages; this catches a
// value being added to one without the other.
func TestEveryAPIConsistencyValueIsMapped(t *testing.T) {
	all := []s3v1alpha1.BucketConsistency{
		s3v1alpha1.BucketConsistencyAll,
		s3v1alpha1.BucketConsistencyStrongGlobal,
		s3v1alpha1.BucketConsistencyStrongSite,
		s3v1alpha1.BucketConsistencyReadAfterNewWrite,
		s3v1alpha1.BucketConsistencyAvailable,
	}

	if len(consistencyBySpec) != len(all) {
		t.Errorf("mapping has %d entries, API declares %d values", len(consistencyBySpec), len(all))
	}

	for _, v := range all {
		if _, ok := consistencyBySpec[v]; !ok {
			t.Errorf("API value %q has no wire mapping", v)
		}
	}
}

// ConsistencyDefault is what a bucket is reset to when spec.consistency is cleared. If it
// drifted away from the value StorageGrid gives a fresh bucket, clearing the field would
// leave the operator fighting the grid on every reconcile.
func TestConsistencyDefaultIsGridDefault(t *testing.T) {
	if ConsistencyDefault != ConsistencyReadAfterNewWrite {
		t.Errorf("got %q, want %q", ConsistencyDefault, ConsistencyReadAfterNewWrite)
	}
}
