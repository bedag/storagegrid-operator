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
	"fmt"
	"net/http"
	"strings"
)

// isNotFound returns true if the error indicates the resource was not found (HTTP 404).
// Works with the StorageGrid SDK error format: "... not found" or "API error: 404 ...".
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.HasSuffix(msg, "not found") || strings.Contains(msg, "404")
}

// hasStatus reports whether err is the SDK's error for an HTTP status code,
// which it formats as "API error: <status> (code: <code>)".
func hasStatus(err error, code int) bool {
	return err != nil && strings.Contains(err.Error(), fmt.Sprintf("(code: %d)", code))
}

// IsUnauthorized returns true if the grid rejected the client's credentials (HTTP 401).
// A 403 is a permission problem a new login would not fix, so it does not count.
func IsUnauthorized(err error) bool {
	return hasStatus(err, http.StatusUnauthorized)
}
