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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRouteTemplate(t *testing.T) {
	cases := map[string]string{
		"/api/v4/grid/accounts/71737386352718534094/usage?includeBucketDetail=true": "/grid/accounts/{id}/usage",
		"/api/v4/org/containers/my-bucket/usage":                                    "/org/containers/{name}/usage",
		"/api/v4/grid/metric-query?query=x":                                         "/grid/metric-query",
		"/api/v4/grid/accounts-cache":                                               "/grid/accounts-cache",
		"/api/v4/authorize":                                                         "/authorize",
		"/api/v4/something/new":                                                     "other",
	}
	for in, want := range cases {
		if got := RouteTemplate(in); got != want {
			t.Errorf("RouteTemplate(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestInstrumentedClientRecords(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v4/authorize" {
			w.Header().Set("Expires", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat))
			_, _ = w.Write([]byte(`{"status":"success","apiVersion":"4.0","data":"token"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"success","apiVersion":"4.0","data":[]}`))
	}))
	defer srv.Close()

	c, err := InitGridClient("primary-grid", "u", "p", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tenant().ListUsage(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(requests.WithLabelValues("primary-grid", "GET", "/grid/accounts-cache", "200")); got != 1 {
		t.Errorf("accounts-cache count = %v, want 1", got)
	}
	if got := testutil.ToFloat64(requests.WithLabelValues("primary-grid", "POST", "/authorize", "200")); got != 1 {
		t.Errorf("authorize count = %v, want 1", got)
	}
}

func TestIsUnauthorized(t *testing.T) {
	cases := map[string]bool{
		"API error: 401 Unauthorized (code: 401)": true,
		"API error: 403 Forbidden (code: 403)":    false,
		"API error: 404 Not Found (code: 404)":    false,
		`tenant 34017401 usage: Get "https://grid/api/v4/grid/accounts/34017401/usage": context deadline exceeded`: false,
	}
	for msg, want := range cases {
		if got := IsUnauthorized(errString(msg)); got != want {
			t.Errorf("IsUnauthorized(%q) = %v, want %v", msg, got, want)
		}
	}
	if IsUnauthorized(nil) {
		t.Error("nil should not be unauthorized")
	}
}

type errString string

func (e errString) Error() string { return string(e) }
