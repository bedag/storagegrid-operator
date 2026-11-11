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
	"crypto/tls"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storagegrid_operator_grid_requests_total",
		Help: "StorageGrid management API requests by outcome.",
	}, []string{"storagegrid", "method", "route", "code"})
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "storagegrid_operator_grid_request_duration_seconds",
		Help:    "StorageGrid management API request latency.",
		Buckets: prometheus.ExponentialBucketsRange(0.05, 30, 10),
	}, []string{"storagegrid", "method", "route"})
	requestErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storagegrid_operator_grid_request_errors_total",
		Help: "StorageGrid management API requests that failed at transport level.",
	}, []string{"storagegrid", "method", "route"})
)

// RequestCollectors returns the grid request collectors for registration.
func RequestCollectors() []prometheus.Collector {
	return []prometheus.Collector{requests, requestDuration, requestErrors}
}

type routePattern struct {
	re       *regexp.Regexp
	template string
}

// routePatterns is ordered; the first match wins.
var routePatterns = []routePattern{
	{regexp.MustCompile(`^/grid/accounts/[^/]+/usage$`), "/grid/accounts/{id}/usage"},
	{regexp.MustCompile(`^/org/containers/[^/]+/usage$`), "/org/containers/{name}/usage"},
	{regexp.MustCompile(`^/grid/metric-query$`), "/grid/metric-query"},
	{regexp.MustCompile(`^/grid/accounts-cache$`), "/grid/accounts-cache"},
	{regexp.MustCompile(`^/authorize$`), "/authorize"},
}

var apiPrefix = regexp.MustCompile(`^/api/v\d+`)

// RouteTemplate maps a request path to a bounded route label, or "other".
func RouteTemplate(path string) string {
	path, _, _ = strings.Cut(path, "?")
	path = apiPrefix.ReplaceAllString(path, "")
	for _, p := range routePatterns {
		if p.re.MatchString(path) {
			return p.template
		}
	}
	return "other"
}

// instrumentedTransport records request count, latency and transport errors per route.
type instrumentedTransport struct {
	storageGrid string
	next        http.RoundTripper
}

func (t instrumentedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	route := RouteTemplate(req.URL.Path)
	start := time.Now()
	resp, err := t.next.RoundTrip(req)
	requestDuration.WithLabelValues(t.storageGrid, req.Method, route).Observe(time.Since(start).Seconds())
	if err != nil {
		requestErrors.WithLabelValues(t.storageGrid, req.Method, route).Inc()
		return resp, err
	}
	requests.WithLabelValues(t.storageGrid, req.Method, route, strconv.Itoa(resp.StatusCode)).Inc()
	return resp, nil
}

// newHTTPClient builds an instrumented client that skips TLS verification, like the SDK's WithSkipSSL.
func newHTTPClient(storageGrid string) *http.Client {
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // matches the previous WithSkipSSL behavior.
	return &http.Client{Transport: instrumentedTransport{storageGrid: storageGrid, next: base}}
}
