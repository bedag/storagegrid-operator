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

package metrics

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
	"github.com/bedag/storagegrid-operator/pkg/grid"
)

func gridEnabled(queries ...s3v1alpha1.GridMetricQuery) *s3v1alpha1.StorageGrid {
	sg := storageGrid("primary-grid", false)
	sg.Spec.Metrics.Grid = &s3v1alpha1.GridMetrics{Enabled: true, AllowedLabels: []string{"site_name"}, Queries: queries}
	return sg
}

func TestForwardKeepsOnlyAllowed(t *testing.T) {
	ms, err := forwardQuery("m", []grid.Sample{{Labels: map[string]string{"site_name": "Bern, MSA", "instance": "adm1"}, Value: 3}},
		[]string{"site_name"}, "primary-grid")
	if err != nil || len(ms) != 1 {
		t.Fatalf("got %v, %v", ms, err)
	}
	var m dto.Metric
	if err := ms[0].Write(&m); err != nil {
		t.Fatal(err)
	}
	if len(m.Label) != 2 || m.Label[0].GetName() != "site_name" || m.Label[0].GetValue() != "Bern, MSA" ||
		m.Label[1].GetName() != "storagegrid" || m.Label[1].GetValue() != "primary-grid" {
		t.Errorf("labels = %v", m.Label)
	}
}

func TestForwardCollisionSkips(t *testing.T) {
	before := testutil.ToFloat64(GridQueryErrors.WithLabelValues("collide-grid", "m"))
	ms, err := forwardQuery("m", []grid.Sample{
		{Labels: map[string]string{"instance": "a"}, Value: 1}, {Labels: map[string]string{"instance": "b"}, Value: 2},
	}, nil, "collide-grid")
	if err == nil || ms != nil {
		t.Fatalf("want error and no metrics, got %v, %v", ms, err)
	}
	if d := testutil.ToFloat64(GridQueryErrors.WithLabelValues("collide-grid", "m")) - before; d != 1 {
		t.Errorf("counter delta %v, want 1", d)
	}
}

func TestGridDisabledNoQueries(t *testing.T) {
	api := newFakeAPI()
	sg := gridEnabled()
	sg.Spec.Metrics.Grid.Enabled = false
	got := series(t, NewGridCollector(fakeReader(t, sg), staticAPI(api), func(string) {}, nil, defaultOpts))
	mustNotContain(t, got, `step="grid"`)
	if n := api.queryCalls.Load(); n != 0 {
		t.Errorf("Query called %d times", n)
	}
}

func TestDefaultsWhenEmpty(t *testing.T) {
	api := newFakeAPI()
	// Endpoint health is served by the fake directly, so Query only sees the grid step.
	got := series(t, NewGridCollector(fakeReader(t, gridEnabled()), staticAPI(api), func(string) {}, nil, defaultOpts))
	if n := api.queryCalls.Load(); int(n) != len(DefaultGridQueries) {
		t.Errorf("Query called %d times, want %d", n, len(DefaultGridQueries))
	}
	mustHave(t, got, p+`grid_scrape_success{step="grid",storagegrid="primary-grid"} 1`)
}

func TestFailingQueryKeepsOthers(t *testing.T) {
	api := newFakeAPI()
	api.queryErr = map[string]error{"bad": errors.New("boom")}
	api.queryResult = map[string][]grid.Sample{"good": {{Labels: map[string]string{"site_name": "Bern"}, Value: 7}}}
	sg := gridEnabled(s3v1alpha1.GridMetricQuery{Name: "bad_m", Query: "bad"}, s3v1alpha1.GridMetricQuery{Name: "good_m", Query: "good"})
	got := series(t, NewGridCollector(fakeReader(t, sg), staticAPI(api), func(string) {}, nil, defaultOpts))
	mustHave(t, got, `good_m{site_name="Bern",storagegrid="primary-grid"} 7`, p+`grid_scrape_success{step="grid",storagegrid="primary-grid"} 1`)
	mustNotContain(t, got, "bad_m")
}

func TestAllQueriesFailing(t *testing.T) {
	api := newFakeAPI()
	api.queryErr = map[string]error{"bad": errors.New("boom")}
	got := series(t, NewGridCollector(fakeReader(t, gridEnabled(s3v1alpha1.GridMetricQuery{Name: "bad_m", Query: "bad"})), staticAPI(api), func(string) {}, nil, defaultOpts))
	mustHave(t, got, p+`grid_scrape_success{step="grid",storagegrid="primary-grid"} 0`)
}
