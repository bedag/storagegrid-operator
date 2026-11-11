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
	"context"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
	"github.com/bedag/storagegrid-operator/pkg/grid"
)

// APIProvider returns the grid API for a StorageGrid.
type APIProvider func(ctx context.Context, sg *s3v1alpha1.StorageGrid) (grid.API, error)

// ClientCache keeps one long-lived grid client per StorageGrid so scrapes reuse
// its login token. It is used by the metrics collector only.
type ClientCache struct {
	reader  client.Reader
	mu      sync.Mutex
	entries map[string]cachedClient // By StorageGrid name.
}

type cachedClient struct {
	key string
	api grid.API
}

// NewClientCache returns an empty ClientCache reading credentials through c.
func NewClientCache(c client.Reader) *ClientCache {
	return &ClientCache{reader: c, entries: map[string]cachedClient{}}
}

// Get returns the cached client for sg, creating a new one when the StorageGrid
// was recreated, its endpoint changed or its credentials secret changed.
func (c *ClientCache) Get(ctx context.Context, sg *s3v1alpha1.StorageGrid) (grid.API, error) {
	// Read the Secret directly: kube.FetchCredentialsFromSecret needs a client.Client and hides the resourceVersion.
	var secret corev1.Secret
	ref := sg.Spec.SecretRef
	if err := c.reader.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &secret); err != nil {
		return nil, err
	}
	key := string(sg.UID) + "/" + secret.ResourceVersion + "/" + sg.Spec.ManagementEndpoint

	c.mu.Lock()
	e, ok := c.entries[sg.Name]
	c.mu.Unlock()
	if ok && e.key == key {
		return e.api, nil
	}

	gc, err := grid.InitGridClient(sg.Name, string(secret.Data["username"]), string(secret.Data["password"]), sg.Spec.ManagementEndpoint)
	if err != nil {
		return nil, err
	}
	api := grid.NewAPI(gc)
	c.mu.Lock()
	c.entries[sg.Name] = cachedClient{key: key, api: api}
	c.mu.Unlock()
	return api, nil
}

// Drop discards the cached client of the named StorageGrid.
func (c *ClientCache) Drop(sgName string) {
	c.mu.Lock()
	delete(c.entries, sgName)
	c.mu.Unlock()
}
