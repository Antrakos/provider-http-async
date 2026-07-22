/*
Copyright 2022 The Crossplane Authors.

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

package interfaces_test

import (
	"testing"

	clusterrequestv1alpha1 "github.com/Antrakos/provider-http-async/apis/cluster/request/v1alpha1"
	clusterrequestv1alpha2 "github.com/Antrakos/provider-http-async/apis/cluster/request/v1alpha2"
	namespacedrequestv1alpha2 "github.com/Antrakos/provider-http-async/apis/namespaced/request/v1alpha2"

	"github.com/Antrakos/provider-http-async/apis/interfaces"
)

func TestClusterScopedInterfaceImplementations(t *testing.T) {
	var _ interfaces.MappedHTTPRequestSpec = (*clusterrequestv1alpha2.AsyncRequestParameters)(nil)
	var _ interfaces.MappedHTTPRequestSpec = (*clusterrequestv1alpha1.AsyncRequestParameters)(nil)

	var _ interfaces.HTTPResponse = (*clusterrequestv1alpha2.Response)(nil)
	var _ interfaces.HTTPResponse = (*clusterrequestv1alpha1.Response)(nil)

	var _ interfaces.HTTPMapping = (*clusterrequestv1alpha2.Mapping)(nil)
	var _ interfaces.HTTPMapping = (*clusterrequestv1alpha1.Mapping)(nil)

	var _ interfaces.HTTPPayload = (*clusterrequestv1alpha2.Payload)(nil)
	var _ interfaces.HTTPPayload = (*clusterrequestv1alpha1.Payload)(nil)
}

func TestNamespacedInterfaceImplementations(t *testing.T) {
	var _ interfaces.MappedHTTPRequestSpec = (*namespacedrequestv1alpha2.AsyncRequestParameters)(nil)

	var _ interfaces.HTTPResponse = (*namespacedrequestv1alpha2.Response)(nil)
	var _ interfaces.HTTPMapping = (*namespacedrequestv1alpha2.Mapping)(nil)
	var _ interfaces.HTTPPayload = (*namespacedrequestv1alpha2.Payload)(nil)
}

func TestClusterScopedV1Alpha2SpecificInterfaces(t *testing.T) {
	var _ interfaces.ResponseCheckAware = (*clusterrequestv1alpha2.AsyncRequestParameters)(nil)
	var _ interfaces.AsyncRequestStatus = (*clusterrequestv1alpha2.AsyncRequest)(nil)
}

func TestNamespacedV1Alpha2SpecificInterfaces(t *testing.T) {
	var _ interfaces.ResponseCheckAware = (*namespacedrequestv1alpha2.AsyncRequestParameters)(nil)
	var _ interfaces.AsyncRequestStatus = (*namespacedrequestv1alpha2.AsyncRequest)(nil)
}

func TestClusterScopedMethodAccess(t *testing.T) {
	params := &clusterrequestv1alpha2.AsyncRequestParameters{
		Mappings: []clusterrequestv1alpha2.Mapping{
			{URL: "https://example.com", Method: "GET"},
		},
	}

	var spec interfaces.MappedHTTPRequestSpec = params
	mappings := spec.GetMappings()
	if len(mappings) != 1 {
		t.Errorf("Expected 1 mapping, got %d", len(mappings))
	}
	if mappings[0].GetURL() != "https://example.com" {
		t.Errorf("Expected URL 'https://example.com', got '%s'", mappings[0].GetURL())
	}
}

func TestNamespacedMethodAccess(t *testing.T) {
	params := &namespacedrequestv1alpha2.AsyncRequestParameters{
		Mappings: []namespacedrequestv1alpha2.Mapping{
			{URL: "https://api.example.com", Method: "POST"},
		},
	}

	var spec interfaces.MappedHTTPRequestSpec = params
	mappings := spec.GetMappings()
	if len(mappings) != 1 {
		t.Errorf("Expected 1 mapping, got %d", len(mappings))
	}
	if mappings[0].GetURL() != "https://api.example.com" {
		t.Errorf("Expected URL 'https://api.example.com', got '%s'", mappings[0].GetURL())
	}
}
