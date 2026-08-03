package disposablerequest

import (
	"context"
	"strings"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/test"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Antrakos/provider-http-async/apis/cluster/disposablerequest/v1alpha2"
	"github.com/Antrakos/provider-http-async/apis/common"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	"github.com/Antrakos/provider-http-async/internal/service"
)

// mockHTTPClient returns canned responses keyed by HTTP method: the trigger request
// (typically POST) and the poll request (GET) can be served independently.
type mockHTTPClient struct {
	trigger httpClient.HttpResponse
	poll    httpClient.HttpResponse
	err     error
}

func (m *mockHTTPClient) SendRequest(_ context.Context, method, url string, body, headers httpClient.Data, _ *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
	if m.err != nil {
		return httpClient.HttpDetails{}, m.err
	}
	resp := m.trigger
	if method == "GET" {
		resp = m.poll
	}
	return httpClient.HttpDetails{
		HttpResponse: resp,
		HttpRequest:  httpClient.HttpRequest{Method: method, URL: url},
	}, nil
}

func newPollingCR() *v1alpha2.AsyncDisposableRequest {
	return &v1alpha2.AsyncDisposableRequest{
		ObjectMeta: v1.ObjectMeta{Name: "deploy-model", Namespace: "testns"},
		Spec: v1alpha2.AsyncDisposableRequestSpec{
			ForProvider: v1alpha2.AsyncDisposableRequestParameters{
				URL:              "https://api.example.com/endpoints:deployModel",
				Method:           "POST",
				Body:             `{"model":"m1"}`,
				ExpectedResponse: `.body.done == true`,
				Polling: &common.Polling{
					URL:   `"https://api.example.com/" + .response.body.name`,
					Done:  `.poll.response.body.done == true`,
					Error: `.poll.response.body.error`,
				},
			},
		},
	}
}

func newSvcCtx(ctx context.Context, kube client.Client, http httpClient.Client) *service.ServiceContext {
	return service.NewServiceContext(ctx, kube, logging.NewNopLogger(), http, nil)
}

func TestDeployAction_PollingSuccess(t *testing.T) {
	cr := newPollingCR()
	http := &mockHTTPClient{
		trigger: httpClient.HttpResponse{StatusCode: 200, Body: `{"name":"operations/op-123"}`},
		poll:    httpClient.HttpResponse{StatusCode: 200, Body: `{"done":true,"response":{"deployedModel":{"id":"dm-999"}}}`},
	}
	kube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}

	err := DeployAction(newSvcCtx(context.Background(), kube, http), service.NewDisposableRequestCRContext(cr))
	if err != nil {
		t.Fatalf("DeployAction returned error: %v", err)
	}
	if !cr.Status.Synced {
		t.Errorf("expected Synced=true, got false")
	}
	if cr.Status.Polling.Response != nil {
		t.Errorf("expected polling anchor cleared, got %v", cr.Status.Polling.Response)
	}
}

func TestDeployAction_PollingOperationError(t *testing.T) {
	cr := newPollingCR()
	http := &mockHTTPClient{
		trigger: httpClient.HttpResponse{StatusCode: 200, Body: `{"name":"operations/op-err"}`},
		poll:    httpClient.HttpResponse{StatusCode: 200, Body: `{"done":true,"error":{"code":9,"message":"boom"}}`},
	}
	kube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}

	err := DeployAction(newSvcCtx(context.Background(), kube, http), service.NewDisposableRequestCRContext(cr))
	if err == nil {
		t.Fatalf("expected operation error, got nil")
	}
	if cr.Status.Synced {
		t.Errorf("expected Synced=false on operation error")
	}
	if cr.Status.Failed != 1 {
		t.Errorf("expected Failed=1, got %d", cr.Status.Failed)
	}
	if cr.Status.Polling.Response != nil {
		t.Errorf("expected polling anchor cleared on error, got %v", cr.Status.Polling.Response)
	}
	if !strings.Contains(cr.Status.Error, "operation failed") {
		t.Errorf("expected operation error message, got %q", cr.Status.Error)
	}
}

// TestDeployAction_PollingExposesStatus verifies polling expressions can reference
// .status (parity with AsyncRequest): the CR pre-seeds status.failed and the
// polling.done expression keys off it to complete the operation.
func TestDeployAction_PollingExposesStatus(t *testing.T) {
	cr := newPollingCR()
	cr.Status.Failed = 3
	cr.Spec.ForProvider.Polling.Done = `.status.failed == 3 and .poll.response.body.done == true`
	http := &mockHTTPClient{
		trigger: httpClient.HttpResponse{StatusCode: 200, Body: `{"name":"operations/op-status"}`},
		poll:    httpClient.HttpResponse{StatusCode: 200, Body: `{"done":true}`},
	}
	kube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}

	err := DeployAction(newSvcCtx(context.Background(), kube, http), service.NewDisposableRequestCRContext(cr))
	if err != nil {
		t.Fatalf("DeployAction returned error: %v", err)
	}
	if !cr.Status.Synced {
		t.Errorf("expected Synced=true when polling.done keyed off .status.failed, got false")
	}
}

func TestDeployAction_PollingBadURLIsError(t *testing.T) {
	cr := newPollingCR()
	// polling.url resolves to a scheme-less path -> terminal config error surfaced as error.
	cr.Spec.ForProvider.Polling.URL = `.response.body.name`
	http := &mockHTTPClient{
		trigger: httpClient.HttpResponse{StatusCode: 200, Body: `{"name":"operations/op-123"}`},
	}
	kube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}

	err := DeployAction(newSvcCtx(context.Background(), kube, http), service.NewDisposableRequestCRContext(cr))
	if err == nil {
		t.Fatalf("expected error for invalid polling.url, got nil")
	}
	if cr.Status.Synced {
		t.Errorf("expected Synced=false for invalid polling.url")
	}
}

func TestDeployAction_PollingResumeUsesAnchor(t *testing.T) {
	cr := newPollingCR()
	// Simulate a prior reconcile that persisted the anchor: DeployAction must resume
	// polling (GET) without re-firing the trigger POST.
	cr.SetPollingResponse(map[string]interface{}{
		"body":       map[string]interface{}{"name": "operations/op-123"},
		"statusCode": 200,
	})
	started := v1.Now()
	cr.SetOperationStartedAt(&started)

	http := &mockHTTPClient{
		trigger: httpClient.HttpResponse{StatusCode: 500, Body: `should-not-be-used`},
		poll:    httpClient.HttpResponse{StatusCode: 200, Body: `{"done":true,"response":{"deployedModel":{"id":"dm-resumed"}}}`},
	}
	kube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}

	err := DeployAction(newSvcCtx(context.Background(), kube, http), service.NewDisposableRequestCRContext(cr))
	if err != nil {
		t.Fatalf("DeployAction returned error: %v", err)
	}
	if !cr.Status.Synced {
		t.Errorf("expected Synced=true after resume")
	}
}

func TestDeployAction_NoPollingPlainRequest(t *testing.T) {
	cr := &v1alpha2.AsyncDisposableRequest{
		ObjectMeta: v1.ObjectMeta{Name: "plain", Namespace: "testns"},
		Spec: v1alpha2.AsyncDisposableRequestSpec{
			ForProvider: v1alpha2.AsyncDisposableRequestParameters{
				URL:              "https://api.example.com/thing",
				Method:           "POST",
				ExpectedResponse: `.body.ok == true`,
			},
		},
	}
	http := &mockHTTPClient{
		trigger: httpClient.HttpResponse{StatusCode: 200, Body: `{"ok":true,"id":"thing-1"}`},
	}
	kube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}

	err := DeployAction(newSvcCtx(context.Background(), kube, http), service.NewDisposableRequestCRContext(cr))
	if err != nil {
		t.Fatalf("DeployAction returned error: %v", err)
	}
	if !cr.Status.Synced {
		t.Errorf("expected Synced=true for plain request")
	}
	if cr.Status.Polling.Response != nil {
		t.Errorf("plain request should not set polling anchor")
	}
}
