package disposablerequest

import (
	"context"
	"strings"
	"testing"

	"github.com/pkg/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane/crossplane-runtime/v2/pkg/test"

	"github.com/Antrakos/provider-http-async/apis/cluster/disposablerequest/v1alpha2"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	"github.com/Antrakos/provider-http-async/internal/service"
)

// countingHTTPClient returns a fixed response (or error) and records how many
// requests were sent, so tests can assert one-off firing semantics.
type countingHTTPClient struct {
	resp  httpClient.HttpResponse
	err   error
	calls int
}

func (c *countingHTTPClient) SendRequest(_ context.Context, method, url string, _, _ httpClient.Data, _ *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
	c.calls++
	if c.err != nil {
		return httpClient.HttpDetails{}, c.err
	}
	return httpClient.HttpDetails{
		HttpResponse: c.resp,
		HttpRequest:  httpClient.HttpRequest{Method: method, URL: url},
	}, nil
}

func plainCR(mods ...func(*v1alpha2.AsyncDisposableRequest)) *v1alpha2.AsyncDisposableRequest {
	cr := &v1alpha2.AsyncDisposableRequest{
		ObjectMeta: v1.ObjectMeta{Name: "plain", Namespace: "testns"},
		Spec: v1alpha2.AsyncDisposableRequestSpec{
			ForProvider: v1alpha2.AsyncDisposableRequestParameters{
				URL:    "https://api.example.com/thing",
				Method: "POST",
				Body:   `{"k":"v"}`,
			},
		},
	}
	for _, m := range mods {
		m(cr)
	}
	return cr
}

func okKube() *test.MockClient {
	return &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}
}

func run(t *testing.T, cr *v1alpha2.AsyncDisposableRequest, kube client.Client, http httpClient.Client) error {
	t.Helper()
	return DeployAction(newSvcCtx(context.Background(), kube, http), service.NewDisposableRequestCRContext(cr))
}

func TestDeployAction_AlreadySynced(t *testing.T) {
	cr := plainCR(func(c *v1alpha2.AsyncDisposableRequest) { c.Status.Synced = true })
	http := &countingHTTPClient{resp: httpClient.HttpResponse{StatusCode: 200, Body: `{}`}}

	if err := run(t, cr, okKube(), http); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if http.calls != 0 {
		t.Errorf("expected no request when already synced, got %d calls", http.calls)
	}
	if !cr.Status.Synced {
		t.Errorf("expected Synced to remain true")
	}
}

func TestDeployAction_SuccessfulSync(t *testing.T) {
	cr := plainCR(func(c *v1alpha2.AsyncDisposableRequest) { c.Spec.ForProvider.ExpectedResponse = `.body.ok == true` })
	http := &countingHTTPClient{resp: httpClient.HttpResponse{StatusCode: 200, Body: `{"ok":true}`}}

	if err := run(t, cr, okKube(), http); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if http.calls != 1 {
		t.Errorf("expected exactly 1 request, got %d", http.calls)
	}
	if !cr.Status.Synced {
		t.Errorf("expected Synced=true")
	}
	if cr.Status.Response.StatusCode != 200 {
		t.Errorf("expected status.response.statusCode=200, got %d", cr.Status.Response.StatusCode)
	}
}

func TestDeployAction_HTTPErrorStatus(t *testing.T) {
	cr := plainCR()
	http := &countingHTTPClient{resp: httpClient.HttpResponse{StatusCode: 400, Body: `{"error":"bad"}`}}

	err := run(t, cr, okKube(), http)
	if err == nil {
		t.Fatalf("expected an error for a 4xx status code")
	}
	if cr.Status.Synced {
		t.Errorf("expected Synced=false on HTTP error status")
	}
	if cr.Status.Failed != 1 {
		t.Errorf("expected Failed=1, got %d", cr.Status.Failed)
	}
}

func TestDeployAction_AllowedStatusCode(t *testing.T) {
	cr := plainCR(func(c *v1alpha2.AsyncDisposableRequest) {
		c.Spec.ForProvider.AllowedStatusCodes = []int{404}
		c.Spec.ForProvider.ExpectedResponse = `.statusCode == 404`
	})
	http := &countingHTTPClient{resp: httpClient.HttpResponse{StatusCode: 404, Body: `{}`}}

	if err := run(t, cr, okKube(), http); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cr.Status.Synced {
		t.Errorf("expected Synced=true when the status code is explicitly allowed")
	}
}

func TestDeployAction_TransportError(t *testing.T) {
	cr := plainCR()
	http := &countingHTTPClient{err: errors.New("boom")}

	err := run(t, cr, okKube(), http)
	if err == nil {
		t.Fatalf("expected the transport error to be returned")
	}
	if cr.Status.Failed != 1 {
		t.Errorf("expected Failed=1, got %d", cr.Status.Failed)
	}
}

func TestDeployAction_ValidationMismatch(t *testing.T) {
	cr := plainCR(func(c *v1alpha2.AsyncDisposableRequest) { c.Spec.ForProvider.ExpectedResponse = `.body.ok == true` })
	http := &countingHTTPClient{resp: httpClient.HttpResponse{StatusCode: 200, Body: `{"ok":false}`}}

	if err := run(t, cr, okKube(), http); err != nil {
		t.Fatalf("validation mismatch should not return a Go error, got %v", err)
	}
	if cr.Status.Synced {
		t.Errorf("expected Synced=false on validation mismatch")
	}
	if cr.Status.Failed != 1 {
		t.Errorf("expected Failed=1, got %d", cr.Status.Failed)
	}
	if !strings.Contains(cr.Status.Error, "retries limit") {
		t.Errorf("expected a validation error message, got %q", cr.Status.Error)
	}
}

// TestDeployAction_OneOffTerminalOnFailure proves rollbackRetriesLimit:1 gives a strict
// one-off: the request fires exactly once, and after the recorded failure a subsequent
// reconcile is a no-op (the trigger is not re-issued).
func TestDeployAction_OneOffTerminalOnFailure(t *testing.T) {
	limit := int32(1)
	cr := plainCR(func(c *v1alpha2.AsyncDisposableRequest) {
		c.Spec.ForProvider.RollbackRetriesLimit = &limit
		c.Spec.ForProvider.ExpectedResponse = `.body.ok == true`
	})
	http := &countingHTTPClient{resp: httpClient.HttpResponse{StatusCode: 200, Body: `{"ok":false}`}}
	kube := okKube()

	// First reconcile: fires once, records the failure.
	if err := run(t, cr, kube, http); err != nil {
		t.Fatalf("unexpected error on first attempt: %v", err)
	}
	if http.calls != 1 || cr.Status.Failed != 1 {
		t.Fatalf("expected 1 call and Failed=1 after first attempt, got calls=%d failed=%d", http.calls, cr.Status.Failed)
	}

	// Second reconcile: retries limit reached -> no-op, trigger not re-issued.
	if err := run(t, cr, kube, http); err != nil {
		t.Fatalf("unexpected error on second attempt: %v", err)
	}
	if http.calls != 1 {
		t.Errorf("expected the trigger to fire only once with rollbackRetriesLimit=1, got %d calls", http.calls)
	}
}
