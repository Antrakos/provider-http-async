package disposablerequest

import (
	"context"
	"strings"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/test"
	corev1 "k8s.io/api/core/v1"
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

func (m *mockHTTPClient) SendRequest(_ context.Context, method string, url httpClient.Data, body, headers httpClient.Data, _ *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
	if m.err != nil {
		return httpClient.HttpDetails{}, m.err
	}
	resp := m.trigger
	if method == "GET" {
		resp = m.poll
	}
	return httpClient.HttpDetails{
		HttpResponse: resp,
		HttpRequest:  httpClient.HttpRequest{Method: method, URL: url.Decrypted.(string)},
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

// newInjectingKube returns a MockClient that simulates a persistent Secret store
// (Get returns the current state, Update commits it), plus two getters: the final
// Secret written via Update, and every status update snapshot (the polling anchor at
// each persist). GetOrCreateSecret reads the Secret through Get and writes it back
// through Update, so the captured Secret is the observable end state of the injection.
func newInjectingKube() (*test.MockClient, func() *corev1.Secret, func() []*v1alpha2.AsyncDisposableRequest) {
	var storedSecret *corev1.Secret
	var statusSnapshots []*v1alpha2.AsyncDisposableRequest
	var clusterSecret *corev1.Secret

	kube := &test.MockClient{
		MockGet: func(_ context.Context, key client.ObjectKey, obj client.Object) error {
			if secret, ok := obj.(*corev1.Secret); ok {
				if clusterSecret == nil {
					clusterSecret = &corev1.Secret{ObjectMeta: v1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}
				}
				clusterSecret.DeepCopyInto(secret)
			}
			return nil
		},
		MockUpdate: func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
			if secret, ok := obj.(*corev1.Secret); ok {
				secret.DeepCopyInto(clusterSecret)
				storedSecret = secret.DeepCopy()
			}
			return nil
		},
		MockStatusUpdate: func(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
			if dr, ok := obj.(*v1alpha2.AsyncDisposableRequest); ok {
				statusSnapshots = append(statusSnapshots, dr.DeepCopy())
			}
			return nil
		},
	}
	return kube,
		func() *corev1.Secret { return storedSecret },
		func() []*v1alpha2.AsyncDisposableRequest { return statusSnapshots }
}

// TestDeployAction_PollingInjectsFromTriggerResponse is the parity regression test:
// the trigger (create) response must feed the secret injection configs on the polling
// path, exactly as AsyncRequest's mutate path and the non-polling DisposableRequest
// path already do. A show-once credential returned only by the trigger response is
// captured before the loop starts, survives finalization, and never appears raw in
// status: the persisted anchor and status.response carry placeholders instead.
func TestDeployAction_PollingInjectsFromTriggerResponse(t *testing.T) {
	cr := newPollingCR()
	cr.Spec.ForProvider.SecretInjectionConfigs = []common.SecretInjectionConfig{{
		SecretRef: common.SecretRef{Name: "deploy-secret", Namespace: "testns"},
		KeyMappings: []common.KeyInjection{{
			SecretKey:            "token",
			ResponseJQ:           `.body.token`,
			MissingFieldStrategy: common.PreserveMissingField,
		}},
	}}
	http := &mockHTTPClient{
		trigger: httpClient.HttpResponse{StatusCode: 200, Body: `{"name":"operations/op-123","token":"sekrit"}`},
		poll:    httpClient.HttpResponse{StatusCode: 200, Body: `{"done":true}`},
	}
	kube, storedSecret, statusSnapshots := newInjectingKube()

	err := DeployAction(newSvcCtx(context.Background(), kube, http), service.NewDisposableRequestCRContext(cr))
	if err != nil {
		t.Fatalf("DeployAction returned error: %v", err)
	}
	if !cr.Status.Synced {
		t.Errorf("expected Synced=true, got false")
	}
	if storedSecret() == nil {
		t.Fatalf("expected the secret to be written")
	}
	if got := string(storedSecret().Data["token"]); got != "sekrit" {
		t.Errorf("expected secret token=sekrit, got %q", got)
	}

	placeholder := "{{deploy-secret:testns:token}}"
	snapshots := statusSnapshots()
	if len(snapshots) < 2 {
		t.Fatalf("expected at least 2 status updates (anchor persist + finalize), got %d", len(snapshots))
	}
	anchor := string(snapshots[0].Status.Polling.Response.Raw)
	if !strings.Contains(anchor, placeholder) {
		t.Errorf("expected anchor to hold the placeholder, got %s", anchor)
	}
	if strings.Contains(anchor, "sekrit") {
		t.Errorf("anchor leaked the raw secret: %s", anchor)
	}
	if snapshots[len(snapshots)-1].Status.Polling.Response != nil {
		t.Errorf("expected anchor cleared after finalize")
	}
	// The poll response contains no token, so the placeholder lives in the anchor;
	// status.response must simply never carry the raw value.
	if body := cr.Status.Response.Body; strings.Contains(body, "sekrit") {
		t.Errorf("status.response leaked the raw secret: %q", body)
	}
}

// TestDeployAction_PollingStillInjectsFromPollResponse guards the pre-existing
// behavior the trigger injection must not break: fields that exist only in the final
// poll response are still injected at finalization, and an injected poll-response
// value is placeholder-ized in status.response (inject runs before the status writes).
func TestDeployAction_PollingStillInjectsFromPollResponse(t *testing.T) {
	cr := newPollingCR()
	cr.Spec.ForProvider.SecretInjectionConfigs = []common.SecretInjectionConfig{{
		SecretRef: common.SecretRef{Name: "deploy-secret", Namespace: "testns"},
		KeyMappings: []common.KeyInjection{
			{
				SecretKey:            "modelId",
				ResponseJQ:           `.body.response.deployedModel.id`,
				MissingFieldStrategy: common.PreserveMissingField,
			},
			{
				SecretKey:            "apiKey",
				ResponseJQ:           `.body.response.deployedModel.apiKey`,
				MissingFieldStrategy: common.PreserveMissingField,
			},
		},
	}}
	http := &mockHTTPClient{
		trigger: httpClient.HttpResponse{StatusCode: 200, Body: `{"name":"operations/op-123"}`},
		poll:    httpClient.HttpResponse{StatusCode: 200, Body: `{"done":true,"response":{"deployedModel":{"id":"dm-999","apiKey":"poll-secret"}}}`},
	}
	kube, storedSecret, _ := newInjectingKube()

	err := DeployAction(newSvcCtx(context.Background(), kube, http), service.NewDisposableRequestCRContext(cr))
	if err != nil {
		t.Fatalf("DeployAction returned error: %v", err)
	}
	if !cr.Status.Synced {
		t.Errorf("expected Synced=true, got false")
	}
	if storedSecret() == nil {
		t.Fatalf("expected the secret to be written")
	}
	if got := string(storedSecret().Data["modelId"]); got != "dm-999" {
		t.Errorf("expected secret modelId=dm-999, got %q", got)
	}
	if got := string(storedSecret().Data["apiKey"]); got != "poll-secret" {
		t.Errorf("expected secret apiKey=poll-secret, got %q", got)
	}
	if body := cr.Status.Response.Body; strings.Contains(body, "poll-secret") {
		t.Errorf("status.response leaked the raw poll secret: %q", body)
	} else if !strings.Contains(body, "{{deploy-secret:testns:apiKey}}") {
		t.Errorf("expected status.response to hold the placeholder, got %q", body)
	}
}

// TestDeployAction_PollingDefaultDeleteWipesShowOnceKey pins the parity behavior for
// the default missingFieldStrategy: the trigger injection writes the key, the
// finalize injection evaluates the poll response, finds the field missing, and deletes
// the key — exactly how AsyncRequest and base Request treat show-once fields when
// later responses lack them. The API server applies "delete" when the field is
// omitted; unit tests bypass defaults, so it is set explicitly here.
func TestDeployAction_PollingDefaultDeleteWipesShowOnceKey(t *testing.T) {
	cr := newPollingCR()
	cr.Spec.ForProvider.SecretInjectionConfigs = []common.SecretInjectionConfig{{
		SecretRef: common.SecretRef{Name: "deploy-secret", Namespace: "testns"},
		KeyMappings: []common.KeyInjection{{
			SecretKey:            "token",
			ResponseJQ:           `.body.token`,
			MissingFieldStrategy: common.DeleteMissingField,
		}},
	}}
	http := &mockHTTPClient{
		trigger: httpClient.HttpResponse{StatusCode: 200, Body: `{"name":"operations/op-123","token":"sekrit"}`},
		poll:    httpClient.HttpResponse{StatusCode: 200, Body: `{"done":true}`},
	}
	kube, storedSecret, _ := newInjectingKube()

	err := DeployAction(newSvcCtx(context.Background(), kube, http), service.NewDisposableRequestCRContext(cr))
	if err != nil {
		t.Fatalf("DeployAction returned error: %v", err)
	}
	if !cr.Status.Synced {
		t.Errorf("expected Synced=true, got false")
	}
	if storedSecret() == nil {
		t.Fatalf("expected the secret to be written")
	}
	if _, exists := storedSecret().Data["token"]; exists {
		t.Errorf("expected the token key to be deleted by the finalize injection (parity with sibling kinds)")
	}
}
