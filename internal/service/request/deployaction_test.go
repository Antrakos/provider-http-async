package request

import (
	"context"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/test"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Antrakos/provider-http-async/apis/cluster/request/v1alpha2"
	"github.com/Antrakos/provider-http-async/apis/common"
	"github.com/Antrakos/provider-http-async/apis/interfaces"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/Antrakos/provider-http-async/internal/service/request/polling"
)

const (
	testURL    = "https://api.example.com/users"
	testMethod = "POST"
	testBody   = `{"username": "john_doe", "email": "john@example.com"}`
	testRespID = `{"id": "123", "username": "john_doe"}`
)

var (
	testHeaders = map[string][]string{
		"Content-Type": {"application/json"},
	}
)

func TestDeployAction(t *testing.T) {
	errBoom := errors.New("boom")

	type args struct {
		ctx        context.Context
		cr         *v1alpha2.AsyncRequest
		action     string
		localKube  client.Client
		httpClient httpClient.Client
	}

	type want struct {
		err        error
		statusCode int
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"SuccessfulPOSTAction": {
			reason: "Should successfully execute POST action with JQ expressions",
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					ObjectMeta: v1.ObjectMeta{
						Name:      "test-request",
						Namespace: "testns",
					},
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{
								Body:    testBody,
								BaseUrl: testURL,
							},
							Mappings: []v1alpha2.Mapping{
								{
									Method: "POST",
									Body:   ".payload.body",
									URL:    ".payload.baseUrl",
								},
							},
						},
					},
					Status: v1alpha2.AsyncRequestStatus{},
				},
				action: "CREATE",
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				httpClient: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 201,
								Body:       testRespID,
								Headers:    testHeaders,
							},
							HttpRequest: httpClient.HttpRequest{
								Method: "POST",
								URL:    testURL,
								Body:   testBody,
							},
						}, nil
					},
				},
			},
			want: want{
				err:        nil,
				statusCode: 201,
			},
		},
		"SuccessfulGETAction": {
			reason: "Should successfully execute GET action",
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					ObjectMeta: v1.ObjectMeta{
						Name:      "test-request",
						Namespace: "testns",
					},
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{
								BaseUrl: testURL + "/123",
							},
							Mappings: []v1alpha2.Mapping{
								{
									Method: "GET",
									URL:    ".payload.baseUrl",
								},
							},
						},
					},
					Status: v1alpha2.AsyncRequestStatus{},
				},
				action: "OBSERVE",
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				httpClient: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 200,
								Body:       testRespID,
								Headers:    testHeaders,
							},
							HttpRequest: httpClient.HttpRequest{
								Method: "GET",
								URL:    testURL + "/123",
							},
						}, nil
					},
				},
			},
			want: want{
				err:        nil,
				statusCode: 200,
			},
		},
		"SuccessfulPUTAction": {
			reason: "Should successfully execute PUT action",
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					ObjectMeta: v1.ObjectMeta{
						Name:      "test-request",
						Namespace: "testns",
					},
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{
								Body:    `{"username": "john_updated"}`,
								BaseUrl: testURL + "/123",
							},
							Mappings: []v1alpha2.Mapping{
								{
									Method: "PUT",
									Body:   ".payload.body",
									URL:    ".payload.baseUrl",
								},
							},
						},
					},
					Status: v1alpha2.AsyncRequestStatus{},
				},
				action: "UPDATE",
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				httpClient: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 200,
								Body:       `{"id": "123", "username": "john_updated"}`,
								Headers:    testHeaders,
							},
							HttpRequest: httpClient.HttpRequest{
								Method: "PUT",
								URL:    testURL + "/123",
								Body:   `{"username": "john_updated"}`,
							},
						}, nil
					},
				},
			},
			want: want{
				err:        nil,
				statusCode: 200,
			},
		},
		"SuccessfulDELETEAction": {
			reason: "Should successfully execute DELETE action",
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					ObjectMeta: v1.ObjectMeta{
						Name:      "test-request",
						Namespace: "testns",
					},
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{
								BaseUrl: testURL + "/123",
							},
							Mappings: []v1alpha2.Mapping{
								{
									Method: "DELETE",
									URL:    ".payload.baseUrl",
								},
							},
						},
					},
					Status: v1alpha2.AsyncRequestStatus{},
				},
				action: "REMOVE",
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				httpClient: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 204,
								Headers:    testHeaders,
							},
							HttpRequest: httpClient.HttpRequest{
								Method: "DELETE",
								URL:    testURL + "/123",
							},
						}, nil
					},
				},
			},
			want: want{
				err:        nil,
				statusCode: 204,
			},
		},
		"HttpRequestError": {
			reason: "Should handle HTTP request errors",
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					ObjectMeta: v1.ObjectMeta{
						Name:      "test-request",
						Namespace: "testns",
					},
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{
								Body:    testBody,
								BaseUrl: testURL,
							},
							Mappings: []v1alpha2.Mapping{
								{
									Method: "POST",
									Body:   ".payload.body",
									URL:    ".payload.baseUrl",
								},
							},
						},
					},
					Status: v1alpha2.AsyncRequestStatus{},
				},
				action: "CREATE",
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				httpClient: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, errBoom
					},
				},
			},
			want: want{
				err: errBoom,
			},
		},
		"MappingNotFound": {
			reason: "Should return nil when mapping not found for action",
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					ObjectMeta: v1.ObjectMeta{
						Name:      "test-request",
						Namespace: "testns",
					},
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Mappings: []v1alpha2.Mapping{
								{
									Method: "GET",
									URL:    testURL,
								},
							},
						},
					},
					Status: v1alpha2.AsyncRequestStatus{},
				},
				action: "CREATE", // Mapping only has GET, not POST for CREATE
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				httpClient: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, nil
					},
				},
			},
			want: want{
				err: nil, // DeployAction returns nil when mapping not found (logged but not error)
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			svcCtx := service.NewServiceContext(
				tc.args.ctx,
				tc.args.localKube,
				logging.NewNopLogger(),
				tc.args.httpClient,
				nil,
			)
			crCtx := service.NewRequestCRContext(tc.args.cr)
			err := DeployAction(
				svcCtx,
				crCtx,
				tc.args.action,
			)

			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nDeployAction(...): -want error, +got error:\n%s", tc.reason, diff)
			}

			// Only check status code if we expect success
			if tc.want.err == nil && tc.want.statusCode != 0 {
				if tc.args.cr.Status.Response.StatusCode != tc.want.statusCode {
					t.Errorf("\n%s\nExpected status code %d, got %d", tc.reason, tc.want.statusCode, tc.args.cr.Status.Response.StatusCode)
				}
			}
		})
	}
}

// ---- Async / polling-specific tests ----

// crWithPolling builds an AsyncRequest whose CREATE mapping polls pollURL. When
// inFlight is non-nil it is persisted as status.polling.response, simulating a
// resume (crash / budget expiry / fixed spec) with an in-flight operation.
func crWithPolling(pollURL string, inFlight map[string]interface{}) *v1alpha2.AsyncRequest {
	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "test-request", Namespace: "testns"},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload: v1alpha2.Payload{
					Body:    testBody,
					BaseUrl: testURL,
				},
				ExternalRef: ".poll.response.body.id",
				Mappings: []v1alpha2.Mapping{
					{
						Method: "POST",
						Body:   ".payload.body",
						URL:    ".payload.baseUrl",
						Polling: &common.Polling{
							URL:  pollURL,
							Done: ".poll.response.body.done == true",
						},
					},
				},
			},
		},
	}
	if inFlight != nil {
		cr.SetPollingResponse(inFlight)
	}
	return cr
}

func mockKube() client.Client {
	return &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}
}

func TestDeployAction_AsyncCreate_DoneAfterTwoPollIterations(t *testing.T) {
	pollCalls := 0
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			if method == "POST" {
				return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
					StatusCode: 202,
					Body:       `{"name": "operations/123"}`,
				}}, nil
			}
			// Poll GET
			pollCalls++
			done := pollCalls >= 2
			body2 := `{"done": false}`
			if done {
				body2 = `{"done": true, "id": "model-789"}`
			}
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 200, Body: body2,
			}}, nil
		},
	}

	cr := crWithPolling(`"http://api/operations/123"`, nil)
	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	err := DeployAction(svcCtx, crCtx, "CREATE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pollCalls < 2 {
		t.Errorf("expected at least 2 poll iterations, got %d", pollCalls)
	}
	if cr.Status.ExternalRef != "model-789" {
		t.Errorf("expected externalRef=model-789, got %q", cr.Status.ExternalRef)
	}
	// The anchor is cleared on completion so subsequent reconciles OBSERVE, not resume.
	if cr.Status.Polling.Response != nil {
		t.Error("expected polling.response anchor cleared on completion")
	}
}

func TestDeployAction_AsyncCreate_TerminalError(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			if method == "POST" {
				return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
					StatusCode: 202, Body: `{"name": "operations/456"}`,
				}}, nil
			}
			// Poll returns done=true with an error
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 200,
				Body:       `{"done": true, "error": {"message": "quota exceeded"}}`,
			}}, nil
		},
	}

	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "test-request", Namespace: "testns"},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload: v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
				Mappings: []v1alpha2.Mapping{{
					Method: "POST", Body: ".payload.body", URL: ".payload.baseUrl",
					Polling: &common.Polling{
						URL:   `"http://api/operations/456"`,
						Done:  ".poll.response.body.done == true",
						Error: ".poll.response.body.error",
					},
				}},
			},
		},
	}

	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	err := DeployAction(svcCtx, crCtx, "CREATE")
	// Terminal errors are written to status, not returned as Go errors
	if err != nil {
		t.Fatalf("expected nil error (terminal failure written to status), got: %v", err)
	}
	// The anchor is PRESERVED across a terminal failure so a spec change resumes the
	// existing operation rather than re-creating it.
	if cr.Status.Polling.Response == nil {
		t.Error("expected polling.response anchor to be retained after a terminal poll failure")
	}
	if cr.Status.Polling.TerminalError == "" {
		t.Error("expected terminalError to be persisted")
	}
}

func TestDeployAction_CrashRecovery_SkipsMutate(t *testing.T) {
	postCalled := false
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			if method == "POST" {
				postCalled = true
			}
			// Resume poll GET — immediately done
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 200, Body: `{"done": true, "id": "model-789"}`,
			}}, nil
		},
	}

	// In-flight anchor already set (simulates a crash after the first POST): the persisted
	// mutate response carries the operation name, and polling.url is recomputed from it.
	cr := crWithPolling(`"http://api/" + .response.body.name`, map[string]interface{}{
		"body": map[string]interface{}{"name": "operations/123"},
	})
	cr.Spec.ForProvider.ExternalRef = ".poll.response.body.id"

	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	err := DeployAction(svcCtx, crCtx, "CREATE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if postCalled {
		t.Error("POST (mutate) must NOT be called when a polling.response anchor is already set (crash recovery)")
	}
}

// TestDeployAction_OrphanRecovery_NoDuplicate is the flagship PRD test: a successful
// mutate that cannot (yet) be polled must never mint a duplicate remote resource, no
// matter how many reconciles or spec edits follow. The cycle:
//  1. CREATE POST succeeds (202) → anchor persisted, but polling.url is bad (scheme-less) → terminal.
//  2. Operator fixes polling.url (spec change) → resume with the retained anchor → done.
//
// Assert POST fires exactly once across the whole cycle.
func TestDeployAction_OrphanRecovery_NoDuplicate(t *testing.T) {
	postCalls := 0
	// The poll server reports done immediately once reached.
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			if method == "POST" {
				postCalls++
				return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
					StatusCode: 202,
					Body:       `{"name": "projects/x/operations/789"}`,
				}}, nil
			}
			// Poll GET — done immediately.
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 200, Body: `{"done": true, "id": "model-789"}`,
			}}, nil
		},
	}

	// Step 1: bad polling.url (bare path, no scheme) — terminal failure, anchor retained.
	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "test-orphan", Namespace: "testns", Generation: 1},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload:     v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
				ExternalRef: ".poll.response.body.id",
				Mappings: []v1alpha2.Mapping{{
					Method: "POST", Body: ".payload.body", URL: ".payload.baseUrl",
					Polling: &common.Polling{
						URL:  `.response.body.name`, // bare path — unpollable
						Done: ".poll.response.body.done == true",
					},
				}},
			},
		},
	}
	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	if err := DeployAction(svcCtx, crCtx, "CREATE"); err != nil {
		t.Fatalf("step 1: unexpected error: %v", err)
	}
	if postCalls != 1 {
		t.Fatalf("step 1: expected exactly 1 POST, got %d", postCalls)
	}
	if cr.Status.Polling.Response == nil {
		t.Fatal("step 1: expected polling.response anchor retained after bad-url terminal failure")
	}
	if cr.Status.Polling.TerminalError == "" {
		t.Error("step 1: expected terminalError set for bad polling.url")
	}

	// Step 2: operator fixes polling.url (bump generation, clear terminal state as the
	// reconciler would on generation drift) and resumes — the retained anchor is reused,
	// so POST must NOT fire again.
	cr.Generation = 2
	cr.Status.Polling.TerminalError = ""
	cr.Spec.ForProvider.Mappings[0].Polling.URL = `"http://api/" + .response.body.name`

	if err := DeployAction(svcCtx, crCtx, "CREATE"); err != nil {
		t.Fatalf("step 2: unexpected error: %v", err)
	}
	if postCalls != 1 {
		t.Errorf("step 2: expected POST to still be 1 (resume, no duplicate), got %d", postCalls)
	}
	if cr.Status.ExternalRef != "model-789" {
		t.Errorf("step 2: expected externalRef=model-789, got %q", cr.Status.ExternalRef)
	}
	if cr.Status.Polling.Response != nil {
		t.Error("step 2: expected anchor cleared on completion")
	}
}

func TestDeployAction_BackwardCompat_NoPollingNoOIDC(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 201, Body: `{"id": "42"}`,
			}}, nil
		},
	}

	// Plain sync manifest — no polling, no oidc, no externalRef
	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "test-plain", Namespace: "testns"},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload:  v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
				Mappings: []v1alpha2.Mapping{{Method: "POST", Body: ".payload.body", URL: ".payload.baseUrl"}},
			},
		},
	}

	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	err := DeployAction(svcCtx, crCtx, "CREATE")
	if err != nil {
		t.Fatalf("backward-compat: unexpected error: %v", err)
	}
	if cr.Status.Response.StatusCode != 201 {
		t.Errorf("expected status 201, got %d", cr.Status.Response.StatusCode)
	}
}

func TestDeployAction_OIDCHeaderInjected(t *testing.T) {
	// Simulate an HTTP client that has already been wrapped with the OIDC decorator
	// (NewOIDCClient) — here we verify that the injected Authorization header reaches
	// the wire by inspecting what SendRequest received.
	var capturedAuthHeader string
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			if hdrs, ok := headers.Decrypted.(map[string][]string); ok {
				if vals := hdrs["Authorization"]; len(vals) > 0 {
					capturedAuthHeader = vals[0]
				}
			}
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 200, Body: `{"id": "99"}`,
			}}, nil
		},
	}

	// Pre-inject the Authorization header as the OIDC decorator would
	preSetHeaders := map[string][]string{"Authorization": {"Bearer oidc-token-xyz"}}
	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "test-oidc", Namespace: "testns"},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload:  v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
				Headers:  preSetHeaders,
				Mappings: []v1alpha2.Mapping{{Method: "POST", Body: ".payload.body", URL: ".payload.baseUrl"}},
			},
		},
	}

	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	err := DeployAction(svcCtx, crCtx, "CREATE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedAuthHeader != "Bearer oidc-token-xyz" {
		t.Errorf("expected Authorization: Bearer oidc-token-xyz, got %q", capturedAuthHeader)
	}
}

// TestDeployAction_ResumeAfterTerminalClear_PersistsStartedAt covers Gap 6: after a
// terminal-clear reset StartedAt to nil, resuming the in-flight operation must persist a
// fresh StartedAt BEFORE polling. Otherwise the poller's local now-fallback re-anchors
// the deadline on every reconcile and polling.timeout never elapses (a never-completing
// operation loops forever instead of hitting PollTimeout). The guarantee is "StartedAt is
// non-nil when Poll runs" — the foreground poller reads it from status to compute the
// deadline, so a nil there would trigger the per-reconcile fallback. (On successful
// completion the completion path clears StartedAt again, so we assert at poll time via an
// injected poller, not after DeployAction returns.)
func TestDeployAction_ResumeAfterTerminalClear_PersistsStartedAt(t *testing.T) {
	// In-flight anchor from a prior CREATE whose poll hit a terminal failure; the
	// terminal-clear then reset StartedAt to nil to give the resume a fresh budget.
	cr := crWithPolling(`"http://api/" + .response.body.name`, map[string]interface{}{
		"body": map[string]interface{}{"name": "operations/123"},
	})
	cr.Spec.ForProvider.ExternalRef = ".poll.response.body.id"
	cr.Status.Polling.StartedAt = nil // terminal-clear reset it

	// Inject a poller that asserts StartedAt is non-nil when Poll runs, then completes.
	var observedStartedAt *v1.Time
	mockP := &captureStartedAtPoller{onPoll: func(s *v1.Time) { observedStartedAt = s }}

	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{}, nil // not called: anchor in flight skips the mutate
		},
	}, nil)
	crCtx := service.NewRequestCRContext(cr)

	if err := DeployAction(svcCtx, crCtx, "CREATE", mockP); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if observedStartedAt == nil {
		t.Fatal("expected StartedAt to be non-nil when Poll runs on resume after terminal-clear; " +
			"nil means the poller uses a per-reconcile now-fallback, re-anchoring the deadline so " +
			"polling.timeout never elapses")
	}
}

// TestDeployAction_NormalResume_PreservesStartedAt is the Gap 6 invariant: a normal
// resume (StartedAt already set from the original entry) must NOT re-anchor the deadline.
// The poll timeout must be measured from the original start, not from the resume instant,
// so a slow operation that exhausts the budget across requeues still hits PollTimeout.
func TestDeployAction_NormalResume_PreservesStartedAt(t *testing.T) {
	originalStart := v1.Now()
	cr := crWithPolling(`"http://api/" + .response.body.name`, map[string]interface{}{
		"body": map[string]interface{}{"name": "operations/123"},
	})
	cr.Spec.ForProvider.ExternalRef = ".poll.response.body.id"
	cr.Status.Polling.StartedAt = &originalStart // normal resume: deadline already anchored

	var observed *v1.Time
	mockP := &captureStartedAtPoller{onPoll: func(s *v1.Time) { observed = s }}

	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{}, nil
		},
	}, nil)

	if err := DeployAction(svcCtx, service.NewRequestCRContext(cr), "CREATE", mockP); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if observed == nil {
		t.Fatal("expected to observe StartedAt at poll time")
	}
	if !observed.Equal(&originalStart) {
		t.Errorf("normal resume must preserve the original StartedAt (deadline not re-anchored); got %v, want %v", observed.Time, originalStart.Time)
	}
}

// captureStartedAtPoller records the StartedAt the foreground path hands to Poll and
// completes immediately. It reads it via the crCtx the real flow passes.
type captureStartedAtPoller struct {
	onPoll func(startedAt *v1.Time)
}

func (m *captureStartedAtPoller) Poll(
	_ *service.ServiceContext,
	crCtx *service.RequestCRContext,
	_ interfaces.HTTPMapping,
	_ map[string]interface{},
) (polling.Result, error) {
	m.onPoll(crCtx.Status().GetOperationStartedAt())
	return polling.Result{Done: true, PollResponse: map[string]interface{}{"body": map[string]interface{}{"id": "model-789"}}}, nil
}
