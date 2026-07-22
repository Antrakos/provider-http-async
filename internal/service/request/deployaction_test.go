package request

import (
	"context"
	"testing"

	"github.com/Antrakos/provider-http-async/apis/cluster/request/v1alpha2"
	"github.com/Antrakos/provider-http-async/apis/common"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/test"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

func crWithPolling(pollURL, operationRef string) *v1alpha2.AsyncRequest {
	return &v1alpha2.AsyncRequest{
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
		Status: v1alpha2.AsyncRequestStatus{Polling: v1alpha2.PollingStatus{OperationRef: operationRef}},
	}
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

	cr := crWithPolling(`"operations/123"`, "")
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
	if cr.Status.Polling.OperationRef != "" {
		t.Errorf("expected operationRef cleared, got %q", cr.Status.Polling.OperationRef)
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
				Payload:  v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
				Mappings: []v1alpha2.Mapping{{
					Method: "POST", Body: ".payload.body", URL: ".payload.baseUrl",
					Polling: &common.Polling{
						URL:   `"operations/456"`,
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

	// operationRef already set (simulates crash after first POST)
	cr := crWithPolling(`"operations/123"`, "http://api/operations/123")
	cr.Spec.ForProvider.ExternalRef = ".poll.response.body.id"

	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	err := DeployAction(svcCtx, crCtx, "CREATE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if postCalled {
		t.Error("POST (mutate) must NOT be called when operationRef is already set (crash recovery)")
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
