package request

import (
	"context"
	"net/http"
	"strconv"
	"strings"
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

// TestDeployAction_AsyncCreate_OperationFailure_RetryByDefault verifies the design doc's
// default: an operation failure (polling.done + polling.error non-null) with isTerminalError
// UNSET is retry, not terminal. The poll positively confirmed the operation finished without
// creating the resource (edge case #1), so a retry re-fires a fresh, duplicate-safe mutate:
// the anchor is CLEARED, no terminal is set, the failing poll response is surfaced to
// status.response (so an operator can inspect the failure), and DeployAction returns nil so the
// controller requeues (the next OBSERVE overwrites status.response before the mutate re-fires).
func TestDeployAction_AsyncCreate_OperationFailure_RetryByDefault(t *testing.T) {
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
	// Retry-by-default: DeployAction returns nil so the controller requeues and re-fires the
	// mutate next reconcile.
	if err != nil {
		t.Fatalf("expected nil error (operation-failure retry-by-default), got: %v", err)
	}
	// The anchor is CLEARED: the operation is dead, so a retry re-fires a fresh mutate rather
	// than re-polling the dead operation.
	if cr.Status.Polling.Response != nil {
		t.Error("expected polling.response anchor CLEARED after an operation failure (retry-by-default)")
	}
	if cr.Status.TerminalError != "" {
		t.Errorf("expected no terminalError on retry-by-default, got %q", cr.Status.TerminalError)
	}
	// The failing poll response is surfaced to status.response so an operator can inspect the
	// full failure until the next reconcile's OBSERVE overwrites it. The real error body is kept.
	if cr.Status.Response.Body == "" {
		t.Error("expected failing poll response surfaced to status.response on poll-failure retry")
	}
	// The REAL status code is persisted (a wire-200 for an LRO that completed with an error body)
	// — no faked 5xx. Routing on the externalRef path consults .status.externalRef, never
	// .status.response, so the write is inert to routing by construction.
	if cr.Status.Response.StatusCode != 200 {
		t.Errorf("expected status.response.statusCode to be the real poll code 200, got %d", cr.Status.Response.StatusCode)
	}
	// method is stamped POST for inspection (this records the CREATE's failure outcome), but
	// routing does not read it on the externalRef path.
	if cr.Status.RequestDetails.Method != http.MethodPost {
		t.Errorf("expected status.requestDetails.method stamped POST (inspection), got %q", cr.Status.RequestDetails.Method)
	}
	// The failure counter is NOT incremented: a poll GET is a read, so even though we persist a
	// 500 it must bypass statushandler's incrementFailures and leave .status.failed untouched.
	if cr.Status.Failed != 0 {
		t.Errorf("expected status.failed unchanged (0) on poll-failure retry, got %d", cr.Status.Failed)
	}
}

// TestDeployAction_AsyncCreate_PollFailure_RealStatusCode_NotPoisoningIsTerminalError is the
// PRD's defect-fix regression: isTerminalError reading .status.response.statusCode must see the
// REAL wire code, not a provider-faked 5xx. A completed-with-error LRO returns HTTP 200 with an
// error block in the body; persistPollFailureResponse now stores that real 200. With the previous
// 500 override, `.status.response.statusCode >= 500` would classify EVERY wire-200 poll failure as
// terminal (the provider's own lie, not the API's response). Now 200 >= 500 is false → retry.
func TestDeployAction_AsyncCreate_PollFailure_RealStatusCode_NotPoisoningIsTerminalError(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			if method == "POST" {
				return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
					StatusCode: 202, Body: `{"name": "operations/456"}`,
				}}, nil
			}
			// Wire-200 with an error body: the LRO finished in failure.
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 200,
				Body:       `{"done": true, "error": {"message": "quota exceeded"}}`,
			}}, nil
		},
	}

	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "test-not-poisoned", Namespace: "testns"},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload: v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
				// Keys off .status.response.statusCode. With the 500 lie this would fire on every
				// wire-200 poll failure; with the real 200 it must NOT (200 >= 500 is false).
				IsTerminalError: `.status.response.statusCode >= 500`,
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

	if err := DeployAction(svcCtx, crCtx, "CREATE"); err != nil {
		t.Fatalf("expected nil error (wire-200 poll failure must retry, not stall), got: %v", err)
	}
	// The real 200 is persisted — not a faked 5xx that would have made isTerminalError fire.
	if cr.Status.Response.StatusCode != 200 {
		t.Errorf("expected status.response.statusCode to be the real poll code 200, got %d", cr.Status.Response.StatusCode)
	}
	if cr.Status.TerminalError != "" {
		t.Errorf("isTerminalError .status.response.statusCode >= 500 must NOT fire on a wire-200 poll failure (real code 200); got terminalError %q", cr.Status.TerminalError)
	}
	// Retry-by-default side effects: anchor cleared so a retry re-fires a fresh mutate.
	if cr.Status.Polling.Response != nil {
		t.Error("expected polling.response anchor CLEARED on retry-by-default")
	}
}

// TestDeployAction_AsyncCreate_OperationFailure_Terminal verifies the opt-in: an operation
// failure with spec.isTerminalError resolving to a non-empty string stalls the resource. The
// message is persisted to status.terminalError, the anchor is CLEARED (the operation is dead),
// and DeployAction returns nil (a terminal is written to status, not returned as a Go error).
func TestDeployAction_AsyncCreate_OperationFailure_Terminal(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			if method == "POST" {
				return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
					StatusCode: 202, Body: `{"name": "operations/456"}`,
				}}, nil
			}
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 200,
				Body:       `{"done": true, "error": {"message": "quota exceeded"}}`,
			}}, nil
		},
	}

	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "test-request-term", Namespace: "testns"},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload: v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
				// isTerminalError keys off the failing poll response body to surface a message.
				IsTerminalError: `.poll.response.body.error.message`,
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
	if err != nil {
		t.Fatalf("expected nil error (terminal written to status), got: %v", err)
	}
	// The anchor is CLEARED on an operation-failure terminal (the operation is dead).
	if cr.Status.Polling.Response != nil {
		t.Error("expected polling.response anchor CLEARED on operation-failure terminal")
	}
	if cr.Status.TerminalError != "quota exceeded" {
		t.Errorf("expected terminalError from isTerminalError expression, got %q", cr.Status.TerminalError)
	}
	// The failing poll response is surfaced to status.response so an operator can inspect the
	// full failure; it is retained (alongside the terminal) until a spec change clears the terminal.
	if cr.Status.Response.Body == "" {
		t.Error("expected failing poll response surfaced to status.response on operation-failure terminal")
	}
	// The REAL status code (a wire-200 for an LRO that completed with an error body) is persisted —
	// no faked 5xx. Routing on the externalRef path consults .status.externalRef, never
	// .status.response, so the write is inert to routing by construction.
	if cr.Status.Response.StatusCode != 200 {
		t.Errorf("expected status.response.statusCode to be the real poll code 200, got %d", cr.Status.Response.StatusCode)
	}
	if cr.Status.RequestDetails.Method != http.MethodPost {
		t.Errorf("expected status.requestDetails.method stamped POST (inspection), got %q", cr.Status.RequestDetails.Method)
	}
}

// TestDeployAction_NonPolling_IsTerminalError_Stalls verifies the opt-in on the non-polling
// path: a non-2xx mutate with spec.isTerminalError resolving to a non-empty string stalls the
// resource instead of returning a retryable Go error. The message is persisted to
// status.terminalError, DeployAction returns nil (terminal written to status, not surfaced as an
// error), and the failing response stays in status.response.
func TestDeployAction_NonPolling_IsTerminalError_Stalls(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 422, Body: `{"error": {"message": "invalid config"}}`,
			}, HttpRequest: httpClient.HttpRequest{Method: "POST", URL: testURL}}, nil
		},
	}
	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "test-nonpoll-term", Namespace: "testns"},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload: v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
				// Key off the failing mutate response body (.response, not .poll — no poll ran).
				IsTerminalError: `.response.body.error.message`,
				Mappings:        []v1alpha2.Mapping{{Method: "POST", Body: ".payload.body", URL: ".payload.baseUrl"}},
			},
		},
	}
	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	err := DeployAction(svcCtx, crCtx, "CREATE")
	if err != nil {
		t.Fatalf("expected nil error (terminal written to status), got: %v", err)
	}
	if cr.Status.TerminalError != "invalid config" {
		t.Errorf("expected terminalError from isTerminalError expression, got %q", cr.Status.TerminalError)
	}
	// The failing mutate response is surfaced via the status handler before classification.
	if cr.Status.Response.StatusCode != 422 {
		t.Errorf("expected failing mutate response (422) surfaced to status.response, got %d", cr.Status.Response.StatusCode)
	}
}

// TestDeployAction_NonPolling_IsTerminalError_BoundedRetry exercises the design's marquee
// bounded-retry use case: keying isTerminalError off .status.failed. The full status is exposed
// in the jq context (GetStatusMap), and the failure counter is incremented by the status handler
// BEFORE classification runs, so with Failed preset to 5 the incremented value (6) trips
// `.status.failed > 5` and the resource stalls. Below the threshold it stays a retryable error.
func TestDeployAction_NonPolling_IsTerminalError_BoundedRetry(t *testing.T) {
	newCR := func(failed int32) *v1alpha2.AsyncRequest {
		cr := &v1alpha2.AsyncRequest{
			ObjectMeta: v1.ObjectMeta{Name: "test-bounded", Namespace: "testns"},
			Spec: v1alpha2.AsyncRequestSpec{
				ForProvider: v1alpha2.AsyncRequestParameters{
					Payload:         v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
					IsTerminalError: `.status.failed > 5`,
					Mappings:        []v1alpha2.Mapping{{Method: "POST", Body: ".payload.body", URL: ".payload.baseUrl"}},
				},
			},
		}
		cr.Status.Failed = failed
		return cr
	}
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 500, Body: `{}`,
			}, HttpRequest: httpClient.HttpRequest{Method: "POST", URL: testURL}}, nil
		},
	}

	// Below threshold: Failed=4 → incremented to 5 → `5 > 5` is false → retryable error.
	crBelow := newCR(4)
	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	if err := DeployAction(svcCtx, service.NewRequestCRContext(crBelow), "CREATE"); err == nil {
		t.Error("below threshold: expected a retryable Go error, got nil (should not have stalled)")
	}
	if crBelow.Status.TerminalError != "" {
		t.Errorf("below threshold: expected no terminalError, got %q", crBelow.Status.TerminalError)
	}

	// At/over threshold: Failed=5 → incremented to 6 → `6 > 5` is true → terminal stall.
	crOver := newCR(5)
	if err := DeployAction(svcCtx, service.NewRequestCRContext(crOver), "CREATE"); err != nil {
		t.Fatalf("over threshold: expected nil error (terminal stall), got %v", err)
	}
	if crOver.Status.TerminalError == "" {
		t.Error("over threshold: expected terminalError set (bounded retry exhausted)")
	}
}

// TestDeployAction_IsTerminalError_BrokenExpression_Stalls verifies that a broken isTerminalError
// jq expression is itself treated as terminal (rather than surfacing as a retry that would hot-loop
// the mutate on a config error the requeue cannot fix). The surfaced message names the jq failure.
func TestDeployAction_IsTerminalError_BrokenExpression_Stalls(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 500, Body: `{}`,
			}, HttpRequest: httpClient.HttpRequest{Method: "POST", URL: testURL}}, nil
		},
	}
	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "test-broken-expr", Namespace: "testns"},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload: v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
				// tonumber on a non-numeric string is a jq runtime error.
				IsTerminalError: `"not-a-number" | tonumber`,
				Mappings:        []v1alpha2.Mapping{{Method: "POST", Body: ".payload.body", URL: ".payload.baseUrl"}},
			},
		},
	}
	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	if err := DeployAction(svcCtx, crCtx, "CREATE"); err != nil {
		t.Fatalf("expected nil error (broken expression stalls, terminal written to status), got %v", err)
	}
	if !strings.Contains(cr.Status.TerminalError, "isTerminalError jq expression failed") {
		t.Errorf("expected terminalError naming the jq failure, got %q", cr.Status.TerminalError)
	}
}

// TestDeployAction_PollingMutate_NonSuccess_RetryByDefault covers the polling-mutate non-2xx block:
// a polling mapping whose mutate call itself returns a non-2xx (e.g. 400) — before any poll — is
// retryable by default. DeployAction surfaces the response to status.response, returns a Go error so
// the controller requeues, and persists NO anchor (nothing was side-effected, so re-firing is safe).
func TestDeployAction_PollingMutate_NonSuccess_RetryByDefault(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 400, Body: `{"error": "bad request"}`,
			}, HttpRequest: httpClient.HttpRequest{Method: "POST", URL: testURL}}, nil
		},
	}
	cr := crWithPolling(`"http://api/operations/1"`, nil)
	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	err := DeployAction(svcCtx, crCtx, "CREATE")
	if err == nil {
		t.Fatal("expected a retryable Go error for a polling-mutate 400, got nil")
	}
	wantMsg := "HTTP POST request failed with status code: 400"
	if err.Error() != wantMsg {
		t.Errorf("error message: want %q, got %q", wantMsg, err.Error())
	}
	// No anchor: the mutate never succeeded, so there is no in-flight operation to resume.
	if cr.Status.Polling.Response != nil {
		t.Error("expected no polling.response anchor after a failed polling-mutate")
	}
	// The failing response is still surfaced for operator inspection.
	if cr.Status.Response.StatusCode != 400 {
		t.Errorf("expected failing mutate response (400) surfaced to status.response, got %d", cr.Status.Response.StatusCode)
	}
	if cr.Status.TerminalError != "" {
		t.Errorf("expected no terminalError on retry-by-default, got %q", cr.Status.TerminalError)
	}
}

// TestDeployAction_PollingMutate_NonSuccess_Terminal verifies the opt-out on the polling-mutate
// path: a polling mapping whose mutate returns non-2xx with spec.isTerminalError matching stalls
// the resource. DeployAction returns nil, terminalError carries the message, and no anchor is set.
func TestDeployAction_PollingMutate_NonSuccess_Terminal(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
				StatusCode: 403, Body: `{"error": {"message": "forbidden"}}`,
			}, HttpRequest: httpClient.HttpRequest{Method: "POST", URL: testURL}}, nil
		},
	}
	cr := crWithPolling(`"http://api/operations/1"`, nil)
	cr.Spec.ForProvider.IsTerminalError = `.response.body.error.message`
	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	err := DeployAction(svcCtx, crCtx, "CREATE")
	if err != nil {
		t.Fatalf("expected nil error (terminal written to status), got: %v", err)
	}
	if cr.Status.TerminalError != "forbidden" {
		t.Errorf("expected terminalError from isTerminalError expression, got %q", cr.Status.TerminalError)
	}
	if cr.Status.Polling.Response != nil {
		t.Error("expected no polling.response anchor on a failed polling-mutate terminal")
	}
	if cr.Status.Response.StatusCode != 403 {
		t.Errorf("expected failing mutate response (403) surfaced to status.response, got %d", cr.Status.Response.StatusCode)
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
	if cr.Status.TerminalError == "" {
		t.Error("step 1: expected terminalError set for bad polling.url")
	}

	// Step 2: operator fixes polling.url (bump generation, clear terminal state as the
	// reconciler would on generation drift) and resumes — the retained anchor is reused,
	// so POST must NOT fire again.
	cr.Generation = 2
	cr.Status.TerminalError = ""
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

// TestDeployAction_NonPolling_NonSuccess_SurfacesError covers the secondary bug in
// bug-empty-externalref-observe-hits-list-endpoint-skips-create.md: a non-polling mutate
// (PATCH/PUT/DELETE) that returns a non-2xx status not in allowedStatusCodes must surface as
// a real error so the controller reports Synced=False, instead of being swallowed by
// SetRequestStatus (which only increments the failures counter and returns nil) and looping
// forever as ReconcileSuccess. Status is still persisted first, so the response stays visible.
func TestDeployAction_NonPolling_NonSuccess_SurfacesError(t *testing.T) {
	cases := []struct {
		name       string
		action     string
		method     string
		statusCode int
	}{
		{"UpdatePatch404", "UPDATE", "PATCH", 404},
		{"UpdatePut500", "UPDATE", "PUT", 500},
		{"RemoveDelete404", "REMOVE", "DELETE", 404},
		{"CreatePost400", "CREATE", "POST", 400},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			httpMock := &MockHttpClient{
				MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
					return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
						StatusCode: tc.statusCode, Body: `{"error":"not found"}`,
					}}, nil
				},
			}
			cr := &v1alpha2.AsyncRequest{
				ObjectMeta: v1.ObjectMeta{Name: "test-broken-url", Namespace: "testns"},
				Spec: v1alpha2.AsyncRequestSpec{
					ForProvider: v1alpha2.AsyncRequestParameters{
						Payload:  v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
						Mappings: []v1alpha2.Mapping{{Method: tc.method, Action: tc.action, Body: ".payload.body", URL: ".payload.baseUrl"}},
					},
				},
			}
			svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
			crCtx := service.NewRequestCRContext(cr)

			err := DeployAction(svcCtx, crCtx, tc.action)
			if err == nil {
				t.Fatalf("expected DeployAction to surface HTTP %d as an error, got nil", tc.statusCode)
			}
			wantMsg := "HTTP " + tc.method + " request failed with status code: " + strconv.Itoa(tc.statusCode)
			if err.Error() != wantMsg {
				t.Errorf("error message: want %q, got %q", wantMsg, err.Error())
			}
			// Status is persisted before the error is returned, so the response stays visible.
			if cr.Status.Response.StatusCode != tc.statusCode {
				t.Errorf("expected persisted status code %d, got %d", tc.statusCode, cr.Status.Response.StatusCode)
			}
		})
	}
}

// TestDeployAction_NonPolling_AllowedStatusCode_NotSurfaced: a non-2xx status that the user
// explicitly allow-listed is not an error and must continue to reconcile successfully.
func TestDeployAction_NonPolling_AllowedStatusCode_NotSurfaced(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{StatusCode: 409, Body: `{}`}}, nil
		},
	}
	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "test-allowed", Namespace: "testns"},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload:            v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
				AllowedStatusCodes: []int{409},
				Mappings:           []v1alpha2.Mapping{{Method: "PATCH", Action: "UPDATE", Body: ".payload.body", URL: ".payload.baseUrl"}},
			},
		},
	}
	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	if err := DeployAction(svcCtx, crCtx, "UPDATE"); err != nil {
		t.Fatalf("expected no error for allow-listed 409, got %v", err)
	}
}

// TestDeployAction_NonPolling_Success_NotSurfaced: a 2xx non-polling mutate stays nil — the
// fix only surfaces errors, it does not change the success path.
func TestDeployAction_NonPolling_Success_NotSurfaced(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{StatusCode: 200, Body: `{"id": "1"}`}}, nil
		},
	}
	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "test-ok", Namespace: "testns"},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload:  v1alpha2.Payload{Body: testBody, BaseUrl: testURL},
				Mappings: []v1alpha2.Mapping{{Method: "PATCH", Action: "UPDATE", Body: ".payload.body", URL: ".payload.baseUrl"}},
			},
		},
	}
	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), httpMock, nil)
	crCtx := service.NewRequestCRContext(cr)

	if err := DeployAction(svcCtx, crCtx, "UPDATE"); err != nil {
		t.Fatalf("expected no error for 2xx, got %v", err)
	}
}
