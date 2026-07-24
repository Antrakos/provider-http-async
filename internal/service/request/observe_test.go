package request

import (
	"context"
	"net/http"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/test"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Antrakos/provider-http-async/apis/cluster/request/v1alpha2"
	"github.com/Antrakos/provider-http-async/apis/common"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/Antrakos/provider-http-async/internal/service/request/observe"
	"github.com/Antrakos/provider-http-async/internal/service/request/requestgen"
	"github.com/Antrakos/provider-http-async/internal/service/request/requestmapping"
)

const (
	providerName    = "http-test"
	testRequestName = "test-request"
	testNamespace   = "testns"
)

var (
	errNotFound = errors.New(observe.ErrObjectNotFound)
)

var (
	testForProvider = v1alpha2.AsyncRequestParameters{
		Payload: v1alpha2.Payload{
			Body:    "{\"username\": \"john_doe\", \"email\": \"john.doe@example.com\"}",
			BaseUrl: "https://api.example.com/users",
		},
		Mappings: []v1alpha2.Mapping{
			testPostMapping,
			testGetMapping,
			testPutMapping,
			testDeleteMapping,
		},
	}
)

var (
	testPostMapping = v1alpha2.Mapping{
		Method: "POST",
		Body:   "{ username: .payload.body.username, email: .payload.body.email }",
		URL:    ".payload.baseUrl",
	}

	testPutMapping = v1alpha2.Mapping{
		Method: "PUT",
		Body:   "{ username: \"john_doe_new_username\" }",
		URL:    "(.payload.baseUrl + \"/\" + .response.body.id)",
	}

	testGetMapping = v1alpha2.Mapping{
		Method: "GET",
		URL:    "(.payload.baseUrl + \"/\" + .response.body.id)",
	}

	testDeleteMapping = v1alpha2.Mapping{
		Method: "DELETE",
		URL:    "(.payload.baseUrl + \"/\" + .response.body.id)",
	}
)

type httpRequestModifier func(request *v1alpha2.AsyncRequest)

func httpRequest(rm ...httpRequestModifier) *v1alpha2.AsyncRequest {
	r := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{
			Name:      testRequestName,
			Namespace: testNamespace,
		},
		Spec: v1alpha2.AsyncRequestSpec{
			ClusterManagedResourceSpec: xpv2.ClusterManagedResourceSpec{
				ProviderConfigReference: &xpv2.Reference{
					Name: providerName,
				},
			},
			ForProvider: testForProvider,
		},
		Status: v1alpha2.AsyncRequestStatus{},
	}

	for _, m := range rm {
		m(r)
	}

	return r
}

type MockSendRequestFn func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error)

type MockHttpClient struct {
	MockSendRequest MockSendRequestFn
}

func (c *MockHttpClient) SendRequest(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
	return c.MockSendRequest(ctx, method, url, body, headers, tlsConfigData)
}

func Test_isUpToDate(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        *v1alpha2.AsyncRequest
	}
	type want struct {
		result ObserveRequestDetails
		err    error
	}

	cases := map[string]struct {
		args args
		want want
	}{
		"ObjectIdKnownBeforeCreate": {
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								Body:       `{"username":"john_doe_new_username"}`,
								StatusCode: 200,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequest(func(r *v1alpha2.AsyncRequest) {
					r.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
						{
							Method: "GET",
							URL:    "(\"http://some.org/\" + \"1423\")",
						},
					}
				}),
			},
			want: want{
				err: nil,
				result: ObserveRequestDetails{
					Details: httpClient.HttpDetails{
						HttpResponse: httpClient.HttpResponse{
							Body:       `{"username":"john_doe_new_username"}`,
							StatusCode: 200,
						},
					},
					Synced: true,
				},
			},
		},
		"ObjectNotFoundEmptyStatus": {
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequest(func(r *v1alpha2.AsyncRequest) {
					r.Status.Response.Body = ""
					r.Status.Response.StatusCode = 0
				}),
			},
			want: want{
				err: errNotFound,
			},
		},
		"ObjectNotFoundPostFailed": {
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequest(func(r *v1alpha2.AsyncRequest) {
					r.Status.RequestDetails.Method = http.MethodPost
					r.Status.Response.StatusCode = 400
				}),
			},
			want: want{
				err: errNotFound,
			},
		},
		"ObjectNotFound404StatusCode": {
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								Body:       "",
								StatusCode: http.StatusNotFound,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequest(func(r *v1alpha2.AsyncRequest) {
					r.Status.Response.StatusCode = http.StatusNotFound
				}),
			},
			want: want{
				err: errNotFound,
			},
		},
		"FailBodyNotJSON": {
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								Body: "not a JSON",
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequest(func(r *v1alpha2.AsyncRequest) {
					r.Status.Response.Body = `{"username":"john_doe_new_username"}`
					r.Status.Response.StatusCode = http.StatusOK
				}),
			},
			want: want{
				err: errors.Errorf(errNotValidJSON, "response body", "not a JSON"),
			},
		},
		"SuccessNotSynced": {
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								Body:       `{"username":"old_name"}`,
								StatusCode: 200,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequest(func(r *v1alpha2.AsyncRequest) {
					r.Status.Response.Body = `{"username":"john_doe_new_username"}`
					r.Status.Response.StatusCode = http.StatusOK
				}),
			},
			want: want{
				err: nil,
				result: ObserveRequestDetails{
					Details: httpClient.HttpDetails{
						HttpResponse: httpClient.HttpResponse{
							Body:       `{"username":"old_name"}`,
							Headers:    nil,
							StatusCode: 200,
						},
					},
					ResponseError: nil,
					Synced:        false,
				},
			},
		},
		"SuccessNoPUTMapping": {
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								Body:       `{"username":"old_name"}`,
								StatusCode: 200,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequest(func(r *v1alpha2.AsyncRequest) {
					r.Status.Response.Body = `{"username":"john_doe_new_username"}`
					r.Status.Response.StatusCode = 200
					r.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
						testPostMapping,
						testGetMapping,
						testDeleteMapping,
					}
				}),
			},
			want: want{
				err: nil,
				result: ObserveRequestDetails{
					Details: httpClient.HttpDetails{
						HttpResponse: httpClient.HttpResponse{
							Body:       `{"username":"old_name"}`,
							Headers:    nil,
							StatusCode: 200,
						},
					},
					ResponseError: nil,
					Synced:        true,
				},
			},
		},
		"SuccessJSONBody": {
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								Body:       `{"username":"john_doe_new_username"}`,
								StatusCode: 200,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequest(func(r *v1alpha2.AsyncRequest) {
					r.Status.Response.Body = `{"username":"john_doe_new_username"}`
					r.Status.Response.StatusCode = 200
				}),
			},
			want: want{
				err: nil,
				result: ObserveRequestDetails{
					Details: httpClient.HttpDetails{
						HttpResponse: httpClient.HttpResponse{
							Body:       `{"username":"john_doe_new_username"}`,
							Headers:    nil,
							StatusCode: 200,
						},
					},
					ResponseError: nil,
					Synced:        true,
				},
			},
		},
		"MissingMappingObjectNotCreated": {
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequest(func(r *v1alpha2.AsyncRequest) {
					r.Status.Response.Body = ""
					r.Status.Response.StatusCode = 0
					r.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
						testPostMapping,
						testPutMapping,
						testDeleteMapping,
						// No GET or OBSERVE mapping
					}
				}),
			},
			want: want{
				err: errors.New("OBSERVE or GET mapping doesn't exist in request, skipping operation"),
			},
		},
		"MissingMappingObjectCreated": {
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequest(func(r *v1alpha2.AsyncRequest) {
					r.Status.Response.Body = `{"id": "123"}`
					r.Status.Response.StatusCode = 201
					r.Status.RequestDetails.Method = http.MethodPost
					r.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
						testPostMapping,
						testPutMapping,
						testDeleteMapping,
						// No GET or OBSERVE mapping
					}
				}),
			},
			want: want{
				err: errors.New("OBSERVE or GET mapping doesn't exist in request, skipping operation"),
			},
		},
		// resourceExistsCheck=false on a parent that always returns 200: the owned sub-resource is
		// absent, so IsUpToDate reports ErrObjectNotFound -> the controller calls Create(). This is
		// the Vertex deployedModel fix (bug-observe-always-fires-blocks-create-for-subresources.md).
		"ResourceExistsCheckFalse_RoutesToCreate": {
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
						// Parent endpoint always returns 200; no deployedModel with our externalRef.
						return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
							StatusCode: 200,
							Body:       `{"deployedModels": []}`,
						}}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequest(func(r *v1alpha2.AsyncRequest) {
					r.Status.ExternalRef = "model-789"
					r.Status.Response.StatusCode = 200 // object already observed before
					r.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
						{Method: "POST", Action: "CREATE", URL: ".payload.baseUrl", Body: ".payload.body"},
						{Method: "GET", Action: "OBSERVE", URL: ".payload.baseUrl"},
					}
					r.Spec.ForProvider.ResourceExistsCheck = v1alpha2.ExpectedResponseCheck{
						Type:  v1alpha2.ExpectedResponseCheckTypeCustom,
						Logic: `(.response.body.deployedModels // []) as $m | .status.externalRef as $ref | ($m | map(select(.id == $ref)) | length > 0)`,
					}
					r.Spec.ForProvider.ExpectedResponseCheck = v1alpha2.ExpectedResponseCheck{
						Type:  v1alpha2.ExpectedResponseCheckTypeCustom,
						Logic: `.response.body.deployedModels != null`,
					}
				}),
			},
			want: want{
				err: errNotFound,
			},
		},
		// resourceExistsCheck=true (sub-resource present) then expectedResponseCheck=false (drift),
		// with an UPDATE mapping present: normal drift -> Synced=false (NOT a terminal error).
		"ResourceExistsCheckTrue_ThenDrift_WithUpdateMapping": {
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
						return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
							StatusCode: 200,
							Body:       `{"deployedModels": [{"id": "model-789"}]}`,
						}}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequest(func(r *v1alpha2.AsyncRequest) {
					r.Status.ExternalRef = "model-789"
					r.Status.Response.StatusCode = 200
					r.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
						{Method: "POST", Action: "CREATE", URL: ".payload.baseUrl", Body: ".payload.body"},
						{Method: "GET", Action: "OBSERVE", URL: ".payload.baseUrl"},
						{Method: "PUT", Action: "UPDATE", URL: ".payload.baseUrl", Body: ".payload.body"},
					}
					r.Spec.ForProvider.ResourceExistsCheck = v1alpha2.ExpectedResponseCheck{
						Type:  v1alpha2.ExpectedResponseCheckTypeCustom,
						Logic: `(.response.body.deployedModels // []) as $m | .status.externalRef as $ref | ($m | map(select(.id == $ref)) | length > 0)`,
					}
					r.Spec.ForProvider.ExpectedResponseCheck = v1alpha2.ExpectedResponseCheck{
						Type: v1alpha2.ExpectedResponseCheckTypeCustom,
						// Drift: claim the resource is NOT up to date.
						Logic: `false`,
					}
				}),
			},
			want: want{
				result: ObserveRequestDetails{
					Synced: false,
					Details: httpClient.HttpDetails{
						HttpResponse: httpClient.HttpResponse{
							StatusCode: 200,
							Body:       `{"deployedModels": [{"id": "model-789"}]}`,
						},
					},
				},
				err: nil,
			},
		},
	}
	for name, tc := range cases {
		tc := tc // Create local copies of loop variables

		t.Run(name, func(t *testing.T) {
			svcCtx := service.NewServiceContext(context.Background(), tc.args.localKube, logging.NewNopLogger(), tc.args.http, nil)
			crCtx := service.NewRequestCRContext(tc.args.mg)
			got, gotErr := IsUpToDate(svcCtx, crCtx)
			if diff := cmp.Diff(tc.want.err, gotErr, test.EquateErrors()); diff != "" {
				t.Fatalf("isUpToDate(...): -want error, +got error: %s", diff)
			}
			if diff := cmp.Diff(tc.want.result, got); diff != "" {
				t.Errorf("isUpToDate(...): -want result, +got result: %s", diff)
			}
		})
	}
}

func Test_determineResponseCheck(t *testing.T) {
	type args struct {
		ctx         context.Context
		cr          *v1alpha2.AsyncRequest
		details     httpClient.HttpDetails
		responseErr error
		localKube   client.Client
	}

	type want struct {
		result ObserveRequestDetails
		err    error
	}

	cases := map[string]struct {
		args args
		want want
	}{
		"DefaultResponseCheckSynced": {
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{
								Body:    "{\"username\": \"john_doe\", \"email\": \"john.doe@example.com\"}",
								BaseUrl: "https://api.example.com/users",
							},
							Mappings: []v1alpha2.Mapping{
								testPostMapping,
								testGetMapping,
								testDeleteMapping,
								testPutMapping,
							},
							ExpectedResponseCheck: v1alpha2.ExpectedResponseCheck{
								Type: v1alpha2.ExpectedResponseCheckTypeDefault,
							},
						},
					},
				},
				details: httpClient.HttpDetails{
					HttpResponse: httpClient.HttpResponse{
						Body:       `{"username": "john_doe_new_username"}`,
						Headers:    nil,
						StatusCode: 200,
					},
				},
				responseErr: nil,
			},
			want: want{
				result: ObserveRequestDetails{
					Details: httpClient.HttpDetails{
						HttpResponse: httpClient.HttpResponse{
							Body:       `{"username": "john_doe_new_username"}`,
							StatusCode: 200,
						},
					},
					Synced: true,
				},
				err: nil,
			},
		},
		"DefaultResponseCheckUnsynced": {
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{
								Body:    "{\"username\": \"john_doe\", \"email\": \"john.doe@example.com\"}",
								BaseUrl: "https://api.example.com/users",
							},
							Mappings: []v1alpha2.Mapping{
								testPostMapping,
								testGetMapping,
								testDeleteMapping,
								testPutMapping,
							},
							ExpectedResponseCheck: v1alpha2.ExpectedResponseCheck{
								Type: v1alpha2.ExpectedResponseCheckTypeDefault,
							},
						},
					},
				},
				details: httpClient.HttpDetails{
					HttpResponse: httpClient.HttpResponse{
						Body:       `{"username": "john_doe"}`,
						Headers:    nil,
						StatusCode: 0,
					},
				},
				responseErr: nil,
			},
			want: want{
				result: ObserveRequestDetails{
					Details: httpClient.HttpDetails{
						HttpResponse: httpClient.HttpResponse{
							Body: `{"username": "john_doe"}`,
						},
					},
					Synced: false,
				},
				err: nil,
			},
		},
		"CustomResponseCheckFails": {
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							ExpectedResponseCheck: v1alpha2.ExpectedResponseCheck{
								Type:  v1alpha2.ExpectedResponseCheckTypeCustom,
								Logic: `.foo == "baz"`,
							},
						},
					},
				},
				details: httpClient.HttpDetails{
					HttpResponse: httpClient.HttpResponse{
						Body:       `{"username": "john_doe"}`,
						Headers:    nil,
						StatusCode: 0,
					},
				},
				responseErr: nil,
			},
			// Drift detected (CUSTOM check false) with no UPDATE mapping -> terminal error
			// rather than a silent Synced:false that the reconciler would report as
			// ReconcileSuccess. See bug-errors-not-propagated-to-status-conditions.md (B).
			want: want{
				result: ObserveRequestDetails{
					TerminalError: errUpdateMappingNotFound,
				},
				err: nil,
			},
		},
		"UnknownResponseCheckType": {
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							ExpectedResponseCheck: v1alpha2.ExpectedResponseCheck{
								Type: "UnknownType",
							},
						},
					},
				},
				details: httpClient.HttpDetails{
					HttpResponse: httpClient.HttpResponse{
						Body:       `{"username": "john_doe"}`,
						Headers:    nil,
						StatusCode: 0,
					},
				},
				responseErr: nil,
			},
			want: want{
				result: ObserveRequestDetails{
					Details: httpClient.HttpDetails{
						HttpResponse: httpClient.HttpResponse{
							Body: `{"username": "john_doe"}`,
						},
					},
					Synced: true,
				},
				err: nil,
			},
		},
		// DEFAULT check with NO UPDATE mapping: the DEFAULT check compares the response to the
		// UPDATE body, so with no UPDATE mapping it cannot detect drift and reports up-to-date.
		// This is the intended behavior for create-only resources (CREATE/OBSERVE only) that are
		// in sync — they are NOT a stuck state and stay Ready=True. The missing-mapping terminal
		// applies only to the CUSTOM check, which reports drift explicitly.
		"DefaultCheckNoUpdateMapping_StaysUpToDate": {
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{
								Body:    `{"username": "john_doe"}`,
								BaseUrl: "https://api.example.com/users",
							},
							Mappings: []v1alpha2.Mapping{
								testPostMapping,
								testGetMapping,
								testDeleteMapping, // no PUT/UPDATE
							},
							ExpectedResponseCheck: v1alpha2.ExpectedResponseCheck{
								Type: v1alpha2.ExpectedResponseCheckTypeDefault,
							},
						},
					},
				},
				details: httpClient.HttpDetails{
					HttpResponse: httpClient.HttpResponse{
						Body:       `{"username": "john_doe"}`,
						Headers:    nil,
						StatusCode: 200,
					},
				},
				responseErr: nil,
			},
			want: want{
				result: ObserveRequestDetails{
					Details: httpClient.HttpDetails{
						HttpResponse: httpClient.HttpResponse{
							Body:       `{"username": "john_doe"}`,
							StatusCode: 200,
						},
					},
					Synced: true,
				},
				err: nil,
			},
		},
		// CUSTOM check reports drift (false) with NO UPDATE mapping -> terminal error, and it
		// persists status.polling.terminalError so IsUpToDate short-circuits on the next
		// reconcile (Gap 3). Asserts the returned terminal (the persisted side-effect is
		// exercised by the controller test across reconciles).
		"CustomCheckDriftNoUpdateMapping_PersistsTerminal": {
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							ExpectedResponseCheck: v1alpha2.ExpectedResponseCheck{
								Type:  v1alpha2.ExpectedResponseCheckTypeCustom,
								Logic: `false`,
							},
						},
					},
				},
				details: httpClient.HttpDetails{
					HttpResponse: httpClient.HttpResponse{
						Body:       `{"username": "john_doe"}`,
						StatusCode: 200,
					},
				},
				responseErr: nil,
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
			},
			want: want{
				result: ObserveRequestDetails{
					TerminalError: errUpdateMappingNotFound,
				},
				err: nil,
			},
		},
	}

	for name, tc := range cases {
		tc := tc // Create local copies of loop variables

		t.Run(name, func(t *testing.T) {
			localKube := tc.args.localKube
			if localKube == nil {
				localKube = &test.MockClient{MockGet: test.NewMockGetFn(nil), MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil)}
			}
			svcCtx := service.NewServiceContext(tc.args.ctx, localKube, logging.NewNopLogger(), nil, nil)
			crCtx := service.NewRequestCRContext(tc.args.cr)
			got, gotErr := determineIfUpToDate(svcCtx, crCtx, tc.args.details, tc.args.responseErr)
			if diff := cmp.Diff(tc.want.err, gotErr, test.EquateErrors()); diff != "" {
				t.Fatalf("determineResponseCheck(...): -want error, +got error: %s", diff)
			}

			if diff := cmp.Diff(tc.want.result, got); diff != "" {
				t.Fatalf("determineResponseCheck(...): -want result, +got result: %s", diff)
			}
		})
	}
}

func Test_isObjectValidForObservation(t *testing.T) {
	type args struct {
		cr *v1alpha2.AsyncRequest
	}

	type want struct {
		valid bool
	}

	cases := map[string]struct {
		args args
		want want
	}{
		"ValidStatusCode": {
			args: args{
				cr: &v1alpha2.AsyncRequest{
					Status: v1alpha2.AsyncRequestStatus{
						Response: v1alpha2.Response{
							Body:       "",
							StatusCode: http.StatusOK,
						},
						RequestDetails: v1alpha2.Mapping{
							Method: http.MethodGet,
						},
					},
				},
			},
			want: want{
				valid: true,
			},
		},
		"EmptyStatusCode": {
			args: args{
				cr: &v1alpha2.AsyncRequest{
					Status: v1alpha2.AsyncRequestStatus{
						Response: v1alpha2.Response{
							Body:       "",
							StatusCode: 0,
						},
					},
				},
			},
			want: want{
				valid: false,
			},
		},
		"POSTMethodWithErrorResponse": {
			args: args{
				cr: &v1alpha2.AsyncRequest{
					Status: v1alpha2.AsyncRequestStatus{
						Response: v1alpha2.Response{
							Body:       "some response",
							StatusCode: http.StatusInternalServerError,
						},
						RequestDetails: v1alpha2.Mapping{
							Method: http.MethodPost,
						},
					},
				},
			},
			want: want{
				valid: false,
			},
		},
		"POSTMethodWithoutErrorResponse": {
			args: args{
				cr: &v1alpha2.AsyncRequest{
					Status: v1alpha2.AsyncRequestStatus{
						Response: v1alpha2.Response{
							Body:       "some response",
							StatusCode: http.StatusOK,
						},
						RequestDetails: v1alpha2.Mapping{
							Method: http.MethodPost,
						},
					},
				},
			},
			want: want{
				valid: true,
			},
		},
	}

	for name, tc := range cases {
		tc := tc // Create local copies of loop variables

		t.Run(name, func(t *testing.T) {
			crCtx := service.NewRequestCRContext(tc.args.cr)
			got := isObjectValidForObservation(crCtx)

			if diff := cmp.Diff(tc.want.valid, got); diff != "" {
				t.Errorf("isObjectValidForObservation(...): -want valid, +got valid: %s", diff)
			}
		})
	}
}

func Test_requestDetails(t *testing.T) {
	type args struct {
		ctx    context.Context
		cr     *v1alpha2.AsyncRequest
		action string
	}

	type want struct {
		result requestgen.RequestDetails
		err    error
	}

	cases := map[string]struct {
		args args
		want want
	}{
		"ValidMappingForGET": {
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{
								Body:    "{\"username\": \"john_doe\", \"email\": \"john.doe@example.com\"}",
								BaseUrl: "https://api.example.com/users",
							},
							Mappings: []v1alpha2.Mapping{
								testGetMapping,
							},
						},
					},
				},
				action: v1alpha2.ActionObserve,
			},
			want: want{
				result: requestgen.RequestDetails{
					Url: "https://api.example.com/users/",
					Body: httpClient.Data{
						Encrypted: "",
						Decrypted: "",
					},
					Headers: httpClient.Data{
						Encrypted: map[string][]string{},
						Decrypted: map[string][]string{},
					},
				},
				err: nil,
			},
		},
		"ValidMappingForPOST": {
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{
								Body:    "{\"username\": \"john_doe\", \"email\": \"john.doe@example.com\"}",
								BaseUrl: "https://api.example.com/users",
							}, Mappings: []v1alpha2.Mapping{
								testPostMapping,
							},
						},
					},
				},
				action: v1alpha2.ActionCreate,
			},
			want: want{
				result: requestgen.RequestDetails{
					Url: "https://api.example.com/users",
					Body: httpClient.Data{
						Encrypted: `{"email":"john.doe@example.com","username":"john_doe"}`,
						Decrypted: `{"email":"john.doe@example.com","username":"john_doe"}`,
					},
					Headers: httpClient.Data{
						Encrypted: map[string][]string{},
						Decrypted: map[string][]string{},
					},
				},
				err: nil,
			},
		},
		"MappingNotFound": {
			args: args{
				ctx: context.Background(),
				cr: &v1alpha2.AsyncRequest{
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{},
					},
				},
				action: "UNKNOWN_METHOD",
			},
			want: want{
				result: requestgen.RequestDetails{},
				err:    errors.Errorf(requestmapping.ErrMappingNotFound, "UNKNOWN_METHOD", http.MethodGet),
			},
		},
	}

	for name, tc := range cases {
		tc := tc

		t.Run(name, func(t *testing.T) {
			mapping, err := requestmapping.GetMapping(&tc.args.cr.Spec.ForProvider, tc.args.action, logging.NewNopLogger())
			if err != nil {
				if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
					t.Fatalf("requestDetails(...): -want error, +got error: %s", diff)
				}
				return
			}
			svcCtx := service.NewServiceContext(tc.args.ctx, nil, logging.NewNopLogger(), nil, nil)
			crCtx := service.NewRequestCRContext(tc.args.cr)
			got, gotErr := requestgen.GenerateValidRequestDetails(svcCtx, crCtx, mapping)
			if diff := cmp.Diff(tc.want.err, gotErr, test.EquateErrors()); diff != "" {
				t.Fatalf("requestDetails(...): -want error, +got error: %s", diff)
			}

			if diff := cmp.Diff(tc.want.result, got); diff != "" {
				t.Fatalf("requestDetails(...): -want result, +got result: %s", diff)
			}
		})
	}
}

// TestIsUpToDate_InFlightAnchor_Routing covers the Gap-1 resume-routing fix: when a
// polling.response anchor is in flight, IsUpToDate must not fire OBSERVE and must route
// the resume back to DeployAction via the right controller action.
func TestIsUpToDate_InFlightAnchor_Routing(t *testing.T) {
	// A resource whose CREATE mapping polls and whose OBSERVE/UPDATE URLs depend on
	// .status.externalRef — the Vertex deployedModel shape (no UPDATE mapping would also
	// exercise this, but here we keep UPDATE to prove the externalRef gate, not the
	// mapping lookup, drives routing).
	buildCR := func(setExternalRef bool) *v1alpha2.AsyncRequest {
		cr := httpRequest()
		cr.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
			{Method: "POST", Action: "CREATE", URL: ".payload.baseUrl", Polling: &common.Polling{URL: ".response.body.name", Done: ".poll.response.body.done == true"}},
			{Method: "GET", Action: "OBSERVE", URL: ".payload.baseUrl + \"/\" + .status.externalRef"},
		}
		cr.SetPollingResponse(map[string]interface{}{"body": map[string]interface{}{"name": "op-1"}})
		if setExternalRef {
			cr.Status.ExternalRef = "model-789"
		}
		return cr
	}

	t.Run("NoExternalRef_ReturnsObjectNotFound_RoutesToCreate", func(t *testing.T) {
		// CREATE+poll still running: externalRef unset → ErrObjectNotFound so the controller
		// calls Create() and resumes the poll via the CREATE mapping. This is the case that
		// breaks for no-UPDATE-mapping resources if routed through Update().
		cr := buildCR(false)
		svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), &MockHttpClient{
			MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
				t.Errorf("OBSERVE must not fire while the anchor is in flight; got %s %s", method, url)
				return httpClient.HttpDetails{}, nil
			},
		}, nil)
		_, err := IsUpToDate(svcCtx, service.NewRequestCRContext(cr))
		if err == nil || err.Error() != observe.ErrObjectNotFound {
			t.Fatalf("expected ErrObjectNotFound to route resume through Create(), got %v", err)
		}
	})

	t.Run("ExternalRefSet_ReturnsNotUpToDate_RoutesToUpdateOrDelete", func(t *testing.T) {
		// An UPDATE/REMOVE LRO is in flight with externalRef already resolved: report
		// not-up-to-date so the controller calls Update() / holds the finalizer for Delete(),
		// whose URLs resolve via .status.externalRef.
		cr := buildCR(true)
		svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), &MockHttpClient{
			MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
				t.Errorf("OBSERVE must not fire while the anchor is in flight; got %s %s", method, url)
				return httpClient.HttpDetails{}, nil
			},
		}, nil)
		got, err := IsUpToDate(svcCtx, service.NewRequestCRContext(cr))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Synced {
			t.Error("expected Synced=false (not up to date) so the controller re-enters DeployAction")
		}
	})
}

// TestIsUpToDate_TerminalClear_ResetsStartedAt covers Gap 2 and Gap 5: a spec change
// (generation drift past a terminal failure) clears TerminalError and resets StartedAt so
// the resumed operation gets a fresh polling.timeout budget, then falls through to resume.
func TestIsUpToDate_TerminalClear_ResetsStartedAt(t *testing.T) {
	startedAt := v1.Now()
	cr := httpRequest()
	cr.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
		{Method: "POST", Action: "CREATE", URL: ".payload.baseUrl", Polling: &common.Polling{URL: ".response.body.name", Done: ".poll.response.body.done == true"}},
		{Method: "GET", Action: "OBSERVE", URL: ".payload.baseUrl + \"/\" + .status.externalRef"},
	}
	// Terminal failure recorded at generation 1 with a stale StartedAt.
	cr.Status.Polling.TerminalError = "polling timeout after 30s for operation op-1"
	cr.Status.Polling.StartedAt = &startedAt
	cr.Status.SetObservedGeneration(1)
	cr.Generation = 2 // spec changed since the terminal failure → generation drift
	// No anchor set here so IsUpToDate falls through to a normal observe after the clear;
	// the assertion is solely about the clear side-effects.
	cr.Status.Response = v1alpha2.Response{StatusCode: 0, Body: ""}

	svcCtx := service.NewServiceContext(context.Background(), mockKube(), logging.NewNopLogger(), &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			// OBSERVE GET for an uncreated resource: returns 404 → ErrObjectNotFound.
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{StatusCode: http.StatusNotFound}}, nil
		},
	}, nil)

	_, _ = IsUpToDate(svcCtx, service.NewRequestCRContext(cr))

	if cr.Status.Polling.TerminalError != "" {
		t.Errorf("expected TerminalError cleared on generation drift, got %q", cr.Status.Polling.TerminalError)
	}
	if cr.Status.Polling.StartedAt != nil {
		t.Errorf("expected StartedAt reset to nil for a fresh timeout budget, got %v", cr.Status.Polling.StartedAt)
	}
}
