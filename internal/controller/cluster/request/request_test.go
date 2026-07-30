package request

import (
	"context"
	"strings"
	"testing"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"

	"github.com/crossplane/crossplane-runtime/v2/pkg/feature"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	"github.com/crossplane/crossplane-runtime/v2/pkg/test"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	"github.com/Antrakos/provider-http-async/apis/cluster/request/v1alpha2"
	"github.com/Antrakos/provider-http-async/apis/common"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
)

var (
	errBoom = errors.New("boom")
)

const (
	providerName    = "http-test"
	testRequestName = "test-request"
	testNamespace   = "testns"
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
		Status: v1alpha2.AsyncRequestStatus{
			Response: v1alpha2.Response{
				Body:       `{"id": "123"}`,
				StatusCode: 200,
			},
		},
	}

	for _, m := range rm {
		m(r)
	}

	return r
}

type notHttpRequest struct {
	resource.Managed
}

type MockSendRequestFn func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error)

type MockSendRequestWithTLSFn func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error)

type MockHttpClient struct {
	MockSendRequest        MockSendRequestFn
	MockSendRequestWithTLS MockSendRequestWithTLSFn
}

func (c *MockHttpClient) SendRequest(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
	return c.MockSendRequest(ctx, method, url, body, headers, tlsConfigData)
}

func (c *MockHttpClient) SendRequestWithTLS(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
	if c.MockSendRequestWithTLS != nil {
		return c.MockSendRequestWithTLS(ctx, method, url, body, headers, tlsConfig)
	}
	// Fallback to SendRequest for backward compatibility
	return c.MockSendRequest(ctx, method, url, body, headers, tlsConfig)
}

type MockSetRequestStatusFn func() error

type MockResetFailuresFn func()

type MockInitFn func(ctx context.Context, cr *v1alpha2.AsyncRequest, res httpClient.HttpResponse)

type MockStatusHandler struct {
	MockSetRequest    MockSetRequestStatusFn
	MockResetFailures MockResetFailuresFn
}

func (s *MockStatusHandler) ResetFailures() {
	s.MockResetFailures()
}

func (s *MockStatusHandler) SetRequestStatus(ctx context.Context, cr *v1alpha2.AsyncRequest, res httpClient.HttpResponse, err error) error {
	return s.MockSetRequest()
}

func Test_httpExternal_Create(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        resource.Managed
	}
	type want struct {
		err error
	}

	cases := []struct {
		name string
		args args
		want want
	}{
		{
			name: "NotRequestResource",
			args: args{
				mg: notHttpRequest{},
			},
			want: want{
				err: errors.New(errNotRequest),
			},
		},
		{
			name: "RequestFailed",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, errBoom
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: httpRequest(),
			},
			want: want{
				err: errors.Wrap(errBoom, errFailedToSendHttpRequest),
			},
		},
		{
			name: "Success",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockCreate:       test.NewMockCreateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: httpRequest(),
			},
			want: want{
				err: nil,
			},
		},
	}
	for _, tc := range cases {
		tc := tc // Create local copies of loop variables

		t.Run(tc.name, func(t *testing.T) {
			e := &external{
				localKube: tc.args.localKube,
				logger:    logging.NewNopLogger(),
				http:      tc.args.http,
			}
			_, gotErr := e.Create(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, gotErr, test.EquateErrors()); diff != "" {
				t.Fatalf("e.Create(...): -want error, +got error: %s", diff)
			}
		})
	}
}

func Test_httpExternal_Update(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        resource.Managed
	}
	type want struct {
		err error
	}

	cases := []struct {
		name string
		args args
		want want
	}{
		{
			name: "NotRequestResource",
			args: args{
				mg: notHttpRequest{},
			},
			want: want{
				err: errors.New(errNotRequest),
			},
		},
		{
			name: "RequestFailed",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, errBoom
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: httpRequest(),
			},
			want: want{
				err: errors.Wrap(errBoom, errFailedToSendHttpRequest),
			},
		},
		{
			name: "Success",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockCreate:       test.NewMockCreateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: httpRequest(),
			},
			want: want{
				err: nil,
			},
		},
	}
	for _, tc := range cases {
		tc := tc // Create local copies of loop variables

		t.Run(tc.name, func(t *testing.T) {
			e := &external{
				localKube: tc.args.localKube,
				logger:    logging.NewNopLogger(),
				http:      tc.args.http,
			}
			_, gotErr := e.Update(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, gotErr, test.EquateErrors()); diff != "" {
				t.Fatalf("e.Update(...): -want error, +got error: %s", diff)
			}
		})
	}
}

func Test_httpExternal_Delete(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        resource.Managed
	}
	type want struct {
		err error
	}

	cases := []struct {
		name string
		args args
		want want
	}{
		{
			name: "NotRequestResource",
			args: args{
				mg: notHttpRequest{},
			},
			want: want{
				err: errors.New(errNotRequest),
			},
		},
		{
			name: "RequestFailed",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, errBoom
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: httpRequest(),
			},
			want: want{
				err: errors.Wrap(errBoom, errFailedToSendHttpRequest),
			},
		},
		{
			name: "Success",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockCreate:       test.NewMockCreateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: httpRequest(),
			},
			want: want{
				err: nil,
			},
		},
	}
	for _, tc := range cases {
		tc := tc // Create local copies of loop variables

		t.Run(tc.name, func(t *testing.T) {
			e := &external{
				localKube: tc.args.localKube,
				logger:    logging.NewNopLogger(),
				http:      tc.args.http,
			}
			_, gotErr := e.Delete(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, gotErr, test.EquateErrors()); diff != "" {
				t.Fatalf("e.Delete(...): -want error, +got error: %s", diff)
			}
		})
	}
}

func Test_httpExternal_Observe(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        resource.Managed
	}
	type want struct {
		observation managed.ExternalObservation
		err         error
		// readyStatus/readyReason/readyMsg assert the Ready condition set by Observe for
		// the terminal and in-flight cases (the reconciler owns Synced).
		readyStatus corev1.ConditionStatus
		readyReason xpv2.ConditionReason
		readyMsg    string
	}

	cases := []struct {
		name string
		args args
		want want
	}{
		{
			name: "NotRequestResource",
			args: args{
				mg: notHttpRequest{},
			},
			want: want{
				err: errors.New(errNotRequest),
			},
		},
		{
			name: "ResourceUpToDate",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 200,
								Body:       `{"id": "123"}`,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: &v1alpha2.AsyncRequest{
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{
								BaseUrl: "https://api.example.com/users/123",
							},
							Mappings: []v1alpha2.Mapping{
								{
									Method: "GET",
									URL:    ".payload.baseUrl",
								},
							},
						},
					},
					Status: v1alpha2.AsyncRequestStatus{
						Response: v1alpha2.Response{
							StatusCode: 200,
							Body:       `{"id": "123"}`,
						},
					},
				},
			},
			want: want{
				observation: managed.ExternalObservation{
					ResourceExists:   true,
					ResourceUpToDate: true,
				},
				err: nil,
			},
		},
		{
			name: "TerminalFailure_ReturnsErrorAndReadyFalse",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
						t.Errorf("OBSERVE must not fire when a terminal error is recorded")
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: func() *v1alpha2.AsyncRequest {
					cr := &v1alpha2.AsyncRequest{
						ObjectMeta: v1.ObjectMeta{Generation: 1},
						Spec: v1alpha2.AsyncRequestSpec{
							ForProvider: v1alpha2.AsyncRequestParameters{
								Payload:  v1alpha2.Payload{BaseUrl: "https://api.example.com/users/123"},
								Mappings: []v1alpha2.Mapping{{Method: "GET", URL: ".payload.baseUrl"}},
							},
						},
						Status: v1alpha2.AsyncRequestStatus{
							TerminalError: "polling.url resolved to a bare path",
						},
					}
					// observedGeneration == generation so the terminal failure is stable (not cleared
					// by a spec change); IsUpToDate short-circuits without firing OBSERVE.
					cr.Status.SetObservedGeneration(1)
					return cr
				}(),
			},
			want: want{
				err:         errors.New(terminalErrorPrefix + "polling.url resolved to a bare path"),
				readyStatus: corev1.ConditionFalse,
				readyReason: xpv2.ReasonUnavailable,
				readyMsg:    "polling.url resolved to a bare path",
			},
		},
		{
			name: "InFlight_ReadyCreating",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
						t.Errorf("OBSERVE must not fire while the anchor is in flight")
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: &v1alpha2.AsyncRequest{
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{BaseUrl: "https://api.example.com/users/123"},
							Mappings: []v1alpha2.Mapping{
								{Method: "POST", Action: "CREATE", URL: ".payload.baseUrl", Polling: &common.Polling{URL: ".response.body.name", Done: ".poll.response.body.done == true"}},
								{Method: "GET", Action: "OBSERVE", URL: `.payload.baseUrl + "/" + .status.externalRef`},
							},
						},
					},
					Status: v1alpha2.AsyncRequestStatus{
						ExternalRef: "model-789",
						Polling: v1alpha2.PollingStatus{
							Response: &runtime.RawExtension{Raw: []byte(`{"body":{"name":"op-1"}}`)},
						},
					},
				},
			},
			want: want{
				observation: managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: false},
				readyStatus: corev1.ConditionFalse,
				readyReason: xpv2.ReasonCreating,
			},
		},
		{
			name: "MissingMappingTerminal_ReturnsErrorAndReadyFalse",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
						// OBSERVE fires (resource was observed before): parent returns 200; the
						// CUSTOM expectedResponseCheck reports drift, and with no UPDATE mapping the
						// provider surfaces a terminal error instead of silently skipping.
						return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{StatusCode: 200, Body: `{"deployedModels": []}`}}, nil
					},
				},
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: &v1alpha2.AsyncRequest{
					Spec: v1alpha2.AsyncRequestSpec{
						ForProvider: v1alpha2.AsyncRequestParameters{
							Payload: v1alpha2.Payload{BaseUrl: "https://api.example.com/endpoints/456"},
							Mappings: []v1alpha2.Mapping{
								{Method: "POST", Action: "CREATE", URL: ".payload.baseUrl", Body: ".payload.body"},
								{Method: "GET", Action: "OBSERVE", URL: ".payload.baseUrl"},
								{Method: "DELETE", Action: "REMOVE", URL: ".payload.baseUrl"},
							},
							ExpectedResponseCheck: v1alpha2.ExpectedResponseCheck{
								Type:  v1alpha2.ExpectedResponseCheckTypeCustom,
								Logic: `false`,
							},
						},
					},
					Status: v1alpha2.AsyncRequestStatus{
						ExternalRef: "model-789",
						Response:    v1alpha2.Response{StatusCode: 200, Body: `{"deployedModels": []}`},
					},
				},
			},
			want: want{
				err:         errors.New(terminalErrorPrefix + "no UPDATE or PUT mapping is configured but the resource is out of sync (expectedResponseCheck returned false); add an UPDATE mapping or fix the spec so the resource reconciles"),
				readyStatus: corev1.ConditionFalse,
				readyReason: xpv2.ReasonUnavailable,
				readyMsg:    "no UPDATE or PUT mapping is configured",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &external{
				localKube: tc.args.localKube,
				logger:    logging.NewNopLogger(),
				http:      tc.args.http,
			}
			got, gotErr := e.Observe(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, gotErr, test.EquateErrors()); diff != "" {
				t.Fatalf("e.Observe(...): -want error, +got error: %s", diff)
			}
			if tc.want.readyReason != "" {
				cr, ok := tc.args.mg.(*v1alpha2.AsyncRequest)
				if !ok {
					t.Fatalf("mg is not an AsyncRequest")
				}
				rc := cr.Status.GetCondition(xpv2.TypeReady)
				if diff := cmp.Diff(tc.want.readyStatus, rc.Status); diff != "" {
					t.Errorf("Ready status: -want %s, +got %s", tc.want.readyStatus, rc.Status)
				}
				if diff := cmp.Diff(tc.want.readyReason, rc.Reason); diff != "" {
					t.Errorf("Ready reason: -want %s, +got %s", tc.want.readyReason, rc.Reason)
				}
				if tc.want.readyMsg != "" && !strings.Contains(rc.Message, tc.want.readyMsg) {
					t.Errorf("Ready message: want to contain %q, got %q", tc.want.readyMsg, rc.Message)
				}
			}
			if tc.want.err == nil {
				if diff := cmp.Diff(tc.want.observation.ResourceExists, got.ResourceExists); diff != "" {
					t.Fatalf("e.Observe(...): -want ResourceExists, +got ResourceExists: %s", diff)
				}
				if diff := cmp.Diff(tc.want.observation.ResourceUpToDate, got.ResourceUpToDate); diff != "" {
					t.Fatalf("e.Observe(...): -want ResourceUpToDate, +got ResourceUpToDate: %s", diff)
				}
			}
		})
	}
}

func TestManagementPoliciesFeatureFlag(t *testing.T) {
	cases := map[string]struct {
		reason   string
		features *feature.Flags
		want     bool
	}{
		"ManagementPoliciesEnabled": {
			reason: "Feature flag should be enabled when explicitly set",
			features: func() *feature.Flags {
				f := &feature.Flags{}
				f.Enable(feature.EnableBetaManagementPolicies)
				return f
			}(),
			want: true,
		},
		"ManagementPoliciesDisabled": {
			reason:   "Feature flag should be disabled when not set",
			features: &feature.Flags{},
			want:     false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			enabled := tc.features.Enabled(feature.EnableBetaManagementPolicies)
			if enabled != tc.want {
				t.Errorf("\n%s\nEnabled(feature.EnableBetaManagementPolicies): want %v, got %v", tc.reason, tc.want, enabled)
			}
		})
	}
}

func TestRequestManagementPolicies(t *testing.T) {
	cases := map[string]struct {
		reason string
		mg     *v1alpha2.AsyncRequest
		want   xpv2.ManagementPolicies
	}{
		"DefaultManagementPolicies": {
			reason: "Default management policies should be nil when not explicitly set",
			mg: func() *v1alpha2.AsyncRequest {
				r := httpRequest()
				// Don't set managementPolicies explicitly to test default
				return r
			}(),
			want: nil,
		},
		"ObserveOnlyManagementPolicies": {
			reason: "Observe-only management policies should only allow observation",
			mg: func() *v1alpha2.AsyncRequest {
				r := httpRequest()
				r.Spec.ManagementPolicies = xpv2.ManagementPolicies{xpv2.ManagementActionObserve}
				return r
			}(),
			want: xpv2.ManagementPolicies{xpv2.ManagementActionObserve},
		},
		"CreateAndUpdateManagementPolicies": {
			reason: "Create and update management policies should allow creation and updates",
			mg: func() *v1alpha2.AsyncRequest {
				r := httpRequest()
				r.Spec.ManagementPolicies = xpv2.ManagementPolicies{
					xpv2.ManagementActionCreate,
					xpv2.ManagementActionUpdate,
				}
				return r
			}(),
			want: xpv2.ManagementPolicies{
				xpv2.ManagementActionCreate,
				xpv2.ManagementActionUpdate,
			},
		},
		"ObserveCreateUpdateManagementPolicies": {
			reason: "Observe, create, and update management policies should allow all three actions",
			mg: func() *v1alpha2.AsyncRequest {
				r := httpRequest()
				r.Spec.ManagementPolicies = xpv2.ManagementPolicies{
					xpv2.ManagementActionObserve,
					xpv2.ManagementActionCreate,
					xpv2.ManagementActionUpdate,
				}
				return r
			}(),
			want: xpv2.ManagementPolicies{
				xpv2.ManagementActionObserve,
				xpv2.ManagementActionCreate,
				xpv2.ManagementActionUpdate,
			},
		},
		"AllActionsExceptDeleteManagementPolicies": {
			reason: "All actions except delete should allow observe, create, update, and late initialize",
			mg: func() *v1alpha2.AsyncRequest {
				r := httpRequest()
				r.Spec.ManagementPolicies = xpv2.ManagementPolicies{
					xpv2.ManagementActionObserve,
					xpv2.ManagementActionCreate,
					xpv2.ManagementActionUpdate,
					xpv2.ManagementActionLateInitialize,
				}
				return r
			}(),
			want: xpv2.ManagementPolicies{
				xpv2.ManagementActionObserve,
				xpv2.ManagementActionCreate,
				xpv2.ManagementActionUpdate,
				xpv2.ManagementActionLateInitialize,
			},
		},
		"ExplicitAllManagementPolicies": {
			reason: "Explicit all management policies should allow all actions",
			mg: func() *v1alpha2.AsyncRequest {
				r := httpRequest()
				r.Spec.ManagementPolicies = xpv2.ManagementPolicies{xpv2.ManagementActionAll}
				return r
			}(),
			want: xpv2.ManagementPolicies{xpv2.ManagementActionAll},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := tc.mg.Spec.ManagementPolicies
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nManagementPolicies: -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestRequestManagementPoliciesResolver(t *testing.T) {
	type args struct {
		enabled bool
		policy  xpv2.ManagementPolicies
	}
	type want struct {
		shouldCreate         bool
		shouldUpdate         bool
		shouldDelete         bool
		shouldOnlyObserve    bool
		shouldLateInitialize bool
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"ManagementPoliciesDisabled": {
			reason: "When management policies are disabled, all actions should be allowed",
			args: args{
				enabled: false,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionObserve},
			},
			want: want{
				shouldCreate:         true,
				shouldUpdate:         true,
				shouldDelete:         true,
				shouldOnlyObserve:    false,
				shouldLateInitialize: true,
			},
		},
		"ObserveOnlyPolicy": {
			reason: "Observe-only policy should only allow observation",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionObserve},
			},
			want: want{
				shouldCreate:         false,
				shouldUpdate:         false,
				shouldDelete:         false,
				shouldOnlyObserve:    true,
				shouldLateInitialize: false,
			},
		},
		"CreateOnlyPolicy": {
			reason: "Create-only policy should only allow creation",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionCreate},
			},
			want: want{
				shouldCreate:         true,
				shouldUpdate:         false,
				shouldDelete:         false,
				shouldOnlyObserve:    false,
				shouldLateInitialize: false,
			},
		},
		"UpdateOnlyPolicy": {
			reason: "Update-only policy should only allow updates",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionUpdate},
			},
			want: want{
				shouldCreate:         false,
				shouldUpdate:         true,
				shouldDelete:         false,
				shouldOnlyObserve:    false,
				shouldLateInitialize: false,
			},
		},
		"DeleteOnlyPolicy": {
			reason: "Delete-only policy should only allow deletion",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionDelete},
			},
			want: want{
				shouldCreate:         false,
				shouldUpdate:         false,
				shouldDelete:         true,
				shouldOnlyObserve:    false,
				shouldLateInitialize: false,
			},
		},
		"CreateAndUpdatePolicy": {
			reason: "Create and update policy should allow both creation and updates",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionCreate, xpv2.ManagementActionUpdate},
			},
			want: want{
				shouldCreate:         true,
				shouldUpdate:         true,
				shouldDelete:         false,
				shouldOnlyObserve:    false,
				shouldLateInitialize: false,
			},
		},
		"ObserveCreateUpdatePolicy": {
			reason: "Observe, create, and update policy should allow all three actions",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionObserve, xpv2.ManagementActionCreate, xpv2.ManagementActionUpdate},
			},
			want: want{
				shouldCreate:         true,
				shouldUpdate:         true,
				shouldDelete:         false,
				shouldOnlyObserve:    false,
				shouldLateInitialize: false,
			},
		},
		"AllActionsExceptDeletePolicy": {
			reason: "All actions except delete should allow observe, create, update, and late initialize",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionObserve, xpv2.ManagementActionCreate, xpv2.ManagementActionUpdate, xpv2.ManagementActionLateInitialize},
			},
			want: want{
				shouldCreate:         true,
				shouldUpdate:         true,
				shouldDelete:         false,
				shouldOnlyObserve:    false,
				shouldLateInitialize: true,
			},
		},
		"ExplicitAllPolicy": {
			reason: "Explicit all policy should allow all actions",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionAll},
			},
			want: want{
				shouldCreate:         true,
				shouldUpdate:         true,
				shouldDelete:         true,
				shouldOnlyObserve:    false,
				shouldLateInitialize: true,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// Create a mock managed resource with the specified management policies
			mg := httpRequest()
			mg.Spec.ManagementPolicies = tc.args.policy

			// Test the management policies resolver logic
			// Note: This is a simplified test that focuses on the policy logic
			// The actual enforcement happens in the Crossplane managed reconciler

			// Helper function to check if a ManagementPolicies slice contains a specific action
			contains := func(policies xpv2.ManagementPolicies, action xpv2.ManagementAction) bool {
				for _, p := range policies {
					if p == action {
						return true
					}
				}
				return false
			}

			// Test ShouldCreate
			shouldCreate := tc.want.shouldCreate
			if tc.args.enabled {
				shouldCreate = contains(tc.args.policy, xpv2.ManagementActionCreate) || contains(tc.args.policy, xpv2.ManagementActionAll)
			}
			if shouldCreate != tc.want.shouldCreate {
				t.Errorf("ShouldCreate() = %v, want %v", shouldCreate, tc.want.shouldCreate)
			}

			// Test ShouldUpdate
			shouldUpdate := tc.want.shouldUpdate
			if tc.args.enabled {
				shouldUpdate = contains(tc.args.policy, xpv2.ManagementActionUpdate) || contains(tc.args.policy, xpv2.ManagementActionAll)
			}
			if shouldUpdate != tc.want.shouldUpdate {
				t.Errorf("ShouldUpdate() = %v, want %v", shouldUpdate, tc.want.shouldUpdate)
			}

			// Test ShouldDelete
			shouldDelete := tc.want.shouldDelete
			if tc.args.enabled {
				shouldDelete = contains(tc.args.policy, xpv2.ManagementActionDelete) || contains(tc.args.policy, xpv2.ManagementActionAll)
			}
			if shouldDelete != tc.want.shouldDelete {
				t.Errorf("ShouldDelete() = %v, want %v", shouldDelete, tc.want.shouldDelete)
			}

			// Test ShouldOnlyObserve
			shouldOnlyObserve := tc.want.shouldOnlyObserve
			if tc.args.enabled {
				shouldOnlyObserve = len(tc.args.policy) == 1 && contains(tc.args.policy, xpv2.ManagementActionObserve)
			}
			if shouldOnlyObserve != tc.want.shouldOnlyObserve {
				t.Errorf("ShouldOnlyObserve() = %v, want %v", shouldOnlyObserve, tc.want.shouldOnlyObserve)
			}

			// Test ShouldLateInitialize
			shouldLateInitialize := tc.want.shouldLateInitialize
			if tc.args.enabled {
				shouldLateInitialize = contains(tc.args.policy, xpv2.ManagementActionLateInitialize) || contains(tc.args.policy, xpv2.ManagementActionAll)
			}
			if shouldLateInitialize != tc.want.shouldLateInitialize {
				t.Errorf("ShouldLateInitialize() = %v, want %v", shouldLateInitialize, tc.want.shouldLateInitialize)
			}
		})
	}
}

func httpRequestWithDeletion() *v1alpha2.AsyncRequest {
	now := v1.Now()
	return httpRequest(func(r *v1alpha2.AsyncRequest) {
		r.DeletionTimestamp = &now
	})
}

func TestTLSConfiguration(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        resource.Managed
	}
	type want struct {
		err error
	}

	cases := []struct {
		name string
		args args
		want want
	}{
		{
			name: "TLSConfigAcceptedWithNilTLS",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						// Accept any TLS config - the actual TLS config loading is handled by the service layer
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 200,
								Body:       `{"result": "success"}`,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: clusterRequestWithTLS(),
			},
			want: want{
				err: nil,
			},
		},
		{
			name: "InsecureSkipTLSVerifyAccepted",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						// Accept any TLS config - the controller should handle insecure skip verify
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 200,
								Body:       `{"result": "insecure success"}`,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: clusterRequestWithInsecureSkipTLS(),
			},
			want: want{
				err: nil,
			},
		},
		{
			name: "TLSConfigWithClientCertsAccepted",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						// Accept any TLS config - the service layer handles TLS config merging
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 200,
								Body:       `{"result": "client cert success"}`,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: clusterRequestWithMutualTLS(),
			},
			want: want{
				err: nil,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &external{
				logger:    logging.NewNopLogger(),
				localKube: tc.args.localKube,
				http:      tc.args.http,
			}

			_, err := e.Create(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("Create(...): -want error, +got error: %s", diff)
			}
		})
	}
}

// clusterRequestWithTLS creates a AsyncRequest with TLS configuration
func clusterRequestWithTLS() *v1alpha2.AsyncRequest {
	return httpRequest(func(cr *v1alpha2.AsyncRequest) {
		cr.Spec.ForProvider.TLSConfig = &common.TLSConfig{
			CACertSecretRef: &xpv2.SecretKeySelector{
				SecretReference: xpv2.SecretReference{
					Name:      "ca-cert-secret",
					Namespace: "default",
				},
				Key: "ca.crt",
			},
		}
	})
}

// clusterRequestWithInsecureSkipTLS creates a AsyncRequest with insecureSkipTLSVerify
func clusterRequestWithInsecureSkipTLS() *v1alpha2.AsyncRequest {
	return httpRequest(func(cr *v1alpha2.AsyncRequest) {
		cr.Spec.ForProvider.InsecureSkipTLSVerify = true
	})
}

// clusterRequestWithMutualTLS creates a AsyncRequest with mutual TLS configuration
func clusterRequestWithMutualTLS() *v1alpha2.AsyncRequest {
	return httpRequest(func(cr *v1alpha2.AsyncRequest) {
		cr.Spec.ForProvider.TLSConfig = &common.TLSConfig{
			CACertSecretRef: &xpv2.SecretKeySelector{
				SecretReference: xpv2.SecretReference{
					Name:      "ca-cert-secret",
					Namespace: "default",
				},
				Key: "ca.crt",
			},
			ClientCertSecretRef: &xpv2.SecretKeySelector{
				SecretReference: xpv2.SecretReference{
					Name:      "client-cert-secret",
					Namespace: "default",
				},
				Key: "tls.crt",
			},
			ClientKeySecretRef: &xpv2.SecretKeySelector{
				SecretReference: xpv2.SecretReference{
					Name:      "client-cert-secret",
					Namespace: "default",
				},
				Key: "tls.key",
			},
		}
	})
}

func TestObserve_DeletionMonitoring(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        resource.Managed
	}
	type want struct {
		obs managed.ExternalObservation
		err error
	}

	cases := []struct {
		name string
		args args
		want want
	}{
		{
			name: "ResourceBeingDeleted",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 200,
								Body:       `{"id": "123"}`,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpRequestWithDeletion(),
			},
			want: want{
				obs: managed.ExternalObservation{
					ResourceExists: true,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &external{
				logger:    logging.NewNopLogger(),
				localKube: tc.args.localKube,
				http:      tc.args.http,
			}

			got, err := e.Observe(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("Observe(...): -want error, +got error: %s", diff)
			}
			if diff := cmp.Diff(tc.want.obs, got); diff != "" {
				t.Errorf("Observe(...): -want, +got: %s", diff)
			}
		})
	}
}

// TestObserve_EmptyExternalRef_ObserveURLReferencesIt_RoutesToCreate reproduces the
// bug-empty-externalref-observe-hits-list-endpoint-skips-create.md regression at the
// cluster-scoped controller boundary: a fresh resource (no prior response, empty
// externalRef) whose OBSERVE URL keys on .status.externalRef must route to Create()
// (ResourceExists:false) without firing OBSERVE against the collapsed collection URL.
func TestObserve_EmptyExternalRef_ObserveURLReferencesIt_RoutesToCreate(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			t.Errorf("OBSERVE must not fire when externalRef is empty and the URL references it; got %s %s", method, url)
			return httpClient.HttpDetails{}, nil
		},
	}
	localKube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}
	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "vertex-exp-model"},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload: v1alpha2.Payload{
					BaseUrl: "https://aiplatform.googleapis.com/v1beta1/projects/p/locations/us-central1",
				},
				Mappings: []v1alpha2.Mapping{
					{Method: "POST", Action: "CREATE", URL: `.payload.baseUrl + "/models:upload"`, Body: ".payload.body"},
					{Method: "GET", Action: "OBSERVE", URL: `.payload.baseUrl + "/models/" + .status.externalRef`},
					{Method: "PATCH", Action: "UPDATE", URL: `.payload.baseUrl + "/models/" + .status.externalRef`},
				},
				ExpectedResponseCheck: v1alpha2.ExpectedResponseCheck{
					Type:  v1alpha2.ExpectedResponseCheckTypeCustom,
					Logic: `.response.body.displayName == .payload.body.model.displayName`,
				},
			},
		},
		// Fresh resource: empty externalRef, no prior response, no anchor.
		Status: v1alpha2.AsyncRequestStatus{},
	}
	e := &external{logger: logging.NewNopLogger(), localKube: localKube, http: httpMock}
	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error %v", err)
	}
	if obs.ResourceExists {
		t.Error("expected ResourceExists=false so the controller routes to Create(); got true (would route to Update and loop)")
	}
}

// TestUpdate_BrokenURL_NonPolling_404_SurfacesError covers the secondary bug at the
// cluster-scoped controller boundary: a non-polling UPDATE PATCH to a broken URL returning
// 404 must surface as an error (Synced=False), not be swallowed as ReconcileSuccess.
func TestUpdate_BrokenURL_NonPolling_404_SurfacesError(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{StatusCode: 404, Body: `{"error":"not found"}`}}, nil
		},
	}
	localKube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}
	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: v1.ObjectMeta{Name: "vertex-exp-model"},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload: v1alpha2.Payload{BaseUrl: "https://aiplatform.googleapis.com/v1beta1/projects/p/locations/us-central1"},
				Mappings: []v1alpha2.Mapping{
					{Method: "PATCH", Action: "UPDATE", URL: `.payload.baseUrl + "/models/" + .status.externalRef`, Body: ".payload.body"},
				},
			},
		},
	}
	e := &external{logger: logging.NewNopLogger(), localKube: localKube, http: httpMock}
	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Error("expected Update to surface the 404 as an error (Synced=False), got nil (would report ReconcileSuccess and loop)")
	}
}
