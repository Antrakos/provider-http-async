package disposablerequest

import (
	"testing"

	"github.com/Antrakos/provider-http-async/apis/cluster/disposablerequest/v1alpha2"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
)

func TestIsResponseAsExpected(t *testing.T) {
	cases := map[string]struct {
		reason   string
		spec     *v1alpha2.AsyncDisposableRequestParameters
		res      httpClient.HttpResponse
		expected bool
		wantErr  bool
	}{
		"NoExpectedResponseDefinition": {
			reason:   "Should return true when no expected response is defined",
			spec:     &v1alpha2.AsyncDisposableRequestParameters{URL: "https://api.example.com/test", Method: "GET"},
			res:      httpClient.HttpResponse{StatusCode: 200, Body: `{"status":"success"}`},
			expected: true,
		},
		"ZeroStatusCode": {
			reason:   "Should return false when status code is zero",
			spec:     &v1alpha2.AsyncDisposableRequestParameters{ExpectedResponse: `.body.status == "success"`},
			res:      httpClient.HttpResponse{StatusCode: 0, Body: `{"status":"success"}`},
			expected: false,
		},
		"ValidJQFilterReturnsTrue": {
			reason:   "Should return true when JQ filter evaluates to true",
			spec:     &v1alpha2.AsyncDisposableRequestParameters{ExpectedResponse: `.body.status == "success"`},
			res:      httpClient.HttpResponse{StatusCode: 200, Body: `{"status":"success"}`},
			expected: true,
		},
		"ValidJQFilterReturnsFalse": {
			reason:   "Should return false when JQ filter evaluates to false",
			spec:     &v1alpha2.AsyncDisposableRequestParameters{ExpectedResponse: `.body.status == "success"`},
			res:      httpClient.HttpResponse{StatusCode: 200, Body: `{"status":"failed"}`},
			expected: false,
		},
		"ComplexJQFilter": {
			reason:   "Should handle complex JQ filter expressions",
			spec:     &v1alpha2.AsyncDisposableRequestParameters{ExpectedResponse: `.body.data.items | length > 0`},
			res:      httpClient.HttpResponse{StatusCode: 200, Body: `{"data":{"items":[1,2,3]}}`},
			expected: true,
		},
		"StatusCodeCheck": {
			reason:   "Should evaluate JQ filter with status code check",
			spec:     &v1alpha2.AsyncDisposableRequestParameters{ExpectedResponse: `.statusCode == 201 and .body.created == true`},
			res:      httpClient.HttpResponse{StatusCode: 201, Body: `{"created":true,"id":"123"}`},
			expected: true,
		},
		"HeadersCheck": {
			reason:   "Should evaluate JQ filter checking headers",
			spec:     &v1alpha2.AsyncDisposableRequestParameters{ExpectedResponse: `.headers."Content-Type"[0] == "application/json"`},
			res:      httpClient.HttpResponse{StatusCode: 200, Body: `{}`, Headers: map[string][]string{"Content-Type": {"application/json"}}},
			expected: true,
		},
		"EmptyBody": {
			reason:   "Should handle empty response body with JQ filter",
			spec:     &v1alpha2.AsyncDisposableRequestParameters{ExpectedResponse: `.statusCode == 204`},
			res:      httpClient.HttpResponse{StatusCode: 204, Body: ""},
			expected: true,
		},
		"InvalidJQFilter": {
			reason:  "Should return an error when the JQ filter does not return a boolean",
			spec:    &v1alpha2.AsyncDisposableRequestParameters{ExpectedResponse: `.body.status`},
			res:     httpClient.HttpResponse{StatusCode: 200, Body: `{"status":"success"}`},
			wantErr: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := IsResponseAsExpected(tc.spec, tc.res)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("%s: expected error, got nil", tc.reason)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.reason, err)
			}
			if got != tc.expected {
				t.Errorf("%s: wanted %v, got %v", tc.reason, tc.expected, got)
			}
		})
	}
}
