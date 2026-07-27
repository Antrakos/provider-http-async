package v1alpha2

import (
	"encoding/json"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/Antrakos/provider-http-async/apis/interfaces"
)

func (d *AsyncRequest) SetStatusCode(statusCode int) {
	d.Status.Response.StatusCode = statusCode
}

func (d *AsyncRequest) SetHeaders(headers map[string][]string) {
	d.Status.Response.Headers = headers
}

func (d *AsyncRequest) SetBody(body string) {
	d.Status.Response.Body = body
}

func (d *AsyncRequest) SetError(err error) {
	d.Status.Failed++
	if err != nil {
		d.Status.Error = err.Error()
	}
}

func (d *AsyncRequest) ResetFailures() {
	d.Status.Failed = 0
	d.Status.Error = ""
}

func (d *AsyncRequest) SetRequestDetails(url, method, body string, headers map[string][]string) {
	d.Status.RequestDetails.Body = body
	d.Status.RequestDetails.URL = url
	d.Status.RequestDetails.Headers = headers
	d.Status.RequestDetails.Method = method
}

func (d *AsyncRequest) SetCache(statusCode int, headers map[string][]string, body string) {
	d.Status.Cache.Response.StatusCode = statusCode
	d.Status.Cache.Response.Headers = headers
	d.Status.Cache.Response.Body = body
	d.Status.Cache.LastUpdated = time.Now().UTC().Format(time.RFC3339)
}

func (d *AsyncRequest) SetExternalRef(ref string) {
	d.Status.ExternalRef = ref
}

// SetPollingResponse persists (or clears, when m is nil) the raw mutate response that
// anchors an in-flight long-running operation. The map is serialized to a
// runtime.RawExtension so the poll jq context can be rebuilt on resume by deserializing
// it back to a map via GetPollingResponse.
func (d *AsyncRequest) SetPollingResponse(m map[string]interface{}) {
	if m == nil {
		d.Status.Polling.Response = nil
		return
	}
	raw, err := json.Marshal(m)
	if err != nil {
		// A marshal failure of an in-memory map is a programming error; leave the
		// anchor unset rather than persisting a corrupt blob so the next reconcile
		// re-fires the mutate call instead of polling a garbage URL.
		d.Status.Polling.Response = nil
		return
	}
	d.Status.Polling.Response = &runtime.RawExtension{Raw: raw}
}

func (d *AsyncRequest) SetTerminalError(msg string) {
	d.Status.Polling.TerminalError = msg
	d.Status.Error = msg
}

// SetTerminalResponse persists (or clears, when resp is nil) the full poll HTTP response
// captured at the moment a poll terminated with an error. Unlike SetPollingResponse it
// stores a typed Response (statusCode/body/headers) rather than a RawExtension, so a
// classifier reads real fields — statusCode as an int, body as the raw JSON string to
// parse, headers as a typed map — instead of grovelling a map[string]interface{}.
func (d *AsyncRequest) SetTerminalResponse(resp interfaces.HTTPResponse) {
	if resp == nil {
		d.Status.Polling.TerminalResponse = nil
		return
	}
	d.Status.Polling.TerminalResponse = &Response{
		StatusCode: resp.GetStatusCode(),
		Body:       resp.GetBody(),
		Headers:    resp.GetHeaders(),
	}
}

func (d *AsyncRequest) SetObservedGeneration(generation int64) {
	d.Status.SetObservedGeneration(generation)
}

func (d *AsyncRequest) SetOperationStartedAt(t *metav1.Time) {
	d.Status.Polling.StartedAt = t
}
