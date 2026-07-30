package v1alpha2

import (
	"encoding/json"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	d.Status.TerminalError = msg
	d.Status.Error = msg
}

func (d *AsyncRequest) SetObservedGeneration(generation int64) {
	d.Status.SetObservedGeneration(generation)
}

func (d *AsyncRequest) SetOperationStartedAt(t *metav1.Time) {
	d.Status.Polling.StartedAt = t
}
