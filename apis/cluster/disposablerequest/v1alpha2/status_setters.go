package v1alpha2

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (d *AsyncDisposableRequest) SetStatusCode(statusCode int) {
	d.Status.Response.StatusCode = statusCode
}

func (d *AsyncDisposableRequest) SetHeaders(headers map[string][]string) {
	d.Status.Response.Headers = headers
}

func (d *AsyncDisposableRequest) SetBody(body string) {
	d.Status.Response.Body = body
}

func (d *AsyncDisposableRequest) SetSynced(synced bool) {
	d.Status.Synced = synced
	d.Status.Failed = 0
	d.Status.Error = ""
}

func (d *AsyncDisposableRequest) SetLastReconcileTime() {
	d.Status.LastReconcileTime = metav1.NewTime(time.Now())
}

func (d *AsyncDisposableRequest) SetError(err error) {
	d.Status.Failed++
	d.Status.Synced = false
	if err != nil {
		d.Status.Error = err.Error()
	}
}

func (d *AsyncDisposableRequest) SetRequestDetails(url, method, body string, headers map[string][]string) {
	d.Status.RequestDetails.Body = body
	d.Status.RequestDetails.URL = url
	d.Status.RequestDetails.Headers = headers
	d.Status.RequestDetails.Method = method
}
