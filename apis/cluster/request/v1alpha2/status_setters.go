package v1alpha2

import "time"

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
