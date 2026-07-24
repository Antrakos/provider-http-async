/*
Copyright 2024 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package polling implements the long-running-operation poll loop for AsyncRequest.
// The design is foreground-budget-bounded: each reconcile drives the loop up to a
// per-reconcile budget (defaultReconcileBudget) and returns if not yet done, letting
// Crossplane requeue. status.polling.response is the crash-recovery anchor — the raw
// mutate response, persisted before the first poll. A reconcile that finds it set
// resumes polling (recomputing polling.url against it) instead of re-firing the mutate
// call. See docs/prd-polling-response-anchor.md.
package polling

import (
	"fmt"
	"net/url"
	"time"

	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	"github.com/Antrakos/provider-http-async/apis/interfaces"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	"github.com/Antrakos/provider-http-async/internal/jq"
	json_util "github.com/Antrakos/provider-http-async/internal/json"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/Antrakos/provider-http-async/internal/service/request/requestgen"
)

// defaultReconcileBudget caps how long a single reconcile iteration will spend
// in the foreground poll loop. Once elapsed the loop returns Done=false and
// Crossplane requeues; the next reconcile resumes via the persisted polling.response.
const defaultReconcileBudget = 2 * time.Minute

// Result reports the outcome of a Poll call.
type Result struct {
	// Done is true when the operation reported completion within this call's budget.
	Done bool
	// OperationURL is the resolved polling.url (set even when Done=false).
	OperationURL string
	// PollResponse is the last .poll.response received (only set when Done=true).
	PollResponse map[string]interface{}
	// TerminalErr is non-empty when polling.error evaluated to a non-null value, the
	// resolved polling.url is unusable, or the overall polling.timeout elapsed; this is
	// a terminal condition, not a Go error. The anchor (polling.response) is retained so
	// a corrected spec resumes the existing operation instead of re-creating it.
	TerminalErr string
}

// Poller runs the poll loop for a long-running operation.
type Poller interface {
	// Poll recomputes polling.url from mutateResponse, validates it, and drives the
	// operation to completion (or until the per-reconcile budget or timeout is reached).
	// The anchor (status.polling.response) is persisted by the caller before Poll runs,
	// so a crash mid-poll resumes against the same operation instead of re-firing the
	// mutate call; Poll itself is stateless across reconciles.
	Poll(
		svcCtx *service.ServiceContext,
		crCtx *service.RequestCRContext,
		mapping interfaces.HTTPMapping,
		mutateResponse map[string]interface{},
	) (Result, error)
}

// foregroundPoller implements Poller with a budget-bounded foreground loop.
type foregroundPoller struct{}

// New returns the default Poller implementation.
func New() Poller { return &foregroundPoller{} }

func (fp *foregroundPoller) Poll(
	svcCtx *service.ServiceContext,
	crCtx *service.RequestCRContext,
	mapping interfaces.HTTPMapping,
	mutateResponse map[string]interface{},
) (Result, error) {
	polling := mapping.GetPolling()
	if polling == nil {
		// No polling block: the caller should not have persisted an anchor for this
		// mapping. Treat the operation as already complete so the caller extracts the
		// externalRef from the mutate response and clears the anchor (if any).
		return Result{Done: true}, nil
	}

	spec := crCtx.Spec()
	statusReader := crCtx.Status()

	// Recompute the operation URL from the mutate response on every reconcile. jq is
	// pure, so the same persisted response yields the same URL within a loop — but a
	// corrected polling.url now takes effect on the next reconcile instead of being
	// frozen on first entry.
	ctx := requestgen.GenerateRequestContextFromMap(spec, statusReader, mutateResponse, nil)
	operationURL, err := jq.ParseString(polling.GetURL(), ctx)
	if err != nil {
		return Result{}, errors.Wrap(err, "failed to evaluate polling.url")
	}
	if operationURL == "" {
		// The mutate call already succeeded and the anchor is persisted, so an empty
		// polling.url is a terminal *config* error — not a transient one. The usual cause
		// is a `polling` block attached to an operation that completes synchronously (its
		// response carries no long-running-operation identifier). Retaining the anchor
		// means removing the `polling` block (a spec change) clears the terminal state and
		// reconciles synchronously instead of re-firing the mutate call.
		return Result{TerminalErr: fmt.Sprintf(
			"polling.url %q resolved to an empty value. This mapping's operation appears "+
				"to be synchronous (no long-running-operation identifier in the response). "+
				"Remove the `polling` block from this mapping if the operation completes "+
				"synchronously.",
			polling.GetURL(),
		)}, nil
	}
	// The mutate call already succeeded (a resource was provisioned) by the time we get
	// here, so a malformed polling.url is a terminal *config* error, not a transient one:
	// retrying re-fires the poll GET forever against an unpollable URL and never recovers.
	// Many GCP LRO APIs return `name` as a bare resource path (e.g.
	// "projects/.../operations/789") — writing `polling.url: .response.body.name` yields a
	// scheme-less string that http.Get rejects with "unsupported protocol scheme". Surface
	// this as a terminal failure with an actionable message so the operator fixes the
	// expression (prepending the base URL) instead of the provider silently retrying. The
	// anchor is retained so the fix resumes the existing operation. Recoverable on spec
	// change.
	if reason := validateOperationURL(operationURL); reason != "" {
		return Result{TerminalErr: fmt.Sprintf(
			"polling.url %q resolved to %q, which is not a valid absolute URL: %s. "+
				"Many GCP LRO APIs return a bare resource path in .response.body.name; "+
				"prepend the base URL, e.g. '\"https://<host>/<version>/\" + .response.body.name'.",
			polling.GetURL(), operationURL, reason,
		)}, nil
	}

	timeout := polling.GetTimeout()
	interval := polling.GetInterval()
	startedAt := crCtx.Status().GetOperationStartedAt()
	if startedAt == nil {
		// StartedAt is nil on a fresh entry (the caller sets it with the anchor) or after a
		// terminal-clear reset it to give the resumed operation a fresh timeout budget. Use
		// now so the deadline is still bounded.
		now := metav1.Now()
		startedAt = &now
	}
	deadline := startedAt.Add(timeout)
	budgetEnd := time.Now().Add(defaultReconcileBudget)

	for {
		// Overall timeout: a terminal failure (not a requeuing error) so a
		// never-completing operation becomes visible and stalled rather than hot-looping
		// requeues. The anchor is retained, so raising polling.timeout (a spec change)
		// clears the terminal state, resets the deadline, and resumes.
		if time.Now().After(deadline) {
			return Result{OperationURL: operationURL, TerminalErr: fmt.Sprintf(
				"polling timeout after %s for operation %s", timeout, operationURL,
			)}, nil
		}

		// Per-reconcile budget: return without error so Crossplane requeues.
		if time.Now().After(budgetEnd) {
			return Result{Done: false, OperationURL: operationURL}, nil
		}

		// Fire the poll GET.
		pollResp, err := svcCtx.HTTP.SendRequest(
			svcCtx.Ctx, "GET", operationURL,
			httpClient.Data{Encrypted: "", Decrypted: ""},
			httpClient.Data{Encrypted: map[string][]string{}, Decrypted: map[string][]string{}},
			svcCtx.TLSConfigData,
		)
		if err != nil {
			// Transport error: transient — return a Go error so the controller requeues
			// with backoff. The anchor is retained; the next reconcile resumes.
			return Result{}, errors.Wrap(err, "polling GET failed")
		}

		pollResponseMap := ResponseToMap(&pollResp.HttpResponse)

		// Build jq context with both .response (mutate, stable) and .poll.response.
		jqCtx := buildPollJQCtx(spec, statusReader, mutateResponse, pollResponseMap)

		done, err := jq.ParseBool(polling.GetDone(), jqCtx)
		if err != nil {
			return Result{}, errors.Wrap(err, "failed to evaluate polling.done")
		}

		if done {
			if errExpr := polling.GetError(); errExpr != "" {
				exists, checkErr := jq.Exists(errExpr, jqCtx)
				if checkErr != nil {
					return Result{}, errors.Wrap(checkErr, "failed to evaluate polling.error")
				}
				if exists {
					terminalMsg, _ := jq.ParseString(errExpr, jqCtx)
					if terminalMsg == "" {
						// non-string non-null error value: stringify it
						terminalMsg = errExpr + " returned non-null"
					}
					return Result{OperationURL: operationURL, TerminalErr: terminalMsg}, nil
				}
			}
			return Result{Done: true, OperationURL: operationURL, PollResponse: pollResponseMap}, nil
		}

		// Not done — wait interval (context-aware). Use an explicit timer so it is
		// stopped promptly if the context fires first, avoiding timer accumulation
		// across a long, short-interval poll loop.
		timer := time.NewTimer(interval)
		select {
		case <-svcCtx.Ctx.Done():
			timer.Stop()
			return Result{}, svcCtx.Ctx.Err()
		case <-timer.C:
		}
	}
}

// validateOperationURL returns an empty string when raw is a usable absolute HTTP(S)
// URL, or a short human-readable reason why it is not. It exists to catch the common
// misconfiguration where polling.url resolves to a scheme-less path (the failure mode
// http.Get reports opaquely as `unsupported protocol scheme ""`).
func validateOperationURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return err.Error()
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		if u.Scheme == "" {
			return "missing scheme (expected http:// or https://)"
		}
		return fmt.Sprintf("unsupported scheme %q (expected http or https)", u.Scheme)
	}
	if u.Host == "" {
		return "missing host"
	}
	return ""
}

// buildPollJQCtx assembles the full jq context for polling.done / polling.error evaluation.
// .response is the stable mutate response (carried as a raw map across iterations) and
// .poll.response is the current poll GET response. GenerateRequestContextFromMap injects
// both without round-tripping through a wrapper type, so .response.body survives intact.
func buildPollJQCtx(
	spec interfaces.MappedHTTPRequestSpec,
	status interfaces.RequestStatusReader,
	mutateResponse map[string]interface{},
	pollResponse map[string]interface{},
) map[string]interface{} {
	return requestgen.GenerateRequestContextFromMap(spec, status, mutateResponse, pollResponse)
}

// ResponseToMap converts an HttpResponse to the raw map used as .response in jq context.
func ResponseToMap(resp *httpClient.HttpResponse) map[string]interface{} {
	m, _ := json_util.StructToMap(resp)
	json_util.ConvertJSONStringsToMaps(&m)
	return m
}

// SetTerminalFailure marks the resource as terminally failed due to a poll or config error.
// It persists the message and records the current generation so a later spec change clears
// the terminal state and retries. It deliberately PRESERVES status.polling.response (and
// StartedAt): the operation is already in flight, and a corrected polling.url must resume it
// rather than re-create it — clearing the anchor here would reintroduce the duplicate. Only
// clearTerminalError (on generation drift) and the completion path touch the anchor. It
// deliberately does not increment the failure counter: a terminal failure is a stable state,
// not a transient retry. RetryOnConflict handles optimistic-lock conflicts.
func SetTerminalFailure(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, terminalErr string) error {
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before setting terminal failure")
		}
		sw := crCtx.StatusWriter()
		sw.SetTerminalError(terminalErr)
		sw.SetObservedGeneration(crCtx.Status().GetGeneration())
		return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, resource)
	})
}
