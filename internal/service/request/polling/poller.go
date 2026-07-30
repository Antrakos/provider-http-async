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
	"encoding/json"
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
	// TerminalErr is non-empty when the poll loop reports a terminal condition. It carries
	// the provider-authored message for a config/timeout terminal. The anchor policy for a
	// terminal is cause-dependent (see ClearAnchor) and applied by the caller.
	TerminalErr string
	// FailingPollResponse is the full poll HTTP response captured when the poll loop reports
	// the operation ended in failure (polling.done + polling.error). It is surfaced to
	// status.response by the caller (the doc's "failing response → status.response" model),
	// where a user's isTerminalError expression can key off its statusCode and structured body.
	// Set only on the operation-failure branch; nil for config/timeout terminals (which carry
	// no HTTP response to surface).
	FailingPollResponse *httpClient.HttpResponse
	// OperationErrorMessage is the polling.error value projected to a string, set on the
	// operation-failure branch. The caller uses it as the default terminal message when
	// spec.isTerminalError resolves to a bare `true` (the user said "this is terminal" but
	// gave no message of their own), and otherwise surfaces it via the failing response body.
	OperationErrorMessage string
	// ClearAnchor is true when the terminal cause is an operation failure (polling.error
	// non-null): the operation is dead, so the anchor must be cleared so a retry re-fires a
	// fresh, duplicate-safe mutate instead of re-polling a dead operation. False (retain) for
	// a polling.timeout / bad-polling.url, where the operation state is unknown and the anchor
	// is the duplicate guard that lets a corrected spec resume the existing operation.
	ClearAnchor bool
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
					// The operation finished in failure. A failed LRO commonly returns 200 with an
					// error block in the body, so failure must be detected by an explicit expression
					// (polling.error), not by an HTTP status heuristic. The poll has positively
					// confirmed the operation finished without creating the resource, so a retry is
					// duplicate-safe (edge case #1): the caller clears the anchor so a retry re-fires
					// a fresh mutate rather than re-polling this dead operation. The full poll response
					// is carried (FailingPollResponse) so the caller surfaces it to status.response and
					// evaluates spec.isTerminalError against it to decide retry-vs-stall. The polling.error
					// value is projected to a string for the human message the caller uses as the default
					// terminal message when isTerminalError resolves to a bare `true`.
					errVal, valErr := jq.ParseAny(errExpr, jqCtx)
					if valErr != nil {
						return Result{}, errors.Wrap(valErr, "failed to evaluate polling.error")
					}
					return Result{
						OperationURL:          operationURL,
						ClearAnchor:           true,
						FailingPollResponse:   &pollResp.HttpResponse,
						OperationErrorMessage: terminalMessageFromValue(errVal, errExpr),
					}, nil
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

// terminalMessageFromValue projects the raw polling.error value onto the single string
// the operation-failure default message carries (Result.OperationErrorMessage). A string is
// surfaced verbatim; a structured value (object/array) is JSON-marshaled so the operator still
// sees its shape; any scalar (number/boolean) is fmt.Sprint'd. The full poll response is
// carried separately in Result.FailingPollResponse and surfaced to status.response by the
// caller, so this is a projection only — nothing is lost.
func terminalMessageFromValue(v interface{}, errExpr string) string {
	switch t := v.(type) {
	case string:
		return t
	case map[string]interface{}, []interface{}:
		raw, err := json.Marshal(t)
		if err != nil {
			// An in-memory map/array that fails to marshal is a programming error; fall back
			// to the opaque marker rather than losing the terminal entirely.
			return errExpr + " returned a non-serializable value"
		}
		return string(raw)
	default:
		return fmt.Sprint(t)
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

// SetTerminalFailure marks the resource as terminally failed, persisting the message and
// recording the current generation so a later spec change clears the terminal state and
// re-evaluates. It applies the cause-dependent anchor policy from the design doc:
//
//   - clearAnchor=true (operation failure, polling.error non-null): the operation is dead,
//     so the anchor is cleared. A spec change then re-fires a fresh mutate (duplicate-safe,
//     per edge case #1 — the poll confirmed the operation finished without creating). Retaining
//     the anchor here would re-poll a dead operation forever. StartedAt is cleared with it.
//   - clearAnchor=false (polling.timeout / bad-polling.url): the operation state is unknown, so
//     the anchor is PRESERVED. A corrected spec resumes the existing operation rather than
//     re-firing the mutate (the duplicate guard — re-firing could mint a duplicate). StartedAt
//     is retained so the resumed operation keeps its timeout budget.
//
// The full failing response is surfaced to status.response by the caller, not here. It does
// not increment the failure counter: a terminal failure is a stable state, not a transient
// retry. RetryOnConflict handles optimistic-lock conflicts.
func SetTerminalFailure(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, terminalErr string, clearAnchor bool) error {
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before setting terminal failure")
		}
		sw := crCtx.StatusWriter()
		sw.SetTerminalError(terminalErr)
		if clearAnchor {
			// Operation failure: the operation is dead. Drop the anchor + StartedAt so a
			// spec change re-fires a fresh, duplicate-safe mutate instead of re-polling it.
			sw.SetPollingResponse(nil)
			sw.SetOperationStartedAt(nil)
		}
		sw.SetObservedGeneration(crCtx.Status().GetGeneration())
		return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, resource)
	})
}
