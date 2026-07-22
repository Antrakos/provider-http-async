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
// Crossplane requeue. status.operationRef is the crash-recovery anchor — a reconcile
// that finds it non-empty re-attaches to the same operation URL instead of re-firing
// the mutate call. See PRD section "On 'a long poll blocks the reconciler'".
package polling

import (
	"fmt"
	"time"

	"github.com/Antrakos/provider-http-async/apis/interfaces"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	"github.com/Antrakos/provider-http-async/internal/jq"
	json_util "github.com/Antrakos/provider-http-async/internal/json"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/Antrakos/provider-http-async/internal/service/request/requestgen"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

// defaultReconcileBudget caps how long a single reconcile iteration will spend
// in the foreground poll loop. Once elapsed the loop returns Done=false and
// Crossplane requeues; the next reconcile resumes via operationRef.
const defaultReconcileBudget = 2 * time.Minute

// Result reports the outcome of a Poll call.
type Result struct {
	// Done is true when the operation reported completion within this call's budget.
	Done bool
	// OperationURL is the resolved polling.url (set even when Done=false).
	OperationURL string
	// PollResponse is the last .poll.response received (only set when Done=true).
	PollResponse map[string]interface{}
	// TerminalErr is non-empty when polling.error evaluated to a non-null value;
	// this is a terminal condition, not a Go error.
	TerminalErr string
}

// Poller runs the poll loop for a long-running operation.
type Poller interface {
	// Poll resolves polling.url, persists operationRef, and drives the operation to
	// completion (or until the per-reconcile budget or timeout is reached).
	// resumeURL is a non-empty status.operationRef to re-attach to instead of
	// re-evaluating polling.url (crash / requeue recovery).
	Poll(
		svcCtx *service.ServiceContext,
		crCtx *service.RequestCRContext,
		mapping interfaces.HTTPMapping,
		mutateResponse map[string]interface{},
		resumeURL string,
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
	resumeURL string,
) (Result, error) {
	polling := mapping.GetPolling()
	if polling == nil {
		return Result{Done: true}, nil
	}

	spec := crCtx.Spec()
	statusReader := crCtx.Status()

	// Resolve the operation URL.
	operationURL := resumeURL
	if operationURL == "" {
		ctx := requestgen.GenerateRequestContextFromMap(spec, statusReader, mutateResponse, nil)
		var err error
		operationURL, err = jq.ParseString(polling.GetURL(), ctx)
		if err != nil {
			return Result{}, errors.Wrap(err, "failed to evaluate polling.url")
		}
		if operationURL == "" {
			return Result{}, errors.Errorf("polling.url %q resolved to an empty operation URL", polling.GetURL())
		}
	}

	// PRD step a': persist operationRef BEFORE the first GET so a crash mid-poll
	// resumes against the same operation instead of re-firing the mutate call.
	if err := persistOperationRef(svcCtx, crCtx, operationURL); err != nil {
		return Result{}, err
	}

	timeout := polling.GetTimeout()
	interval := polling.GetInterval()
	startedAt := crCtx.Status().GetOperationStartedAt()
	if startedAt == nil {
		// persistOperationRef will set this; use now as a safe local fallback.
		now := metav1.Now()
		startedAt = &now
	}
	deadline := startedAt.Add(timeout)
	budgetEnd := time.Now().Add(defaultReconcileBudget)

	for {
		// Timeout check.
		if time.Now().After(deadline) {
			return Result{OperationURL: operationURL}, errors.New(fmt.Sprintf("polling timeout after %s for operation %s", timeout, operationURL))
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

// persistOperationRef writes operationRef (and, on first entry, operationStartedAt) to
// status and flushes to the API server. RetryOnConflict handles optimistic-lock conflicts.
func persistOperationRef(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, url string) error {
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before setting operationRef")
		}
		sw := crCtx.StatusWriter()
		sw.SetOperationRef(url)
		if crCtx.Status().GetOperationStartedAt() == nil {
			now := metav1.Now()
			sw.SetOperationStartedAt(&now)
		}
		return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, resource)
	})
}

// ClearOperationRef clears operationRef and operationStartedAt in status and flushes to
// the API server. RetryOnConflict handles optimistic-lock conflicts from concurrent reconciles.
func ClearOperationRef(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext) error {
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before clearing operationRef")
		}
		sw := crCtx.StatusWriter()
		sw.SetOperationRef("")
		sw.SetOperationStartedAt(nil)
		return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, resource)
	})
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

// MergeStatus injects the status subtree (externalRef, operationRef) into an existing jq context.
func MergeStatus(ctx map[string]interface{}, status interfaces.RequestStatusReader) {
	statusMap := map[string]interface{}{
		"externalRef":  status.GetExternalRefValue(),
		"operationRef": status.GetOperationRef(),
	}
	maps.Copy(ctx, map[string]interface{}{"status": statusMap})
}

// SetTerminalFailure marks the resource as terminally failed due to a poll or config error.
// It persists the message, clears operationRef so subsequent reconciles do not resume the
// (already-failed) poll loop, and records the current generation so a later spec change
// clears the terminal state and retries. It deliberately does not increment the failure
// counter: a terminal failure is a stable state, not a transient retry.
// RetryOnConflict handles optimistic-lock conflicts from concurrent reconciles.
func SetTerminalFailure(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, terminalErr string) error {
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before setting terminal failure")
		}
		sw := crCtx.StatusWriter()
		sw.SetOperationRef("")
		sw.SetOperationStartedAt(nil)
		sw.SetTerminalError(terminalErr)
		sw.SetObservedGeneration(crCtx.Status().GetGeneration())
		return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, resource)
	})
}

