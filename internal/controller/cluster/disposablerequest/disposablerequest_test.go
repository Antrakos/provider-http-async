/*
Copyright 2023 The Crossplane Authors.

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

package disposablerequest

import (
	"context"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/test"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Antrakos/provider-http-async/apis/cluster/disposablerequest/v1alpha2"
)

func terminalCR() *v1alpha2.AsyncDisposableRequest {
	limit := int32(1)
	return &v1alpha2.AsyncDisposableRequest{
		ObjectMeta: v1.ObjectMeta{Name: "term"},
		Spec: v1alpha2.AsyncDisposableRequestSpec{
			ForProvider: v1alpha2.AsyncDisposableRequestParameters{
				URL:                  "https://api.example.com/x",
				Method:               "POST",
				RollbackRetriesLimit: &limit,
			},
		},
		Status: v1alpha2.AsyncDisposableRequestStatus{Failed: 1, Error: "operation failed"},
	}
}

// TestObserve_TerminalFailureMarksUnavailable verifies a retries-exhausted one-off is
// reported as existing+up-to-date (stopping no-op Create churn) with Ready=Unavailable.
func TestObserve_TerminalFailureMarksUnavailable(t *testing.T) {
	cr := terminalCR()
	kube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}
	e := &external{localKube: kube, logger: logging.NewNopLogger()}

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.ResourceExists || !obs.ResourceUpToDate {
		t.Errorf("expected ResourceExists && ResourceUpToDate to stop Create churn, got %+v", obs)
	}
	cond := cr.GetCondition(xpv2.TypeReady)
	if cond.Status != corev1.ConditionFalse || cond.Reason != xpv2.ReasonUnavailable {
		t.Errorf("expected Ready=False/Unavailable, got status=%q reason=%q", cond.Status, cond.Reason)
	}
	if cond.Message != "operation failed" {
		t.Errorf("expected the error surfaced in the Ready message, got %q", cond.Message)
	}
}

// TestClearErrorObserved verifies a stale ErrorObserved condition is dropped on recovery.
func TestClearErrorObserved(t *testing.T) {
	cr := terminalCR()
	cr.Status.Conditions = []xpv2.Condition{
		{Type: xpv2.TypeReady, Status: corev1.ConditionTrue},
		{Type: "ErrorObserved", Status: corev1.ConditionTrue, Message: "operation failed"},
	}
	r := &errorConditionReconciler{kube: &test.MockClient{MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil)}}

	if err := r.clearErrorObserved(context.Background(), cr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, c := range cr.Status.Conditions {
		if c.Type == "ErrorObserved" {
			t.Fatalf("expected ErrorObserved condition to be removed, still present")
		}
	}
	if cr.GetCondition(xpv2.TypeReady).Status != corev1.ConditionTrue {
		t.Errorf("clearErrorObserved should not disturb other conditions")
	}
}
