package config

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"

	"github.com/Antrakos/provider-http-async/apis/namespaced/v1alpha2"
)

// TestEnqueueRequestForProviderConfig_KindFilter locks in the fix for
// bug-spurious-namespaced-providerconfig-lookup-when-clusterscoped.md: the namespaced
// ProviderConfig controller must only enqueue reconciles for usages that reference a
// ProviderConfig, not a ClusterProviderConfig. Without the Kind filter on the watch
// handler, a ClusterProviderConfig usage enqueued a reconcile into the namespaced
// controller, which then failed to find a namespaced ProviderConfig (correctly absent)
// and logged "cannot get ProviderConfig" every cycle.
func TestEnqueueRequestForProviderConfig_KindFilter(t *testing.T) {
	usage := func(name, refKind string) *v1alpha2.ProviderConfigUsage {
		return &v1alpha2.ProviderConfigUsage{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "common"},
			TypedProviderConfigUsage: xpv2.TypedProviderConfigUsage{
				ProviderConfigReference: xpv2.ProviderConfigReference{
					Kind: refKind, Name: "gcp",
				},
			},
		}
	}

	cases := []struct {
		name    string
		handler *resource.EnqueueRequestForProviderConfig
		usage   *v1alpha2.ProviderConfigUsage
		wantLen int
		wantNN  types.NamespacedName
	}{
		{
			name:    "NamespacedController_ProviderConfigUsage_Enqueued",
			handler: &resource.EnqueueRequestForProviderConfig{Kind: "ProviderConfig"},
			usage:   usage("pcu-1", "ProviderConfig"),
			wantLen: 1,
			wantNN:  types.NamespacedName{Name: "gcp", Namespace: "common"},
		},
		{
			name:    "NamespacedController_ClusterProviderConfigUsage_NotEnqueued",
			handler: &resource.EnqueueRequestForProviderConfig{Kind: "ProviderConfig"},
			usage:   usage("pcu-2", "ClusterProviderConfig"),
			wantLen: 0,
		},
		{
			name:    "ClusterController_ProviderConfigUsage_NotEnqueued",
			handler: &resource.EnqueueRequestForProviderConfig{Kind: "ClusterProviderConfig"},
			usage:   usage("pcu-3", "ProviderConfig"),
			wantLen: 0,
		},
		{
			name:    "ClusterController_ClusterProviderConfigUsage_Enqueued",
			handler: &resource.EnqueueRequestForProviderConfig{Kind: "ClusterProviderConfig"},
			usage:   usage("pcu-4", "ClusterProviderConfig"),
			wantLen: 1,
			// Cluster-scoped reference: enqueued with Name only (no namespace), matching the
			// upstream handler's strings.HasPrefix(refKind, "Cluster") branch.
			wantNN: types.NamespacedName{Name: "gcp"},
		},
		{
			name:    "EmptyKind_BackwardCompat_EnqueuesAll",
			handler: &resource.EnqueueRequestForProviderConfig{}, // pre-fix behavior: no filter
			usage:   usage("pcu-5", "ClusterProviderConfig"),
			wantLen: 1,
			// Cluster-scoped reference kind → enqueued with Name only (no namespace).
			wantNN: types.NamespacedName{Name: "gcp"},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			q := workqueue.NewTypedRateLimitingQueue[reconcile.Request](
				workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			tc.handler.Create(context.Background(), event.CreateEvent{Object: tc.usage}, q)
			q.ShutDown()

			got := drain(q)
			if len(got) != tc.wantLen {
				t.Fatalf("enqueued %d requests, want %d: %+v", len(got), tc.wantLen, got)
			}
			if tc.wantLen == 1 {
				if got[0].NamespacedName != tc.wantNN {
					t.Errorf("request NamespacedName: want %+v, got %+v", tc.wantNN, got[0].NamespacedName)
				}
			}
		})
	}
}

func drain(q workqueue.TypedRateLimitingInterface[reconcile.Request]) []reconcile.Request {
	var out []reconcile.Request
	for {
		item, shutdown := q.Get()
		if shutdown {
			return out
		}
		out = append(out, item)
		q.Done(item)
	}
}
