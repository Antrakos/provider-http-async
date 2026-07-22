/*
Copyright 2020 The Crossplane Authors.

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

package namespaced

import (
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	ctrl "sigs.k8s.io/controller-runtime"

	namespacedrequestv1alpha2 "github.com/Antrakos/provider-http-async/apis/namespaced/request/v1alpha2"
	namespacedv1alpha2 "github.com/Antrakos/provider-http-async/apis/namespaced/v1alpha2"
	"github.com/Antrakos/provider-http-async/internal/controller/namespaced/config"
	"github.com/Antrakos/provider-http-async/internal/controller/namespaced/request"
)

// Setup creates all namespaced http controllers with the supplied logger and adds them to
// the supplied manager.
func Setup(mgr ctrl.Manager, o controller.Options, timeout time.Duration) error {
	for _, setup := range []func(ctrl.Manager, controller.Options, time.Duration) error{
		config.Setup,
		config.SetupCluster,
		request.Setup,
	} {
		if err := setup(mgr, o, timeout); err != nil {
			return err
		}
	}
	return nil
}

// SetupGated creates all namespaced http controllers with SafeStart capability (controllers start as their CRDs appear)
func SetupGated(mgr ctrl.Manager, o controller.Options, timeout time.Duration) error {
	o.Gate.Register(func() {
		if err := config.Setup(mgr, o, timeout); err != nil {
			panic(err)
		}
	}, namespacedv1alpha2.ProviderConfigGroupVersionKind)

	o.Gate.Register(func() {
		if err := config.SetupCluster(mgr, o, timeout); err != nil {
			panic(err)
		}
	}, namespacedv1alpha2.ClusterProviderConfigGroupVersionKind)

	o.Gate.Register(func() {
		if err := request.Setup(mgr, o, timeout); err != nil {
			panic(err)
		}
	}, namespacedrequestv1alpha2.RequestGroupVersionKind)

	return nil
}
