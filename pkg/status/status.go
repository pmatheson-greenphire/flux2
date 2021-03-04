/*
Copyright 2020, 2021 The Flux authors

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

package status

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling"
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling/aggregator"
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling/collector"
	"sigs.k8s.io/cli-utils/pkg/kstatus/polling/event"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/cli-utils/pkg/object"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"github.com/fluxcd/flux2/pkg/log"
)

type StatusChecker struct {
	pollInterval time.Duration
	timeout      time.Duration
	client       client.Client
	statusPoller *polling.StatusPoller
	logger       log.Logger
}

func NewStatusChecker(kubeConfig *rest.Config, pollInterval time.Duration, timeout time.Duration, log log.Logger) (*StatusChecker, error) {
	restMapper, err := apiutil.NewDynamicRESTMapper(kubeConfig)
	if err != nil {
		return nil, err
	}
	c, err := client.New(kubeConfig, client.Options{Mapper: restMapper})
	if err != nil {
		return nil, err
	}

	return &StatusChecker{
		pollInterval: pollInterval,
		timeout:      timeout,
		client:       c,
		statusPoller: polling.NewStatusPoller(c, restMapper),
		logger:       log,
	}, nil
}

func (sc *StatusChecker) Assess(identifiers ...object.ObjMetadata) error {
	ctx, cancel := context.WithTimeout(context.Background(), sc.timeout)
	defer cancel()

	opts := polling.Options{PollInterval: sc.pollInterval, UseCache: true}
	eventsChan := sc.statusPoller.Poll(ctx, identifiers, opts)

	coll := collector.NewResourceStatusCollector(identifiers)
	done := coll.ListenWithObserver(eventsChan, collector.ObserverFunc(
		func(statusCollector *collector.ResourceStatusCollector, e event.Event) {
			var rss []*event.ResourceStatus
			for _, rs := range statusCollector.ResourceStatuses {
				rss = append(rss, rs)
			}
			desired := status.CurrentStatus
			aggStatus := aggregator.AggregateStatus(rss, desired)
			if aggStatus == desired {
				cancel()
				return
			}
		}),
	)
	<-done

	if coll.Error != nil || ctx.Err() == context.DeadlineExceeded {
		for _, rs := range coll.ResourceStatuses {
			if rs.Status != status.CurrentStatus {
				if rs.Resource == nil {
					sc.logger.Failuref("%s: %s not found", rs.Identifier.Name, rs.Identifier.GroupKind.Kind)
				} else {
					sc.logger.Failuref("%s: unhealthy (timed out waiting for rollout)", rs.Identifier.Name)
				}
			}
		}
		return fmt.Errorf("timed out waiting for condition")
	}

	return nil
}
