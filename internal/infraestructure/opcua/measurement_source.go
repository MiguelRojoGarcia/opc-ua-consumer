package opcua

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	gopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"

	"opc-ua-consumer/internal/application"
	"opc-ua-consumer/internal/domain/measurement"
	"opc-ua-consumer/internal/domain/tag"
	"opc-ua-consumer/internal/shared/logger"
)

var _ application.MeasurementSource = (*MeasurementSource)(nil)

// SourceConfig configures how the tags are read from the server.
type SourceConfig struct {
	Site               string        // código del centro que se pone en cada medida
	Namespace          string        // URI del namespace de los tags, p. ej. KEPServerEX
	SamplingInterval   time.Duration // cada cuánto muestrea el servidor cada tag
	PublishingInterval time.Duration // cada cuánto envía el servidor los cambios acumulados
}

// MeasurementSource is the OPC-UA adapter of application.MeasurementSource: it
// subscribes to the tags on a connected client and turns each data change into
// a domain measurement.
type MeasurementSource struct {
	client *gopcua.Client
	site   measurement.SiteCode
	cfg    SourceConfig
	log    logger.Logger
}

// NewMeasurementSource builds the adapter on a client obtained from Connector.
// The caller keeps owning the client and must Close it.
func NewMeasurementSource(client *gopcua.Client, cfg SourceConfig, log logger.Logger) (*MeasurementSource, error) {
	if client == nil {
		return nil, errors.New("opcua: client is required")
	}
	site, err := measurement.NewSiteCode(cfg.Site)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.Namespace) == "" {
		return nil, errors.New("opcua: namespace is required")
	}
	return &MeasurementSource{client: client, site: site, cfg: cfg, log: log}, nil
}

// Subscribe creates a subscription with one monitored item per tag and passes
// every change to handler until ctx is cancelled. When each monitored item is
// created the server sends its current value, which acts as the starting point.
// If the connection drops, gopcua reconnects and restores the subscription.
func (s *MeasurementSource) Subscribe(ctx context.Context, tags []tag.Tag, handler application.MeasurementHandler) error {
	refs := make([]tagRef, len(tags))
	for i, t := range tags {
		ref, err := parseTag(string(t))
		if err != nil {
			return err
		}
		refs[i] = ref
	}

	ns, err := s.client.FindNamespace(ctx, s.cfg.Namespace)
	if err != nil {
		return fmt.Errorf("finding namespace %q: %w", s.cfg.Namespace, err)
	}

	notifyCh := make(chan *gopcua.PublishNotificationData, 256)
	sub, err := s.client.Subscribe(ctx, &gopcua.SubscriptionParameters{Interval: s.cfg.PublishingInterval}, notifyCh)
	if err != nil {
		return fmt.Errorf("creating subscription: %w", err)
	}
	defer sub.Cancel(context.Background())

	// El client handle es el índice del tag, para traducir las notificaciones a su tagRef.
	items := make([]*ua.MonitoredItemCreateRequest, len(refs))
	for i, ref := range refs {
		items[i] = gopcua.NewMonitoredItemCreateRequestWithDefaults(ua.NewStringNodeID(ns, ref.tag), ua.AttributeIDValue, uint32(i))
		items[i].RequestedParameters.SamplingInterval = float64(s.cfg.SamplingInterval.Milliseconds())
	}
	resp, err := sub.Monitor(ctx, ua.TimestampsToReturnBoth, items...)
	if err != nil {
		return fmt.Errorf("monitoring tags: %w", err)
	}
	monitored := 0
	for i, r := range resp.Results {
		if r.StatusCode != ua.StatusOK {
			s.log.Warn(ctx, "tag not monitored", "tag", refs[i].tag, "status", statusName(r.StatusCode))
			continue
		}
		monitored++
	}
	if monitored == 0 {
		return errors.New("no tag could be monitored")
	}
	s.log.Info(ctx, "consuming changes", "subscription_id", sub.SubscriptionID, "monitored", monitored, "total", len(refs))

	for {
		select {
		case <-ctx.Done():
			return nil
		case n := <-notifyCh:
			s.handleNotification(ctx, refs, n, handler)
		}
	}
}

func (s *MeasurementSource) handleNotification(
	ctx context.Context,
	refs []tagRef,
	n *gopcua.PublishNotificationData,
	handler application.MeasurementHandler,
) {
	if n.Error != nil {
		s.log.Warn(ctx, "subscription error", "error", n.Error)
		return
	}
	switch v := n.Value.(type) {
	case *ua.DataChangeNotification:
		for _, item := range v.MonitoredItems {
			if int(item.ClientHandle) >= len(refs) {
				continue
			}
			ref := refs[item.ClientHandle]
			m, err := toMeasurement(s.site, ref, item.Value)
			if err != nil {
				// p. ej. un Bad sin valor o un tipo no soportado: se descarta la lectura.
				var status ua.StatusCode
				if item.Value != nil {
					status = item.Value.Status
				}
				s.log.Warn(ctx, "reading discarded", "tag", ref.tag, "status", statusName(status), "error", err)
				continue
			}
			if err := handler.Handle(ctx, m); err != nil {
				// La suscripción sigue: un fallo puntual al procesar no debe parar el consumo.
				s.log.Error(ctx, "error handling measurement", "tag", ref.tag, "error", err)
			}
		}
	case *ua.StatusChangeNotification:
		s.log.Warn(ctx, "subscription status changed", "status", statusName(v.Status))
	}
}
