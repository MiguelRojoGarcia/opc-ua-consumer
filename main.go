// opc-ua-consumer: CLI que consume métricas de un servidor OPC-UA (KEPServerEX).
//
// Funciona como un consumidor de larga duración (pensado para supervisord):
// lee la lista de tags de un fichero YAML al arrancar, conecta, se suscribe a
// sus cambios y procesa las notificaciones hasta recibir SIGTERM/SIGINT. Al
// crear cada monitored item el servidor envía su valor actual, que sirve de
// punto de partida. Si se pierde la conexión, gopcua reconecta y restaura la
// suscripción.
//
// El fichero de tags se lee solo al arrancar: para cambiarlo hay que reiniciar
// el proceso.
//
// Todos los flags se pueden configurar también por variables de entorno.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"

	"opc-ua-consumer/internal/application"
	"opc-ua-consumer/internal/infraestructure/kafka"
	"opc-ua-consumer/internal/infraestructure/opcua"
	tagrepo "opc-ua-consumer/internal/infraestructure/repository/tag"
	"opc-ua-consumer/internal/shared/logger"
)

// app guarda el logger para compartirlo entre main, el hook Before y las acciones.
type app struct {
	log logger.Logger
}

func main() {
	// supervisord y docker paran el proceso con SIGTERM; Ctrl+C envía SIGINT.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Logger por defecto para poder registrar errores aunque fallen los flags;
	// setupLogger lo sustituye por el configurado.
	a := &app{log: logger.NewSlog(logger.SlogConfig{Level: logger.LevelInfo})}

	if err := a.command().Run(ctx, os.Args); err != nil {
		a.log.Error(ctx, "fatal error", "error", err)
		os.Exit(1)
	}
}

func (a *app) command() *cli.Command {
	return &cli.Command{
		Name:  "opc-ua-consumer",
		Usage: "consume métricas industriales de un servidor OPC-UA",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "endpoint",
				Usage:   "endpoint del servidor OPC-UA",
				Value:   "opc.tcp://localhost:49320",
				Sources: cli.EnvVars("OPCUA_ENDPOINT"),
			},
			&cli.StringFlag{
				Name:    "log-level",
				Usage:   "nivel de log: debug, info, warn, error",
				Value:   "info",
				Sources: cli.EnvVars("LOG_LEVEL"),
			},
			&cli.StringFlag{
				Name:    "log-format",
				Usage:   "formato de log: text, json",
				Value:   "text",
				Sources: cli.EnvVars("LOG_FORMAT"),
			},
		},
		Before: a.setupLogger,
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "se suscribe a los tags del fichero YAML y consume sus cambios hasta recibir una señal de parada",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "center-id",
						Usage:    "identificador del centro; se usará como key al publicar las métricas, para que todas las de un centro vayan a la misma partición",
						Required: true,
						Sources:  cli.EnvVars("CENTER_ID"),
					},
					&cli.StringFlag{
						Name:    "tags-file",
						Usage:   "fichero YAML con los tags a consumir (se lee solo al arrancar)",
						Value:   "config/tags.yaml",
						Sources: cli.EnvVars("OPCUA_TAGS_FILE"),
					},
					&cli.StringFlag{
						Name:    "namespace",
						Usage:   "URI del namespace de los tags",
						Value:   "KEPServerEX",
						Sources: cli.EnvVars("OPCUA_NAMESPACE"),
					},
					&cli.DurationFlag{
						Name:    "sampling-interval",
						Usage:   "cada cuánto muestrea el servidor el valor de cada tag",
						Value:   500 * time.Millisecond,
						Sources: cli.EnvVars("OPCUA_SAMPLING_INTERVAL"),
					},
					&cli.DurationFlag{
						Name:    "publishing-interval",
						Usage:   "cada cuánto envía el servidor los cambios acumulados",
						Value:   time.Second,
						Sources: cli.EnvVars("OPCUA_PUBLISHING_INTERVAL"),
					},
					&cli.StringSliceFlag{
						Name:    "kafka-brokers",
						Usage:   "brokers de Kafka (host:port), separados por comas",
						Value:   []string{"localhost:9094"},
						Sources: cli.EnvVars("KAFKA_BROKERS"),
					},
					&cli.StringFlag{
						Name:    "kafka-topic",
						Usage:   "tópico donde se publican las medidas (key = center-id)",
						Value:   "measurements",
						Sources: cli.EnvVars("KAFKA_TOPIC"),
					},
					// Tuning del productor. Sin valor se usan los defaults del
					// paquete kafka, pensados para mucho volumen de métricas.
					&cli.DurationFlag{
						Name:    "kafka-linger",
						Usage:   "cuánto espera un batch a llenarse antes de enviarse (por defecto 50ms)",
						Sources: cli.EnvVars("KAFKA_LINGER"),
					},
					&cli.IntFlag{
						Name:    "kafka-batch-max-bytes",
						Usage:   "tamaño máximo de un batch por partición; ≤ max.message.bytes del tópico (por defecto 1000012)",
						Sources: cli.EnvVars("KAFKA_BATCH_MAX_BYTES"),
					},
					&cli.StringFlag{
						Name:    "kafka-compression",
						Usage:   "compresión de los batches: zstd, lz4, snappy, gzip, none (por defecto zstd)",
						Sources: cli.EnvVars("KAFKA_COMPRESSION"),
					},
					&cli.IntFlag{
						Name:    "kafka-max-buffered-records",
						Usage:   "mensajes máximos en memoria pendientes de confirmar (por defecto 500000)",
						Sources: cli.EnvVars("KAFKA_MAX_BUFFERED_RECORDS"),
					},
					&cli.IntFlag{
						Name:    "kafka-max-buffered-bytes",
						Usage:   "bytes máximos en memoria pendientes de confirmar (por defecto 268435456 = 256 MiB)",
						Sources: cli.EnvVars("KAFKA_MAX_BUFFERED_BYTES"),
					},
					&cli.DurationFlag{
						Name:    "kafka-delivery-timeout",
						Usage:   "tiempo máximo para entregar un mensaje, incluidos reintentos (por defecto 2m)",
						Sources: cli.EnvVars("KAFKA_DELIVERY_TIMEOUT"),
					},
				},
				Action: a.runAction,
			},
		},
	}
}

func (a *app) setupLogger(ctx context.Context, cmd *cli.Command) (context.Context, error) {
	level, err := logger.ParseLevel(cmd.String("log-level"))
	if err != nil {
		return ctx, err
	}
	format := logger.Format(cmd.String("log-format"))
	if format != logger.FormatText && format != logger.FormatJSON {
		return ctx, fmt.Errorf("invalid --log-format %q: use text or json", format)
	}
	a.log = logger.NewSlog(logger.SlogConfig{Level: level, Format: format})
	return ctx, nil
}

func (a *app) runAction(ctx context.Context, cmd *cli.Command) error {
	centerID := strings.TrimSpace(cmd.String("center-id"))
	if centerID == "" {
		return fmt.Errorf("--center-id must not be empty")
	}
	// Todas las trazas llevan el centro, para distinguir instancias en logs agregados.
	a.log = a.log.With("center_id", centerID)
	log := a.log

	// Los tags salen de un repositorio: hoy un fichero YAML, mañana una BBDD o
	// el propio servidor OPC-UA, sin cambiar el servicio.
	tagRepository, err := tagrepo.NewFileRepository(cmd.String("tags-file"))
	if err != nil {
		return err
	}
	tagsResolver, err := application.NewTagsResolver(tagRepository, log.With("tags_file", cmd.String("tags-file")))
	if err != nil {
		return err
	}
	tags, err := tagsResolver.Resolve(ctx)
	if err != nil {
		return err
	}

	// Kafka primero: no tiene sentido empezar a consumir si no hay dónde publicar.
	kafkaConnector, err := kafka.NewConnector(kafka.Config{
		Brokers:            cmd.StringSlice("kafka-brokers"),
		Linger:             cmd.Duration("kafka-linger"),
		BatchMaxBytes:      int32(cmd.Int("kafka-batch-max-bytes")),
		Compression:        cmd.String("kafka-compression"),
		MaxBufferedRecords: cmd.Int("kafka-max-buffered-records"),
		MaxBufferedBytes:   cmd.Int("kafka-max-buffered-bytes"),
		DeliveryTimeout:    cmd.Duration("kafka-delivery-timeout"),
	}, log)
	if err != nil {
		return err
	}
	kafkaClient, err := kafkaConnector.Connect(ctx)
	if err != nil {
		// Connect solo devuelve error si se pidió parar antes de conectar.
		return nil
	}
	defer func() {
		// Al parar se envía lo que quede en el buffer. El margen es menor que
		// stopwaitsecs de supervisord (15s) para no acabar con un SIGKILL.
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := kafka.Close(flushCtx, kafkaClient); err != nil {
			log.Error(flushCtx, "closing Kafka", "error", err)
		}
	}()

	opcuaConnector, err := opcua.NewConnector(opcua.Config{Endpoint: cmd.String("endpoint")}, log)
	if err != nil {
		return err
	}
	opcuaClient, err := opcuaConnector.Connect(ctx)
	if err != nil {
		return nil
	}
	defer opcuaClient.Close(context.Background())

	// Adaptadores (infraestructura) vistos solo a través de sus puertos.
	var publisher application.MeasurementPublisher
	publisher, err = kafka.NewMeasurementPublisher(kafkaClient, cmd.String("kafka-topic"), log)
	if err != nil {
		return err
	}
	var source application.MeasurementSource
	source, err = opcua.NewMeasurementSource(opcuaClient, opcua.SourceConfig{
		Site:               centerID,
		Namespace:          cmd.String("namespace"),
		SamplingInterval:   cmd.Duration("sampling-interval"),
		PublishingInterval: cmd.Duration("publishing-interval"),
	}, log)
	if err != nil {
		return err
	}

	// Caso de uso: recibe cada medida y la publica.
	service, err := application.NewPublishMeasurementService(publisher, log)
	if err != nil {
		return err
	}

	err = source.Subscribe(ctx, tags, service)
	if ctx.Err() != nil {
		log.Info(ctx, "shutdown signal received, closing")
		return nil
	}
	return err
}
