# opc-ua-consumer

**Resumen.** Consumidor en Go que se suscribe a tags de un servidor OPC-UA (KEPServerEX) y procesa sus cambios de valor,
pensado para ejecutarse de forma continua bajo supervisord dentro de Docker. El repositorio incluye un simulador de
KEPServerEX para desarrollar sin un servidor real. Este documento explica cómo arrancarlo todo, los conceptos de
OPC-UA en los que se basa el diseño y cómo usar la librería `gopcua` para conectar un cliente.

## Índice

1. [Arquitectura](#arquitectura)
2. [Puesta en marcha](#puesta-en-marcha)
3. [Estructura del proyecto](#estructura-del-proyecto)
4. [El consumidor](#el-consumidor)
5. [Conceptos de OPC-UA](#conceptos-de-opc-ua)
6. [Cómo conectar un cliente OPC-UA con gopcua](#cómo-conectar-un-cliente-opc-ua-con-gopcua)
7. [Publicación en Kafka](#publicación-en-kafka)
8. [Limitaciones conocidas](#limitaciones-conocidas)

---

## Arquitectura

```
┌─────────────────────────────┐          ┌───────────────────────────────────────┐
│ kepserver-sim (Python)      │          │ opc-ua-consumer (Go)                  │
│                             │ OPC-UA   │  supervisord (PID 1)                  │
│ tags.yaml → espacio de      │◄─────────┤   └── opc-ua-consumer run             │
│ direcciones ns=2;s=...      │ :49320   │        ├── lee config/tags.yaml       │
│ valores simulados cada 1 s  │          │        ├── conecta + suscripción      │
└─────────────────────────────┘          │        └── handleNotification → logs  │
                                         │            (después: Kafka)           │
                                         └───────────────────────────────────────┘
```

Ambos contenedores se definen en `docker-compose.yml`. El consumidor espera a que el simulador esté sano (healthcheck)
antes de arrancar.

## Puesta en marcha

Requisitos: Docker con Compose y Go 1.27.

```bash
make up                # construye y arranca simulador + consumidor
make consumer-logs     # ver las métricas que llegan
make check             # cliente Python de comprobación contra el simulador
make down              # parar todo
```

Para ejecutar el consumidor en local (fuera de Docker), con el simulador arrancado:

```bash
docker compose up -d kepserver
go run . run --center-id center-01
```

`make help` lista todos los comandos:

| Comando | Qué hace |
|---|---|
| `make up` / `make down` | Arranca / para todo el ecosistema |
| `make build` | Construye las imágenes |
| `make logs` | Logs del simulador |
| `make restart` | Reinicia el simulador (tras editar `kepserver-sim/config/tags.yaml`) |
| `make check` | Cliente Python que navega, lee, escribe y se suscribe al simulador |
| `make ps` / `make shell` | Estado de los contenedores / shell en el simulador |
| `make run ARGS="..."` | Ejecuta el consumidor en local con `go run . run $(ARGS)` |
| `make client` | Compila el consumidor en `bin/opc-ua-consumer` |
| `make consumer-logs` | Logs del consumidor dockerizado |
| `make consumer-status` | Estado del proceso dentro de supervisord |
| `make consumer-restart` | Reinicia el consumidor (tras editar `config/tags.yaml`) |
| `make clean` | Para todo y borra las imágenes construidas |

## Estructura del proyecto

```
.
├── main.go                     # CLI (urfave/cli) y composition root: construye adaptadores y servicios
├── internal/
│   ├── domain/
│   │   ├── measurement/        # value object Measurement
│   │   └── tag/                # Tag y su puerto tag.Repository
│   ├── application/            # servicios (TagsResolver, PublishMeasurementService) y puertos
│   │                           #   (MeasurementSource, MeasurementPublisher, MeasurementHandler)
│   ├── infraestructure/
│   │   ├── opcua/              # conector OPC-UA + adaptador MeasurementSource
│   │   ├── kafka/              # conector Kafka + adaptador MeasurementPublisher
│   │   └── repository/tag/     # tagrepo: tag.Repository sobre fichero YAML
│   └── shared/logger/          # logger centralizado (slog)
├── config/tags.yaml            # tags que consume el consumidor
├── Dockerfile                  # imagen del consumidor: binario Go + supervisord
├── docker/supervisord.conf     # mantiene vivo el consumidor dentro del contenedor
├── docker-compose.yml          # simulador + consumidor + Kafka + Kafka UI
├── Makefile
└── kepserver-sim/              # simulador de KEPServerEX (ver su README)
    ├── server.py
    ├── config/tags.yaml        # tags que publica el simulador
    ├── check_client.py
    └── Dockerfile
```

El simulador tiene su propio [README](kepserver-sim/README.md). En resumen: imita las convenciones de KEPServerEX
(puerto `49320`, namespace `KEPServerEX` en el índice 2, NodeIds `ns=2;s=Canal.Dispositivo[.Grupo].Tag`, tags
`_System` por dispositivo) y genera valores simulados (senoides, rampas, contadores, booleanos...) cada segundo.
Su `tags.yaml` se lee solo al arrancar.

Los tags del consumidor los resuelve `application.TagsResolver` a través del puerto `tag.Repository` (quita
duplicados y falla si no queda ninguno). Hoy la única implementación es `tagrepo.FileRepository` (YAML); para leerlos
de una BBDD o del servidor OPC-UA (Browse) basta con otra implementación del puerto y cambiar una línea en `main.go`.

## El consumidor

### Flujo de `run`

```
loadTags        lee config/tags.yaml                     → ["Line1.PLC1.Temperature", ...]
parseTrigger    valida --trigger
connect         conexión + sesión (reintenta con backoff hasta conectar)
FindNamespace   "KEPServerEX" → índice (p. ej. 2)
subscribe       una suscripción con un monitored item por tag (NodeId = ns + ruta)
bucle           select { ctx.Done → salir | notifyCh → handleNotification }
```

- Si el servidor no está disponible al arrancar, `connect` reintenta con backoff exponencial (1 s → 30 s máx.).
- Si la conexión se cae después, gopcua reconecta (`AutoReconnect`) y restaura la suscripción por su cuenta.
- Con SIGTERM/SIGINT (supervisord, `docker stop`, Ctrl+C) cancela la suscripción, cierra la sesión y sale con código 0.

### Parámetros

Todos los flags se pueden dar también por variable de entorno.

| Flag | Variable | Por defecto | Descripción |
|---|---|---|---|
| `--endpoint` (global) | `OPCUA_ENDPOINT` | `opc.tcp://localhost:49320` | Endpoint del servidor |
| `--log-level` (global) | `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `--log-format` (global) | `LOG_FORMAT` | `text` | `text` o `json` (logs vía `internal/shared/logger`) |
| `--center-id` | `CENTER_ID` | **obligatorio** | Identificador del centro. Va en todos los logs y será la key de Kafka |
| `--tags-file` | `OPCUA_TAGS_FILE` | `config/tags.yaml` | YAML con los tags a consumir |
| `--namespace` | `OPCUA_NAMESPACE` | `KEPServerEX` | URI del namespace de los tags |
| `--sampling-interval` | `OPCUA_SAMPLING_INTERVAL` | `500ms` | Cada cuánto muestrea el servidor cada tag |
| `--publishing-interval` | `OPCUA_PUBLISHING_INTERVAL` | `1s` | Cada cuánto envía el servidor los cambios acumulados |
| `--trigger` | `OPCUA_TRIGGER` | `status-value` | Qué provoca una notificación: `status`, `status-value`, `status-value-timestamp` (ver [Triggers](#triggers-qué-cuenta-como-cambio)) |

### Fichero de tags

```yaml
tags:
  - Line1.PLC1.Temperature
  - Line1.PLC1.Energy.Power_kW
  - Simulation Examples.Functions.Ramp1
```

Cada entrada es la ruta del tag en KEPServerEX (`Canal.Dispositivo[.Grupo].Tag`). Los duplicados se ignoran; si el
fichero no existe o no tiene tags, el consumidor no arranca. Un tag que no existe en el servidor no detiene nada: se
registra `tag not monitored ... status=StatusBadNodeIDUnknown` y el resto sigue.

El fichero se lee solo al arrancar; tras editarlo, `make consumer-restart` (Docker) o relanzar el proceso (local).

### Ejemplos de ejecución

```bash
go run . run --center-id madrid-01
go run . run --center-id madrid-01 --log-level debug --tags-file otro.yaml
go run . run --center-id madrid-01 --trigger status-value-timestamp
go run . --endpoint opc.tcp://192.168.1.50:49320 run --center-id madrid-01
CENTER_ID=madrid-01 LOG_LEVEL=debug go run . run
make run ARGS="--center-id madrid-01"
```

Salida con `--log-level debug` (con `info` no se registra cada medida publicada):

```
level=INFO msg="tags loaded" center_id=madrid-01 file=config/tags.yaml count=18
level=INFO msg="connected to Kafka" center_id=madrid-01 brokers=[localhost:9094] linger=50ms batch_max_bytes=1000012 compression=zstd ...
level=INFO msg="connected to OPC-UA server" center_id=madrid-01 endpoint=opc.tcp://localhost:49320 security_policy=None security_mode=None
level=INFO msg="consuming changes" center_id=madrid-01 subscription_id=79 monitored=18 total=18
level=DEBUG msg="measurement published" center_id=madrid-01 measurement="{DeviceId:Simulation Examples.Functions Name:Ramp1 Value:{Kind:DECIMAL Value:64} ...}"
```

### Docker y supervisord

La imagen (`Dockerfile`) compila el binario en una etapa `golang:1.27-alpine` y lo copia a `alpine:3.22` con
supervisord como PID 1. `docker/supervisord.conf`:

- arranca `opc-ua-consumer run` como usuario sin privilegios `consumer`;
- lo reinicia si muere (`autorestart=true`);
- lo para con SIGTERM y espera hasta 15 s a que cierre;
- envía sus logs a la salida del contenedor (`docker logs`).

La configuración va por variables de entorno en el servicio `consumer` de `docker-compose.yml`. `config/` se monta en
`/etc/opc-ua-consumer`, así que el YAML se puede editar sin reconstruir la imagen.

¿Por qué supervisord y no cron? Una suscripción OPC-UA es una sesión abierta con el servidor: con cron habría que
reconectar en cada ejecución y se perderían los cambios ocurridos entre ejecuciones.

### GoLand

Crear una configuración *Go Build* de tipo *Package* (`opc-ua-consumer`), *Program arguments* `run`, *Working
directory* la raíz del proyecto y las variables de entorno de la tabla anterior (al menos `CENTER_ID`). Guardarla como
fichero del proyecto (*Store as project file*) la deja en `.run/` para compartirla.

---

## Conceptos de OPC-UA

### Capas

```
Endpoint (opc.tcp://host:49320)
└── Conexión + Sesión            ← connect()
    └── Suscripción               ← subscribe()
        ├── MonitoredItem: Line1.PLC1.Temperature
        └── ...  (uno por tag)
```

- **Sesión**: el "login" del cliente. Con ella ya se puede leer (`Read`) o escribir (`Write`) bajo demanda.
- **Suscripción**: objeto que vive *en el servidor*, dentro de la sesión. Agrupa monitored items y envía sus cambios
  periódicamente.
- **Monitored item**: "vigila este atributo de este nodo". Uno por tag.

### NodeIds y namespace

Cada nodo se identifica por un NodeId: índice de namespace + identificador. En KEPServerEX el identificador es un
string con la ruta completa del tag: `ns=2;s=Line1.PLC1.Temperature`.

El índice (`2`) lo asigna el servidor, por eso no se pone fijo: se pregunta por la URI del namespace
(`c.FindNamespace(ctx, "KEPServerEX")`) después de conectar.

### Polling vs suscripción

| | Polling (`Read`) | Suscripción |
|---|---|---|
| Quién inicia | El cliente, cuando quiere (ticker, cron) | El servidor, cuando hay cambios |
| Cambios entre lecturas | Se pierden | Se reciben (dentro del sampling interval) |
| Tráfico sin cambios | Lee igualmente | Nada |
| Modelo de código | Bucle con ticker | Bucle que lee de un canal, como un consumidor de Kafka |

El consumidor usa suscripción: no hay ticker; el bucle espera en `notifyCh`.

### Sampling interval y publishing interval

```
Servidor:  cada SamplingInterval (500 ms) lee el valor de cada tag
           si cambió, lo encola en la cola del monitored item (QueueSize 10, DiscardOldest)
           cada PublishingInterval (1 s) envía todo lo encolado en una notificación
Cliente:   gopcua la recibe y la deja en notifyCh → handleNotification
```

- Un cambio que ocurre y se revierte entre dos muestras no se ve: para señales rápidas, bajar el sampling interval.
- Al crear cada monitored item el servidor envía su **valor actual** aunque no haya cambiado: es la foto inicial,
  no hace falta un `Read` previo.
- Internamente gopcua envía peticiones `Publish` que el servidor responde cuando hay datos; el código de usuario solo
  lee del canal.

### Notificaciones

`handleNotification` es el punto donde se procesa cada mensaje del servidor (el *handler* de un consumidor de Kafka).
Una notificación puede ser:

| Tipo | Significado | Qué hacemos |
|---|---|---|
| `n.Error != nil` | Error al recibir (p. ej. durante una reconexión) | Log de aviso |
| `*ua.DataChangeNotification` | Cambios de valor: **lote** con todos los cambios del publishing interval, de varios tags y posiblemente varios del mismo tag | Por cada item: `tags[ClientHandle]` → nombre del tag; `item.Value` → valor, calidad (`Status`) y `SourceTimestamp` |
| `*ua.StatusChangeNotification` | La suscripción cambia de estado (p. ej. el servidor la va a cerrar) | Log de aviso |

El **client handle** es un número que el cliente asigna a cada monitored item y el servidor devuelve en cada
notificación. Usamos el índice del tag en la lista, de modo que `tags[item.ClientHandle]` da su nombre.

`SourceTimestamp` es cuándo se produjo el dato en origen (PLC/driver); `ServerTimestamp` cuándo lo procesó el
servidor. Para métricas interesa el primero.

### Triggers: qué cuenta como cambio

El `DataChangeFilter` de cada monitored item define qué provoca una notificación (`--trigger`):

| `--trigger` | Notifica cuando cambia… | Uso típico |
|---|---|---|
| `status` | solo la calidad (Good/Bad/Uncertain) | Vigilar la salud de la comunicación |
| `status-value` (defecto) | la calidad o el valor | Métricas normales; no repite valores iguales |
| `status-value-timestamp` | la calidad, el valor o el `SourceTimestamp` | Latido por tag: saber que el dato se refrescó aunque no cambie |

Medido contra el simulador durante 4 s (`TempSetpoint` es un valor fijo que el simulador reescribe cada segundo con
timestamp nuevo; `Temperature` cambia cada segundo):

| `--trigger` | Notificaciones de `TempSetpoint` | Notificaciones de `Temperature` |
|---|---|---|
| `status` | 1 (la inicial) | 1 (la inicial) |
| `status-value` | 1 (la inicial) | 4 |
| `status-value-timestamp` | 4 | 4 |

Ojo: con `status-value-timestamp` solo hay notificación si el servidor *actualiza* el `SourceTimestamp` aunque el valor
no cambie. Eso depende del servidor y del driver; en KEPServerEX hay que comprobarlo contra el servidor real. Si no lo
actualiza, el resultado es igual que con `status-value`. Y si lo actualiza, cada tag genera una notificación por
lectura al PLC: mucho más tráfico.

Alternativa si se quiere un latido controlado: `status-value` y que el consumidor reemita periódicamente el último
valor conocido de cada tag.

El filtro también admite **deadband** (ignorar variaciones menores que un umbral absoluto o un porcentaje del rango),
útil para reducir ruido en señales analógicas. No está implementado (`DeadbandTypeNone`).

### Calidad (StatusCode)

Cada valor llega con un StatusCode: `Good`, `Uncertain` o `Bad` (con subcódigos: `BadNodeIdUnknown`,
`BadCommunicationError`...). Un valor `Bad` suele significar que el servidor no puede leer el PLC; el valor que
acompaña no es fiable. Lo habitual es publicarlo marcado como tal en vez de descartarlo.

### Sin offsets: reconexión y pérdida de datos

A diferencia de Kafka, OPC-UA no guarda histórico para el cliente:

- La suscripción vive en el servidor ligada a la sesión. Si la conexión se cae, el cliente debe restaurarla
  (gopcua lo hace con `AutoReconnect`: intenta transferirla a la nueva sesión o recrearla).
- Lo que cambie mientras el cliente está desconectado no se recibe; al recrearse los monitored items llega el valor
  actual de cada tag.
- Si se necesita no perder datos, el servidor tiene que ofrecer *Historical Access* (fuera del alcance actual).

### De dónde sale la lista de tags

`subscribe` necesita una lista de NodeIds; de dónde sale es independiente:

| Forma | Ventaja | Inconveniente |
|---|---|---|
| **Lista en YAML** (actual) | Simple, explícita, controlas exactamente qué se consume | Un tag nuevo requiere editar el YAML y reiniciar |
| **Descubrimiento con Browse** | Los tags nuevos se detectan solos (re-browse periódico + `Monitor`/`Unmonitor`) | Más código; en servidores grandes puede devolver miles de tags si no se acota por ramas |

Se probó la versión con descubrimiento y se descartó por ahora en favor de la simplicidad. Un punto intermedio: listar
ramas (`Line1.PLC1`) en el YAML y hacer Browse solo de ellas al arrancar.

---

## Cómo conectar un cliente OPC-UA con gopcua

[gopcua/opcua](https://github.com/gopcua/opcua) es un cliente OPC-UA en Go puro. Este proyecto usa la v0.9.1. Todos
los ejemplos de esta sección se han ejecutado contra el simulador.

```bash
go get github.com/gopcua/opcua@v0.9.1
```

```go
import (
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)
```

### Conectar

```go
ctx := context.Background()

c, err := opcua.NewClient("opc.tcp://localhost:49320",
	opcua.SecurityMode(ua.MessageSecurityModeNone),
	opcua.AuthAnonymous(),
	opcua.AutoReconnect(true),                // reconecta y restaura suscripciones si cae la conexión
	opcua.ReconnectInterval(5*time.Second),
	opcua.RequestTimeout(10*time.Second),
)
if err != nil {
	log.Fatal(err)
}
if err := c.Connect(ctx); err != nil {      // conexión TCP + canal seguro + sesión
	log.Fatal(err)
}
defer c.Close(ctx)

ns, err := c.FindNamespace(ctx, "KEPServerEX") // índice del namespace (p. ej. 2)
```

`Connect` falla si el servidor no responde; para un servicio de larga duración, envolverlo en un bucle de reintentos
(ver `connect` en `main.go`). `AutoReconnect` solo actúa *después* de la primera conexión.

### Leer (Read)

```go
resp, err := c.Read(ctx, &ua.ReadRequest{
	NodesToRead: []*ua.ReadValueID{
		{NodeID: ua.NewStringNodeID(ns, "Line1.PLC1.Temperature"), AttributeID: ua.AttributeIDValue},
	},
	TimestampsToReturn: ua.TimestampsToReturnBoth,
})
if err != nil {
	log.Fatal(err)
}
dv := resp.Results[0] // un resultado por nodo, en el mismo orden
fmt.Println(dv.Value.Value(), dv.Status, dv.SourceTimestamp)
```

Se pueden leer muchos nodos en una sola petición; comprobar `dv.Status` de cada uno.

### Escribir (Write)

```go
resp, err := c.Write(ctx, &ua.WriteRequest{
	NodesToWrite: []*ua.WriteValue{{
		NodeID:      ua.NewStringNodeID(ns, "Line1.PLC1.TempSetpoint"),
		AttributeID: ua.AttributeIDValue,
		Value: &ua.DataValue{
			EncodingMask: ua.DataValueValue,
			Value:        ua.MustVariant(78.5), // el tipo Go debe coincidir con el del tag (float64 → Double)
		},
	}},
})
// resp.Results[0] es el StatusCode de la escritura (StatusOK si fue bien)
```

Si el tipo no coincide, el servidor responde `BadTypeMismatch`; si el tag es de solo lectura, `BadNotWritable` /
`BadUserAccessDenied`.

### Navegar (Browse)

```go
children, err := c.Node(ua.NewStringNodeID(ns, "Line1.PLC1")).
	Children(ctx, id.HierarchicalReferences, ua.NodeClassVariable)
for _, n := range children {
	fmt.Println(n.ID) // ns=2;s=Line1.PLC1.Temperature, ...
}
```

Para recorrer todo el árbol: partir de `ua.NewNumericNodeID(0, id.ObjectsFolder)` y bajar recursivamente por los
nodos `NodeClassObject`. En servidores grandes, usar `c.Browse` con `RequestedMaxReferencesPerNode` y seguir los
*continuation points* con `c.BrowseNext`.

### Suscribirse

```go
notifyCh := make(chan *opcua.PublishNotificationData, 256)

sub, err := c.Subscribe(ctx, &opcua.SubscriptionParameters{
	Interval: time.Second, // publishing interval
}, notifyCh)
if err != nil {
	log.Fatal(err)
}
defer sub.Cancel(ctx)

item := opcua.NewMonitoredItemCreateRequestWithDefaults(
	ua.NewStringNodeID(ns, "Line1.PLC1.Temperature"),
	ua.AttributeIDValue,
	1, // client handle: vuelve en cada notificación para identificar el tag
)
item.RequestedParameters.SamplingInterval = 500 // ms
item.RequestedParameters.Filter = ua.NewExtensionObject(&ua.DataChangeFilter{
	Trigger:      ua.DataChangeTriggerStatusValue,
	DeadbandType: uint32(ua.DeadbandTypeNone),
})

res, err := sub.Monitor(ctx, ua.TimestampsToReturnBoth, item)
// res.Results[i].StatusCode indica si cada item se creó (p. ej. BadNodeIdUnknown si no existe)

for n := range notifyCh {
	if n.Error != nil {
		log.Print(n.Error)
		continue
	}
	switch v := n.Value.(type) {
	case *ua.DataChangeNotification:
		for _, it := range v.MonitoredItems {
			fmt.Println(it.ClientHandle, it.Value.Value.Value(), it.Value.Status, it.Value.SourceTimestamp)
		}
	case *ua.StatusChangeNotification:
		log.Print("estado de la suscripción: ", v.Status)
	}
}
```

Se pueden añadir items a una suscripción existente con más llamadas a `sub.Monitor` y quitarlos con
`sub.Unmonitor(ctx, monitoredItemIDs...)` (los IDs vienen en `res.Results[i].MonitoredItemID`). Ojo: si gopcua recrea
la suscripción tras una reconexión, esos IDs cambian y no se exponen.

Para enterarse de las reconexiones: `opcua.StateChangedCh(ch)` o `opcua.StateChangedFunc(f)` al crear el cliente
(estados `Connected`, `Disconnected`, `Reconnecting`, `Closed`). Si se usa el canal, hay que leerlo siempre: gopcua se
bloquea al escribir en él.

### Conectar a un KEPServerEX real (seguridad)

El simulador solo acepta `None` + anónimo. Un KEPServerEX real suele exigir cifrado, certificado de cliente y usuario.

**1. Generar un certificado de cliente.** El `subjectAltName` URI debe coincidir con el `ApplicationURI` del cliente:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 365 \
  -keyout client.key -out client.crt \
  -subj "/CN=opc-ua-consumer" \
  -addext "subjectAltName=URI:urn:opc-ua-consumer,DNS:localhost" \
  -addext "keyUsage=critical,digitalSignature,nonRepudiation,keyEncipherment,dataEncipherment" \
  -addext "extendedKeyUsage=clientAuth"
```

gopcua acepta certificado y clave en PEM o DER.

**2. Elegir el endpoint según la política de seguridad y conectar:**

```go
endpoint := "opc.tcp://kepserver:49320"

eps, err := opcua.GetEndpoints(ctx, endpoint)
if err != nil {
	log.Fatal(err)
}
ep, err := opcua.SelectEndpoint(eps, "Basic256Sha256", ua.MessageSecurityModeSignAndEncrypt)
if err != nil {
	log.Fatal(err) // el servidor no ofrece esa combinación
}

c, err := opcua.NewClient(endpoint, // la URL propia, no ep.EndpointURL (puede traer un hostname interno)
	opcua.CertificateFile("client.crt"),
	opcua.PrivateKeyFile("client.key"),
	opcua.ApplicationURI("urn:opc-ua-consumer"),
	opcua.AuthUsername("usuario", "contraseña"), // o opcua.AuthAnonymous()
	opcua.SecurityFromEndpoint(ep, ua.UserTokenTypeUserName), // ua.UserTokenTypeAnonymous si es anónimo
	opcua.AutoReconnect(true),
)
```

**3. Confiar en el certificado en el servidor.** La primera conexión será rechazada: en KEPServerEX, abrir la *OPC UA
Configuration Manager* → *Trusted Clients*, y marcar el certificado del cliente como confiable. Comprobar también en la
configuración del endpoint de KEPServerEX qué políticas de seguridad están habilitadas.

Otras políticas habituales: `Aes128_Sha256_RsaOaep`, `Aes256_Sha256_RsaPss`. `Basic128Rsa15` y `Basic256` están
obsoletas.

### Errores frecuentes

| Error | Causa probable |
|---|---|
| `connection refused` / timeout en `Connect` | Servidor caído, puerto incorrecto o firewall |
| `BadNodeIdUnknown` | El NodeId no existe: ruta del tag mal escrita o índice de namespace incorrecto |
| `BadSecurityChecksFailed` | Certificado del cliente no confiado en el servidor, o `ApplicationURI` distinto del del certificado |
| `BadIdentityTokenRejected` / `BadUserAccessDenied` | Usuario/contraseña incorrectos o sin permisos |
| `BadTypeMismatch` (Write) | El tipo del `Variant` no coincide con el del tag |
| `no matching endpoint found for policy ... and mode ...` (`SelectEndpoint`) | El servidor no ofrece la política/modo pedidos: listar `GetEndpoints` para ver cuáles hay |

---

## Publicación en Kafka

Cada `Measurement` se publica en JSON en el tópico `--kafka-topic` (`measurements`), con **key = `--center-id`**.
Implementación en `internal/infraestructure/kafka` con [franz-go](https://github.com/twmb/franz-go), detrás del puerto
`application.MeasurementPublisher`.

### Ver los mensajes

- **Kafka UI** ([kafbat/kafka-ui](https://github.com/kafbat/kafka-ui)): <http://localhost:8085> (`make kafka-ui`).
  En *Topics → measurements → Messages* se ven las medidas con su key y partición; en *Consumers*, los consumer groups
  y su lag.
- **Terminal**: `make kafka-tail` (o `make kafka-tail TOPIC=otro`) muestra en vivo partición, key y JSON.

Kafka no tiene volumen: si se recrea el contenedor (`make down`, cambios en su servicio) se pierden los tópicos y los
mensajes. `docker compose up -d` vuelve a crear `measurements` con `kafka-init`.

### Garantías

- **Orden por centro**: todas las medidas de un centro van a la misma partición. franz-go usa murmur2, el mismo hash que
  el cliente Java (`sarama` y `segmentio/kafka-go` no, salvo que se configuren).
- **Sin duplicados ni reordenación en reintentos**: productor idempotente con `acks=all` (defaults de franz-go).
- **Asíncrono**: `Publish` solo mete la medida en el buffer; franz-go la agrupa en batches y la envía. Los fallos de
  entrega (tras reintentar hasta `--kafka-delivery-timeout`) se registran como `measurement not delivered to Kafka`.
- **Backpressure**: si el buffer se llena (Kafka caído mucho tiempo), `Publish` se bloquea y deja de leer
  notificaciones de OPC-UA hasta que haya hueco.
- **Parada limpia**: al recibir SIGTERM se vacía el buffer (hasta 10 s, por debajo del `stopwaitsecs` de supervisord).

### Tuning del productor

| Flag / variable | Default | Por qué |
|---|---|---|
| `--kafka-linger` / `KAFKA_LINGER` | `50ms` | Espera a llenar el batch. Con tráfico moderado evita batches de pocos mensajes; con mucho tráfico el batch se llena antes por tamaño. Es latencia añadida máxima, aceptable para métricas. |
| `--kafka-batch-max-bytes` / `KAFKA_BATCH_MAX_BYTES` | `1000012` | Igual que `max.message.bytes` del broker. Subirlo exige subir también `max.message.bytes` del tópico. |
| `--kafka-compression` / `KAFKA_COMPRESSION` | `zstd` | El JSON de métricas es muy repetitivo: zstd es el que mejor comprime con un coste de CPU similar. |
| `--kafka-max-buffered-records` / `KAFKA_MAX_BUFFERED_RECORDS` | `500000` | Margen para absorber caídas cortas del broker. |
| `--kafka-max-buffered-bytes` / `KAFKA_MAX_BUFFERED_BYTES` | `256 MiB` | Tope de memoria del buffer (~185 B por medida). |
| `--kafka-delivery-timeout` / `KAFKA_DELIVERY_TIMEOUT` | `2m` | Cubre un failover de líder; pasado ese tiempo la medida se descarta y se registra. |

Medido con `BenchmarkPublish` (`publisher_benchmark_test.go`) contra el Kafka local de un nodo:

| Configuración | msgs/s | Bytes en red por medida |
|---|---|---|
| Síncrono (`ProduceSync`, una medida por petición) | ~1.500 | 322 |
| Asíncrono, sin compresión | ~115.000–170.000 | 195 |
| Asíncrono, snappy | ~125.000–140.000 | 25 |
| Asíncrono, lz4 | ~80.000–125.000 | 27 |
| Asíncrono, zstd | ~115.000–125.000 | 18 |

El cuello de botella con estos números es el propio productor (serialización JSON), no Kafka. Los ratios de
compresión son optimistas porque las medidas sintéticas se repiten; con datos reales serán algo peores, pero el orden
entre algoritmos se mantiene.

### Configuración del tópico en producción

El `docker-compose.yml` crea `measurements` con 3 particiones y réplica 1 (solo desarrollo). En producción:

- `replication.factor=3` y `min.insync.replicas=2`: con `acks=all` se tolera la caída de un broker sin perder datos.
- **Particiones**: una partición sirve a varios centros, pero un centro solo usa una. Dimensionar para que la
  partición más cargada (el centro más grande) vaya holgada. Cambiar el número de particiones reasigna las keys.
- `compression.type=producer` (default): el broker guarda los batches tal cual llegan, sin recomprimir.
- `retention.ms` / `retention.bytes` según cuánto tiempo deban poder releerse las métricas.

### Pendiente

- Formato: JSON es cómodo pero ocupa más y cuesta más CPU que Avro/Protobuf con Schema Registry.
- Timestamp: hoy es la hora de creación de la medida, no el `SourceTimestamp` de OPC-UA.

## Limitaciones conocidas

- **Seguridad**: el consumidor solo conecta con `None` + anónimo. Para un KEPServerEX real hay que añadir política,
  certificado y usuario (ver [Conectar a un KEPServerEX real](#conectar-a-un-kepserverex-real-seguridad)).
- **Reconexión**: se confía en la restauración de gopcua. Si el servidor vuelve sin algún tag, gopcua lo descarta y solo
  lo registra en su propio log. Si al reiniciar el servidor cambia el índice del namespace, la suscripción restaurada
  sigue usando el antiguo. Solución sencilla si llega a importar: salir con error al reconectar y dejar que supervisord
  reinicie el proceso.
- **Tags estáticos**: los tags nuevos en el servidor no se consumen hasta añadirlos al YAML y reiniciar.
- **Simulador**: al ser `asyncua`, el comportamiento de timestamps y calidad no es necesariamente idéntico al de
  KEPServerEX; validar `--trigger status-value-timestamp` contra el servidor real.
