# Simulador KEPServerEX (OPC-UA)

Servidor OPC-UA en Docker que imita las convenciones de KEPServerEX y publica métricas industriales simuladas.

| Parámetro   | Valor                                  |
|-------------|----------------------------------------|
| Endpoint    | `opc.tcp://localhost:49320`            |
| Seguridad   | `None` / usuario `Anonymous`           |
| Namespace   | `KEPServerEX` (índice `2`)             |
| NodeIds     | `ns=2;s=<Canal>.<Dispositivo>[.<Grupo>].<Tag>` |

## Uso

Desde la raíz del proyecto hay un `Makefile` (`make help` lista los comandos): `make up`, `make logs`, `make check`, `make restart`, `make down`, `make clean`.

Equivalente con Docker Compose:

```bash
docker compose up -d --build          # arrancar (desde la raíz del proyecto)
docker compose logs -f kepserver      # ver los tags publicados
docker compose exec kepserver python check_client.py   # navegar, leer, escribir y suscribirse
docker compose restart kepserver      # tras editar config/tags.yaml
docker compose down                   # parar
```

## Tags incluidos

- `Simulation Examples.Functions.{Ramp1,Sine1,Random1,User1}` — como el proyecto de ejemplo de KEPServerEX.
- `Line1.PLC1.{Temperature,Pressure,MotorSpeed,Running,ProductionCount,HighTempAlarm,TempSetpoint}`
  - `TempSetpoint` es escribible por el cliente.
  - `HighTempAlarm` se activa cuando `Temperature > 82`.
- `Line1.PLC1.Energy.{Power_kW,Voltage_V,Energy_kWh}` — grupo de tags.
- `Line1.Tank1.{Level_pct,InletValve,OutletValve,Status}`
- `<Canal>.<Dispositivo>._System.{_Enabled,_Error,_NoError,_Simulated,_DeviceId}` en cada dispositivo.

Los tags, tipos y tipo de simulación se definen en `config/tags.yaml` (documentado en la cabecera del fichero).
El fichero se lee solo al arrancar: tras editarlo hay que reiniciar el simulador (`make restart`).
El intervalo de actualización se controla con `UPDATE_INTERVAL_MS` en `docker-compose.yml`.

## Conectar desde Go

Con [gopcua](https://github.com/gopcua/opcua):

```go
c, _ := opcua.NewClient("opc.tcp://localhost:49320", opcua.SecurityMode(ua.MessageSecurityModeNone))
_ = c.Connect(ctx)
id, _ := ua.ParseNodeID("ns=2;s=Line1.PLC1.Temperature")
```

En producción conviene resolver el índice del namespace por URI (`KEPServerEX`) en vez de fijar `ns=2`.
Un KEPServerEX real suele exigir `Basic256Sha256` + certificados y usuario/contraseña; este simulador
solo expone `None`/anónimo para desarrollo.
