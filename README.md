# AndesTransporte — integración de telemetría OT→IT

Prototipo de una capa de integración dirigida por eventos para detectar, en tiempo casi real, la firma de una válvula ilícita o fuga en un tramo de oleoducto: caída de presión junto con descuadre entre el caudal que entra y el que sale. La telemetría nace en el lado OT (MQTT), cruza una DMZ industrial como CloudEvents validados contra un esquema gobernado, y llega al lado IT (Redpanda), donde se detecta la anomalía, se publica la alerta y se integra con el sistema de balance volumétrico.

Taller 1 · ARTI4212.

## Estructura

```
asyncapi/     contrato AsyncAPI 3 (canales de telemetría y alertas)
schemas/      JSON Schema de los dos eventos
prototipo/
  docker-compose.yml
  mosquitto/  broker MQTT (nivel 3, OT simulado)
  simulador/  generador de telemetría con inyección de anomalías
  puente/     puente OT→IT: normaliza a CloudEvents, valida y publica (DMZ, nivel 3.5)
  detector/   regla de detección + outbox transaccional (nivel 4)
  api/        API REST del estado del tramo (OpenAPI) y stub del sistema de balance
  camel/      flujo de integración Apache Camel (perfil opcional)
  panel/      panel de operación para la demo
  registrar-esquemas.py
```

## Requisitos

Docker Desktop y Python 3.10+. Node solo para validar el AsyncAPI.

## Cómo correrlo

```bash
cd prototipo
docker compose up -d --build
python registrar-esquemas.py
```

El primer arranque tarda un par de minutos (descarga de imágenes y build). El registry arranca vacío, por eso el segundo comando: registra los dos esquemas con `schemaType: JSON` y fija `BACKWARD_TRANSITIVE`. Con `--probar` además envía un cambio incompatible (`data.valor` de `number` a `string`) y muestra el rechazo del registry.

Servicios:

| | |
|---|---|
| Redpanda Console | http://localhost:8080 |
| API y OpenAPI | http://localhost:8090/docs |
| Schema Registry | http://localhost:18081/subjects |
| MQTT | localhost:1883 |
| Kafka (externo) | localhost:19092 |

Flujo Camel (opcional, tarda en construir la primera vez):

```bash
docker compose --profile camel up -d --build camel
```

## Panel de operación

Corre fuera de Docker porque ejecuta `docker compose`:

```bash
cd prototipo/panel/backend
pip install -r requirements.txt
python app.py
```

http://localhost:9500. Levanta y baja el stack, muestra cada tramo en vivo (presión, caudales de entrada y salida, descuadre) refrescado cada segundo, inyecta o quita la anomalía en un tramo concreto, registra los esquemas y ejecuta la prueba de compatibilidad, y muestra los logs de cada contenedor. Más en `prototipo/panel/README.md`.

## Cómo funciona la detección

El simulador publica cada segundo presión, caudal aguas arriba, caudal aguas abajo y temperatura por tramo. El puente valida cada lectura contra `schemas/telemetria-lectura.schema.json` y la publica en `ot.telemetry.v1` con `tramoId` como clave de partición; lo que no valida no cruza la DMZ.

El detector compara cada muestra contra la ficha nominal del tramo (`prototipo/detector/tramos-nominal.json`, 45 bar y 120 m³/h). Si la presión cae más del 10 % respecto del nominal y el descuadre entre caudales supera 8 m³/h durante 3 muestras seguidas, emite una alerta en `it.alerts.valve-anomaly.v1`, una por episodio. La alerta llega unos 4 segundos después de inyectar la anomalía. La regla está aislada en `detector/regla.py` y tiene pruebas:

```bash
cd prototipo/detector
python -m pytest -q
```

Umbrales y persistencia se ajustan en `docker-compose.yml` (servicio `detector`); el punto de operación de cada tramo, en la ficha nominal.

## Demo

1. Levantar el stack y registrar los esquemas.
2. Abrir el panel. Ambos tramos en `normal`.
3. Inyectar la anomalía en `tramo-14`. En ~4 s pasa a `alerta_activa`; `tramo-22` sigue en `normal`. En Redpanda Console se ve el evento en `it.alerts.valve-anomaly.v1`; en `GET /tramos/tramo-14/estado`, el estado consolidado.
4. Con el perfil Camel arriba, la alerta llega a `POST /notificaciones/urgente` (si es alta) y a `POST /tramos/estado`; se ve en los logs de `api-estado-tramo`.
5. Quitar la anomalía: el tramo vuelve a `normal`.
6. Probar el cambio incompatible: el registry responde `is_compatible: false` con `TYPE_CHANGED` en `data.valor`.

## Validar el contrato

```bash
npx -y @asyncapi/cli validate asyncapi/telemetria-alertas.asyncapi.yaml
```

## Decisiones principales

- Publicación/suscripción sobre Redpanda con CloudEvents como envelope y JSON Schema en el registry, compatibilidad `BACKWARD_TRANSITIVE`.
- Clave de partición `tramoId`: orden garantizado por tramo, que es lo que la regla necesita.
- Entrega at-least-once en cada salto (QoS 1 en MQTT, `acks=all` y espera del ack en el puente, commit manual de offsets tras procesar) con consumidores idempotentes (deduplicación por `id` del CloudEvent).
- Transactional outbox en el detector: la alerta se escribe en SQLite local y un poller la publica; si el proceso cae entre detectar y publicar, no se pierde.
- La API es un modelo de lectura: se reconstruye completo desde el log en cada arranque.
- Flujo Camel con Idempotent Consumer, Content-Based Router por severidad, Message Translator hacia el formato del sistema de balance, Wire Tap de auditoría y Dead Letter Channel.
- Referencia nominal fija por tramo en vez de línea base aprendida: sin sesgo de arranque y con latencia de segundos.

Los diagramas C4, el mapeo Purdue/DMZ/IEC 62443 y los ADRs se entregan por separado.
