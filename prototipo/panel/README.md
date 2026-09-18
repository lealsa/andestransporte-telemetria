# Panel de operación

Interfaz para la demo: controla el stack, muestra el estado de cada tramo en vivo, inyecta anomalías en un tramo concreto y ejecuta las pruebas de gobierno del esquema.

## Correrlo

Requiere Python 3.10+ y Docker Desktop corriendo.

```bash
cd prototipo/panel/backend
pip install -r requirements.txt
python app.py
```

Abrir http://localhost:9500. El backend sirve el frontend y escucha solo en `127.0.0.1` porque puede ejecutar `docker compose down` sin autenticación. Corre fuera de Docker porque necesita invocar `docker compose` sobre el propio stack.

## Secciones

1. **Stack de Docker.** `up -d --build`, `down`, perfil Camel y "Reiniciar detección" (`restart detector api`). La consola de la derecha muestra la salida del comando en curso.
2. **Estado en vivo por tramo.** Cada tarjeta dibuja la tubería: caudal de entrada, presión y caudal de salida, con el nominal y la desviación. Se refresca cada segundo. Las filas fuera de umbral se pintan en rojo antes de que el detector confirme.
3. **Control manual de la anomalía.** Escribe `panel/control/anomalias.json`, que el simulador relee en cada ciclo. Solo los tramos listados reciben la anomalía. "Quitar anomalía" devuelve el simulador a valores normales y reconoce la alerta en la API, así el tramo vuelve a `normal` al instante.
4. **Gobierno del Schema Registry.** "Registrar esquemas" registra ambos subjects con `schemaType: JSON` y fija `BACKWARD_TRANSITIVE` (idempotente; necesario tras cada `down -v`). "Probar cambio incompatible" envía `data.valor` como `string` y muestra la respuesta: `is_compatible: false` con `TYPE_CHANGED`.
5. **Consolas por contenedor.** `docker logs --tail 80` de cada contenedor, refrescado cada 4 s.

## Notas

- Al levantar el stack o reiniciar la detección, el panel deja el control en modo manual sin tramos, para partir siempre de un estado conocido.
- La alerta aparece unos 4 s después de inyectar. No hay que esperar nada tras arrancar: el detector compara contra la ficha nominal, no aprende una línea base.
- El reconocimiento de alertas vive en memoria de la API. Tras reiniciarla, las alertas del log reaparecen sin reconocer; "Reiniciar detección" las reconoce automáticamente al terminar.
- El archivo de control está en `.gitignore`. Si el repositorio vive en una carpeta sincronizada (OneDrive), puede reaparecer con contenido viejo; la sección 3 muestra siempre el estado real.
- Tras editar `app.py` hay que reiniciar el backend a mano.
