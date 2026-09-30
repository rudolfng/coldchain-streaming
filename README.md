# coldchain-streaming

**Proyecto integrador: Pipeline de streaming para cadena de frío ultrabaja con Kafka y Apache Beam**

- **Universidad**: Universidad Nacional de Asunción, Facultad Politécnica
- **Programa**: Maestría en Inteligencia Artificial
- **Materia**: Streaming de datos y sus aplicaciones
- **Docente**: Rodrigo Parra, M.Sc.
- **Alumno**: Rudolf Niedhammer
- **Documento técnico**: [`docs/pf-coldchain.pdf`](docs/pf-coldchain.pdf) (diseño, decisiones, semántica de entrega y límites del sistema)

Pipeline de procesamiento de flujos de datos en tiempo real (Apache Kafka y Apache Beam sobre Apache Flink) para el monitoreo térmico en cadenas de frío de ultracongelación (−80 °C a −60 °C), orientado a la distribución de vacunas.

El sistema reproduce de forma determinista la telemetría de un ensayo físico de 3 horas realizado por el Oak Ridge National Laboratory con Carrier, simulando anomalías de red (duplicados, desorden temporal y registros inválidos) para computar agregaciones estadísticas por sensor en ventanas fijas de 5 minutos con garantías de idempotencia y consistencia convergente.

---

## 1. Origen de los datos

La telemetría proviene del estudio publicado en:
* Sun, Z., Ostrowski, K., et al. (2022). *Dataset of ultralow temperature refrigeration for COVID-19 vaccine distribution solution*. Scientific Data 9, 85. DOI: [10.1038/s41597-022-01167-y](https://doi.org/10.1038/s41597-022-01167-y).
* Datos fuente en Figshare: DOI [10.6084/m9.figshare.14888121](https://doi.org/10.6084/m9.figshare.14888121) (Licencia CC BY 4.0).

El pipeline utiliza el archivo `Test1_TempCO2O2.csv` (ID 28668003, 64 MB, MD5: `b8164462feaca1d699a53a2503f92d30`), correspondiente a un ensayo experimental de un contenedor de distribución con hielo seco monitoreado por 20 sensores internos (`ULT1-b01` a `ULT1-b20`).

---

## 2. Prerrequisitos y configuración del entorno

* **Docker y Docker Compose**: Docker Engine o Docker Desktop con al menos 8 GB de memoria RAM asignada y soporte para Compose v2 (`docker compose version`).
  * *Entornos Linux (Docker Engine)*: Los contenedores se ejecutan con el usuario `coldchain` (UID 1000) y persisten datos en `./data`. Si el usuario del sistema posee un UID distinto, es necesario otorgar permisos de lectura y escritura antes de iniciar los servicios:
    ```bash
    chmod -R a+rwX data
    ```
* **Python y gestor de paquetes**: [`uv`](https://docs.astral.sh/uv/) para ejecutar las pruebas y herramientas de línea de comandos fuera de Docker. El entorno utiliza Python 3.12 (`uv python install 3.12`).
* **GNU Make**: Utilizado para simplificar los comandos operativos. En Windows puede instalarse mediante `winget install ezwinports.make`, `choco install make` o ejecutarse desde Git Bash. En ausencia de `make`, cada instrucción puede ejecutarse invocando directamente las órdenes de Compose y `uv` detalladas en el archivo `Makefile`.
* **Conexión a internet**: Requerida durante el primer inicio para descargar el CSV fuente y construir las imágenes de Flink y Beam con soporte para ejecución políglota (Java + Python).

---

## 3. Inicio rápido

Para iniciar el clúster completo:

```bash
make up
```

Este comando levanta Kafka (un broker en modo KRaft), inicializa los tópicos, descarga y procesa el dataset fuente, despliega el clúster Flink (1 JobManager y 2 TaskManagers con paralelismo 2), inicia el Beam Job Server y deja en ejecución el `pipeline` de procesamiento junto con el `consumer` persistente hacia SQLite.

El grafo del pipeline tarda entre uno y dos minutos en registrarse en el panel de control de Flink (`http://localhost:8081`). Conviene verificar su estado activo antes de enviar eventos.

---

## 4. Modos de ejecución y comportamiento temporal

El sistema admite dos modalidades operativas para evaluar la respuesta del pipeline ante desorden temporal y retardos.

```bash
make replay   # Transmite los eventos del ensayo con inyecciones deterministas
make show     # Muestra las ventanas consolidadas, paneles tardíos y estado de la DLQ
make down     # Detiene los contenedores (make down-volumes elimina además los datos de Kafka)
```

`make show` lee las ventanas y los paneles de `data/out/metrics.sqlite` (el sink que escribe el `consumer`) y solo se conecta a Kafka, desde el anfitrión por `localhost:29092`, para contar la DLQ. Para inspeccionar el historial cronológico de paneles de un pod específico o filtrar la DLQ (fuera de `make`, fijar antes el broker del anfitrión; en PowerShell, `$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"`):

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:29092
uv run python scripts/report.py --history ULT1-b12
uv run python scripts/report.py --dlq-only
```

### 4.1. Corrida acelerada a 20× (modo por defecto)

Con `REPLAY_SPEEDUP=20`, las 3 horas del ensayo se transmiten en aproximadamente 9 minutos. El generador registra cada anomalía inyectada en `docs/evidencia/producer.log`: duplicados, ráfagas de retransmisión en `ULT1-b03`, registros corruptos y retención temporal de lecturas en `ULT1-b07` (66:20 a 66:26 del ensayo) y `ULT1-b12` (66:40 a 66:45, liberadas a las 67:05).

**Comportamiento de la marca de agua a 20×**:
El conector `KafkaIO` opera con política `CreateTime`, la cual ajusta la marca de agua al reloj físico del sistema. En una reproducción acelerada, el atributo `event_time` avanza hasta 2 horas y 51 minutos por delante del reloj real. Al quedar la marca de agua desfasada respecto a los datos:
1. Las ventanas emiten paneles de actualización anticipada (`EARLY`).
2. Las lecturas de `ULT1-b12`, liberadas con 20 a 25 minutos de retraso relativo, ingresan antes de que la marca de agua física alcance el fin de su ventana, por lo que son incorporadas para corregir las ventanas `14:50` y `14:55` (de 15 a 30 muestras) en lugar de ser descartadas.

**Resultado esperado de `make show`** (`docs/evidencia/report_despues_b07.txt`, corrida del 2026-09-24): 740 ventanas, 0 paneles `LATE`, DLQ 134 (73 `missing_field:payload.temp_c` + 61 `temp_not_numeric`) y ninguna ventana con más de 30 lecturas pese a 399 duplicados y 20 reenvíos. La ventana `ULT1-b07 | 14:30` pasa de 14 lecturas (`report_antes_b07.txt`, al liberarse b07) a 29. Solo las ventanas 13:10 y 13:15 de cada sensor cierran `ON_TIME` (40 panes).

### 4.2. Demostración en tiempo real a 1× (descarte efectivo por latencia)

Para comprobar el descarte de eventos tardíos y la emisión de paneles `ON_TIME` y `LATE` en Docker, se ejecuta el subtramo 66:15–67:15 a velocidad real (`REPLAY_SPEEDUP=1`). En este modo, el tiempo de evento coincide con el reloj físico y la marca de agua progresa junto con la telemetría.

En PowerShell:
```powershell
make down-volumes
make clean-data
$env:REPLAY_SPEEDUP = "1"; $env:REPLAY_START_HOURS = "66.25"; $env:REPLAY_END_HOURS = "67.25"
$env:REPLAY_ALIGN_START = "1"
make up
make replay
```

En bash:
```bash
make down-volumes
make clean-data
export REPLAY_SPEEDUP=1 REPLAY_START_HOURS=66.25 REPLAY_END_HOURS=67.25
export REPLAY_ALIGN_START=1
make up
make replay
```

La opción `REPLAY_ALIGN_START=1` sincroniza el inicio de la transmisión para que los minutos múltiplos de 5 del ensayo coincidan con los límites de ventana del reloj físico (espera hasta 5 min). Desde que empiezan a salir eventos: ~5 min, primeras ventanas `ON_TIME`; ~11 min, llega la ráfaga de `ULT1-b07` (paneles `LATE`); ~50 min, `ULT1-b12` libera sus lecturas y se descartan. El replay termina a los ~60 min; los paneles finales siguen llegando ~15 min más (hasta que vence el `allowed_lateness`).

**Resultados observados a 1× (`docs/evidencia/tiempo_real_report.txt`, corrida del 2026-09-25):**
* **Transmisión controlada**: 7 373 mensajes enviados (7 200 lecturas base, 153 duplicados y 20 reenvíos en ráfaga; de ellos, 51 eventos inválidos).
* **Sensor `ULT1-b07` (retraso de 0 a 6 minutos)**: 31 paneles `LATE` corrigen la ventana de las 15:50, actualizando el conteo de 1 a 30 lecturas conforme se procesa la ráfaga.
* **Sensor `ULT1-b12` (retraso de 20 a 25 minutos)**: Sus 30 lecturas llegan una vez superado el margen de 15 minutos de retraso permitido (`allowed_lateness`); Beam las descarta y su ventana de las 16:10 nunca se materializa en el almacenamiento (239 ventanas finales sobre 240 esperadas).
* **Balance exacto**: 7 119 registros válidos consolidados en SQLite (7 200 originales menos 51 inválidos y 30 descartados por latencia), 270 paneles `LATE` (239 cierres definitivos con `is_final: true` y 31 correcciones) y 51 registros en la DLQ.

Al finalizar la demostración a 1×, restablecer las variables de entorno. En PowerShell:
```powershell
Remove-Item Env:REPLAY_SPEEDUP, Env:REPLAY_START_HOURS, Env:REPLAY_END_HOURS, Env:REPLAY_ALIGN_START
make dataset
```

En bash:
```bash
unset REPLAY_SPEEDUP REPLAY_START_HOURS REPLAY_END_HOURS REPLAY_ALIGN_START
make dataset
```

---

## 5. Tópicos, claves y distribución de particiones

| Tópico | Estrategia de clave | Particiones | Productor | Consumidor | Contenido |
| --- | --- | :---: | --- | --- | --- |
| `coldchain.telemetry.raw.v1` | `sensor_id` | 4 | `producer.py` | Pipeline Beam | Telemetría bruta por sensor y muestra |
| `coldchain.metrics.5min.v1` | `sensor_id\|window_start` | 4 | Pipeline Beam | `consumer.py` | Paneles estadísticos agregados por ventana |
| `coldchain.telemetry.dlq.v1` | `sensor_id` o `unknown` | 4 | Pipeline Beam | `scripts/report.py` | Eventos descartados con tipificación de error |

### Justificación de claves y particionado
* **Clave de partición (`sensor_id`)**: Cada caja contenedora (`pod`) genera tráfico a un ritmo uniforme (una muestra cada 10 segundos). Utilizar `sensor_id` como clave preserva el orden secuencial por dispositivo físico y distribuye la carga equilibradamente. Si se utilizara el identificador general del contenedor (`asset_id: "ULT1"`), todo el tráfico convergería en una única partición (*hot key*).
* **Dimensionamiento a 4 particiones**: Diseñado para 20 sensores y un paralelismo de cómputo de 2 en Flink. El particionador por defecto (CRC32 de librdkafka) asigna 5, 6, 5 y 4 claves a las particiones 0 a 3, produciendo una distribución de 5 487, 6 615, 5 512 y 4 405 mensajes en el ensayo completo. La relación de desbalance máxima es de 1,50×, manteniéndose por debajo del umbral de 2× establecido para implementar un particionador algorítmico específico.

---

## 6. Contratos de eventos y esquema de salida

### Telemetría de entrada (`coldchain.telemetry.raw.v1`, versión 2)

```json
{
  "schema_version": 2,
  "event_id": "ULT1-b07@20201219T043012Z",
  "event_time": "2026-09-25T14:03:12Z",
  "sensor_id": "ULT1-b07",
  "asset_id": "ULT1",
  "asset_type": "contenedor",
  "payload": {
    "temp_c": -80.63,
    "temp_f": -113.13,
    "source_time": "2020-12-19T04:30:12Z"
  }
}
```

El identificador determinista `event_id` combina el sensor y la marca temporal de origen (`{sensor_id}@{source_time}`). Las retransmisiones originadas por cortes de conexión conservan el mismo identificador, posibilitando su deduplicación sin importar variaciones en el reloj de transmisión. La versión 2 del esquema otorga flexibilidad volviendo opcionales parámetros ambientales no críticos (humedad y batería) e incorporando la conversión térmica precalculada.

### Métricas de salida (`coldchain.metrics.5min.v1`, versión 1)

```json
{
  "schema_version": 1,
  "aggregate_id": "ULT1-b07|2026-09-19T04:00:00Z",
  "sensor_id": "ULT1-b07",
  "asset_id": "ULT1",
  "window_start": "2026-09-19T04:00:00Z",
  "window_end": "2026-09-19T04:05:00Z",
  "n_readings": 30,
  "avg_temp_c": -81.14,
  "min_temp_c": -81.19,
  "max_temp_c": -81.1,
  "out_of_range": true,
  "pane_timing": "EARLY",
  "pane_index": 1,
  "is_final": false,
  "emitted_at": "2026-09-19T02:02:08.041392Z"
}
```

`n_readings` contabiliza registros únicos por `event_id`. La bandera `out_of_range` se activa si la temperatura máxima supera los −60 °C o la mínima cae por debajo de −80 °C.

### Tipificación en Dead Letter Queue (DLQ)
Los registros rechazados durante la validación se publican en `coldchain.telemetry.dlq.v1` categorizados bajo causas precisas: `invalid_json`, `missing_field:<campo>`, `unsupported_schema_version`, `bad_timestamp`, `stale_clock`, `temp_missing`, `temp_not_numeric`, `temp_out_of_physical_range`, `bad_sensor_id` y `unexpected:<error>`.

---

## 7. Semántica de procesamiento y deduplicación

| Etapa | Garantía | Mecanismo técnico | Tolerancia operativa |
| --- | --- | --- | --- |
| Replay → Kafka | Al menos una vez | Productor con confirmación completa (`acks=all`) | Absorbe duplicados lógicos inyectados |
| Kafka → Beam | Al menos una vez | Puntos de control en Flink cada 30 segundos | Recupera fallas de ejecución desde el último checkpoint |
| Núcleo Beam | Efectivamente una vez | Deduplicación en el acumulador de `CombineFn` | Horizonte acotado a la duración de ventana más latencia |
| Beam → Kafka | Al menos una vez | `WriteToKafka` sin transacciones distribuidas | Tolera reemisiones tras reinicios no coordinados |
| Destino SQLite | Consistencia convergente | Operación upsert basada en mayor `pane_index` | Desestima paneles con índice inferior al registrado |

### Deduplicación integrada en agregación
Una etapa previa aislada de `GroupByKey` por `event_id` combinada con el modo acumulativo reemite la totalidad del conjunto ante cada disparo, provocando que las transformaciones posteriores sumen duplicados de forma incremental. Para asegurar precisión analítica, la deduplicación se implementó directamente en la estructura acumuladora de `TempStatsCombineFn` (`{event_id: temp_c}`). El acumulador mantiene un diccionario de eventos únicos cuyo ciclo de vida coincide exactamente con el tiempo en que la ventana permanece activa.

---

## 8. Pruebas y verificación de consistencia

Para ejecutar la verificación sin levantar contenedores Docker:

```bash
uv sync --frozen
uv run pytest        # Ejecuta la suite completa de 77 pruebas unitarias
uv run ruff check .  # Validación estática de código
uv run ruff format --check .
docker compose config --quiet
```

### Prueba de integración end-to-end (`make smoke`)
La instrucción `make smoke` despliega un entorno transitorio en Docker para validar el sistema de extremo a extremo:
1. Crea tópicos efímeros en Kafka y reproduce un segmento representativo (65:00 a 65:20).
2. Procesa los registros con el pipeline acotado en tiempo y volumen.
3. Verifica que la suma de registros válidos en SQLite coincida de forma exacta con los eventos únicos transmitidos (2 346 registros en `docs/evidencia/smoke.txt`).
4. Valida que ninguna ventana supere el tope calculado de muestras admisibles en 5 minutos (31 lecturas; sin deduplicación alcanzaría 34).
5. Confirma que la DLQ reciba con exactitud los 59 registros malformados inyectados.

---

## 9. Trazabilidad de evidencia experimental

Todos los archivos almacenados en `docs/evidencia/` se generaron mediante ejecuciones reales de los comandos del sistema (corrida a 20× del 2026-09-24, `smoke.txt` del 2026-09-25, demo a 1× del 2026-09-25 y capturas `vm_*` del 2026-09-29):

| Archivo | Origen | Propósito y métricas verificadas |
| --- | --- | --- |
| `producer.log` | `make replay` (20×) | Bitácora de 22 019 eventos; balance de particiones 1,50× y registro de inyecciones |
| `report_antes_b07.txt` | `make show` | Estado en t = 4 min 18 s: ventana `14:30` con 14 lecturas antes de liberar `ULT1-b07` |
| `report_despues_b07.txt` | `make show` | Estado final: 740 ventanas activas y actualización de la ventana `14:30` a 29 lecturas |
| `historial_b07_b12.txt` | `scripts/report.py` | Progresión de paneles para `ULT1-b07` y actualización de 15 a 30 lecturas en `ULT1-b12` |
| `dlq.txt` | `scripts/report.py` | 134 eventos en la DLQ: 73 por omisión de temperatura y 61 por datos no numéricos |
| `flink-ui.png` | Interfaz Flink (8081) | Registro de 22 019 eventos consumidos, 21 885 agregados y 134 a la DLQ |
| `smoke.txt` | `make smoke` | Validación de consistencia: 2 346 eventos válidos, 59 en DLQ y tope n ≤ 30 respetado |
| `tiempo_real_report.txt` | Demo a 1× | Registro de 239 ventanas, 270 paneles tardíos y descarte comprobado de `ULT1-b12` |
| `tiempo_real_producer.log` | Demo a 1× | Bitácora de los 7 373 eventos transmitidos durante la prueba en tiempo real |
| `vm_replay20x_*.png` | Capturas en Debian 13 | Validación independiente de la corrida a 20× en un servidor Debian sin interfaz gráfica |
| `vm_tiempo_real_*.png` | Capturas en Debian 13 | Verificación de paneles `LATE`, descarte por latencia y estado de Flink en Linux |

### Validación independiente en Debian 13 (2026-09-29)
El repositorio se probó en una máquina virtual limpia con Debian 13 (8 vCPU, 11 GB RAM) ejecutando Docker Engine nativo. La prueba validó las 77 pruebas unitarias, el smoke test (2 346 = 2 346, DLQ = 59) y las ejecuciones a 20× (740 ventanas, DLQ 134) y 1× (239 ventanas, 270 paneles tardíos, DLQ 51), con las mismas cifras que la evidencia salvo la cantidad de ventanas fuera de rango a 20× (561 frente a 558), que depende de cómo caen las ventanas respecto del reloj.

---

## 10. Parámetros operativos y variables de entorno

| Variable | Valor por defecto | Función operativa |
| --- | :---: | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Dirección del broker Kafka (`localhost:29092` desde el anfitrión) |
| `TOPIC_RAW` | `coldchain.telemetry.raw.v1` | Tópico de telemetría de entrada |
| `TOPIC_METRICS` | `coldchain.metrics.5min.v1` | Tópico de paneles agregados de salida |
| `TOPIC_DLQ` | `coldchain.telemetry.dlq.v1` | Tópico para eventos malformados o inválidos |
| `WINDOW_SECONDS` | `300` | Tamaño de ventana fija (5 minutos) |
| `ALLOWED_LATENESS_SECONDS` | `900` | Retraso admisible tras el cierre de ventana (15 minutos) |
| `EARLY_FIRING_SECONDS` | `5` | Frecuencia de emisión de paneles anticipados |
| `BEAM_PARALLELISM` | `2` | Paralelismo de ejecución en los TaskManagers |
| `REPLAY_SPEEDUP` | `20` | Factor de aceleración en la reproducción temporal |
| `REPLAY_START_HOURS` / `END` | `65` / `68` | Segmento horario del dataset recortado para el ensayo |
| `REPLAY_ALIGN_START` | `0` | Sincroniza el inicio del replay con los bordes de ventana del reloj |
| `SINK_PATH` | `data/out/metrics.sqlite` | Ruta de persistencia de la base de datos de métricas |

**Límite del tramo de replay.** El replay acelerado adelanta `event_time` respecto del reloj en `duración del tramo × (1 − 1/REPLAY_SPEEDUP)`, y la validación rechaza eventos más de 4 h en el futuro (`FUTURE_TOLERANCE` en `contracts.py`). A 20×, un tramo de más de ~4 h 12 min (por ejemplo `REPLAY_END_HOURS=70` con el inicio por defecto) envía a la DLQ como `stale_clock` todo lo que supere ese adelanto. Para tramos más largos, bajar `REPLAY_SPEEDUP` o partir el tramo.

---

## 11. Decisiones técnicas y límites del sistema

* **Lectura clásica en KafkaIO (`use_deprecated_read`)**: El conector moderno basado en SDF (Splittable DoFn) presentaba fallas en la sincronización de puntos de control en Flink, provocando la pérdida del último lote derivado a la DLQ. El uso de la lectura clásica resolvió este problema, garantizando el vaciado íntegro de registros y puntos de control estables.
* **Estabilidad y memoria en Flink**: Para mitigar fallas por saturación de metaspace (`OutOfMemoryError: Metaspace`, código de salida 239) ante pausas del anfitrión, se elevó `heartbeat.timeout` a 180 000 ms y se estableció la directiva `restart: unless-stopped` en los TaskManagers.
* **Portabilidad en Linux y permisos de montaje**: Los contenedores ejecutan bajo el UID 1000 y cuentan con permisos de lectura explícitos sobre `/opt/coldchain-lab/src`, evitando incidencias de importación en el SDK harness de Beam bajo sistemas Linux puros.
* **Comportamiento térmico en ultracongelación**: 558 de las 740 ventanas del ensayo operaron por debajo de −80 °C (con un mínimo de −82,9 °C frente a una máxima en apertura de −76,9 °C). Esto activa la condición `out_of_range` casi exclusivamente por frío. En un despliegue operativo conviene segregar alertas de descongelamiento (`> −60 °C`) de registros analíticos de ultracongelación.
* **Filtrado de paneles vacíos**: Al cerrar lecturas acotadas, Flink emite un panel final sin registros acumulados. El pipeline descarta estas emisiones (`is_non_empty_pane`) para impedir que sobrescriban métricas consolidadas en SQLite.
* **Descarte de elementos tardíos**: Beam descarta en silencio los registros que superan el límite de latencia. Su confirmación se realiza mediante la comprobación de ventanas ausentes en el almacenamiento.

---

## 12. Estructura del repositorio

```
coldchain-streaming/
├── README.md                 # Documentación técnica y guía de ejecución
├── pyproject.toml            # Definición del proyecto, dependencias y herramientas
├── Makefile                  # Instrucciones abreviadas para despliegue y validación
├── docker-compose.yml        # Topología multicontenedor (Kafka, Flink, Beam, productor y consumidor)
├── Dockerfile                # Imagen base del entorno de ejecución
├── docker/flink/             # Configuración de clúster Flink y Dockerfile de workers
├── src/coldchain_streaming/  # Módulos del pipeline, contratos, productor y sink
├── scripts/                  # Automatizaciones de validación (smoke.py, report.py)
├── tests/                    # Batería de pruebas unitarias y de integración
├── data/                     # Volúmenes de caché, procesamiento y bases SQLite
└── docs/
    ├── evidencia/            # Salidas y capturas de corridas reproducibles
    └── pf-coldchain.pdf      # Documento técnico consolidado
```

---

## 13. Licencia de datos

El conjunto de datos de telemetría térmica pertenece a Sun et al. (2022) y se reutiliza bajo los términos de la licencia Creative Commons Attribution 4.0 International (CC BY 4.0).
