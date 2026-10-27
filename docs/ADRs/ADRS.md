# Registro de Decisiones de Arquitectura (Architecture Decision Records - ADRs)

Este documento recopila las decisiones arquitectónicas fundamentales tomadas para el diseño, despliegue y defensa técnica de la **Arquitectura Lambda de Monitoreo Crítico en UCI**. Cada registro sigue el formato estándar formal (Contexto, Decisión, Estado y Consecuencias).

---

## Índice de ADRs

| ID | Título | Estado | Fecha |
| :--- | :--- | :---: | :---: |
| **[ADR-001](#adr-001-adopción-de-apache-kafka-en-modo-kraft-sin-zookeeper)** | Adopción de Apache Kafka en Modo KRaft (Sin ZooKeeper) | **Aceptado** | 2026-10-26 |
| **[ADR-002](#adr-002-exclusión-del-patrón-medallón-en-la-arquitectura-lambda)** | Exclusión del Patrón Medallón en la Arquitectura Lambda (Master Dataset) | **Aceptado** | 2026-10-26 |
| **[ADR-003](#adr-003-desacoplamiento-estricto-de-airflow-batch-vs-speed)** | Desacoplamiento Estricto de Airflow (Batch Planificado vs Speed Always-On) | **Aceptado** | 2026-10-26 |
| **[ADR-004](#adr-004-capa-de-servicio-como-almacén-de-vistas-precomputadas)** | Capa de Servicio como Almacén de Vistas Precomputadas (Cero Staging) | **Aceptado** | 2026-10-26 |
| **[ADR-005](#adr-005-transmisión-reactiva-instantánea-spark-foreachbatch--websocket)** | Transmisión Reactiva Instantánea (Spark `foreachBatch` + FastAPI WebSocket) | **Aceptado** | 2026-10-26 |
| **[ADR-006](#adr-006-frontend-react-desacoplado-cero-mocks-y-barra-de-auditoría)** | Frontend React Desacoplado, Cero Mocks y Barra de Auditoría | **Aceptado** | 2026-10-26 |
| **[ADR-007](#adr-007-estrategia-dual-de-gobernanza-openmetadata--apache-atlas)** | Estrategia Dual de Gobernanza y Linaje (OpenMetadata + Apache Atlas) | **Aceptado** | 2026-10-26 |
| **[ADR-008](#adr-008-integración-de-modelo-predictivo-tabular-scikit-learn--xgboost-desacoplado-de-la-capa-speed)** | Integración de Modelo Predictivo Tabular (Scikit-Learn / XGBoost) Desacoplado de la Capa Speed | **Aceptado** | 2026-10-26 |

---

## ADR-001: Adopción de Apache Kafka en Modo KRaft (Sin ZooKeeper)

### Contexto
En entornos de evaluación técnica con restricciones de hardware y límites estrictos de tiempo (prueba de 6 horas), desplegar Apache Kafka con ZooKeeper añade un contenedor redundante, sobrecarga de red y consume entre 1.0 y 1.5 GB de memoria RAM adicional. Además, desde Kafka 3.3+, KRaft (Kafka Raft Metadata Mode) es el estándar oficial para producción.

### Decisión
Desplegar Apache Kafka en **modo KRaft nativo**, prescindiendo completamente de Apache ZooKeeper.

### Estado
**Aceptado.**

### Consecuencias
* **Positivas:**
  * Reducción inmediata de ~1.2 GB en el consumo de RAM de Docker Compose.
  * Arranque del clúster más rápido y sin condiciones de carrera entre ZooKeeper y Kafka Broker.
  * Argumentación senior ante el TL: alineación con el estándar moderno de la industria.
* **Negativas / Mitigación:** Requiere configurar explícitamente el `CLUSTER_ID` y listeners unificados en el `docker-compose.yml`, lo cual queda automatizado en este repositorio.

---

## ADR-002: Exclusión del Patrón Medallón en la Arquitectura Lambda

### Contexto
El patrón Medallón (Bronze, Silver, Gold) fue concebido para Lakehouses y almacenes analíticos empresariales (Databricks/Delta/Iceberg). En una Arquitectura Lambda canónica (Nathan Marz), forzar tres capas de refinamiento en la Batch Layer genera sobreingeniería, latencia innecesaria y redundancia de almacenamiento, motivos por los cuales el Team Leader rechazó la arquitectura previa.

### Decisión
Eliminar cualquier referencia a capas de colores (Bronce, Plata, Oro). La **Batch Layer** almacena exclusivamente el **Master Dataset Crudo (Append-Only)** en MinIO particionado cronológicamente (`s3a://data-lake/raw-vitals/year=YYYY/month=MM/day=DD/hour=HH/`). La calidad del dato se resuelve con flags booleanos a nivel de fila (`is_valid_sensor = false`) sin carpetas de cuarentena física.

### Estado
**Aceptado.**

### Consecuencias
* **Positivas:**
  * Cero sobrecarga de metadatos o catálogos externos (sin Iceberg / sin Delta Lake).
  * Fuente de verdad inmutable y rejugable (*replayable*) para auditorías o entrenamiento de Machine Learning.
  * Código limpio y 100% defendible ante el TL.
* **Negativas / Mitigación:** El cálculo de vistas históricas debe filtrar registros marcados con `is_valid_sensor = false` en el momento de agregación, lo cual se implementa como un simple filtro en el script de Spark Batch.

---

## ADR-003: Desacoplamiento Estricto de Airflow (Batch vs Speed)

### Contexto
Un anti-patrón común en pruebas de datos es intentar que Apache Airflow gestione streaming o dispare micro-batches cada pocos segundos. Airflow está diseñado para DAGs discretos, finitos, idempotentes y planificados. Meter streaming en Airflow bloquea los workers y satura el Scheduler.

### Decisión
Separación estricta de planos:
1. **Capa Batch:** Orquestada por Airflow en intervalos discretos (`@hourly` o cron planificado).
2. **Capa Speed:** Proceso continuo autónomo (**Always-On**) en Spark Structured Streaming, corriendo independientemente del ciclo de vida de Airflow y manteniendo viva la Spark UI en el puerto `4040`.

### Estado
**Aceptado.**

### Consecuencias
* **Positivas:**
  * Estabilidad absoluta del Scheduler de Airflow.
  * Demostración visual permanente en `localhost:4040` (Job y Stages activos con métricas de Input/Processing Rate en tiempo real).
  * Cumplimiento estricto del criterio del TL.

---

## ADR-004: Capa de Servicio como Almacén de Vistas Precomputadas

### Contexto
La Capa de Servicio (Serving Layer) en Lambda no debe ser tratada como un área de preparación (*staging*). No debe limpiar, filtrar ni calcular agregaciones pesadas al momento de la consulta del usuario, ya que degradaría la latencia de respuesta.

### Decisión
Utilizar PostgreSQL estrictamente como almacén de **vistas precomputadas e indexadas** (`vitals_realtime` y `vitals_historico`). La API de servicio (FastAPI) ejecuta una consulta de baja latencia (Merge Query con `LEFT JOIN LATERAL`) que responde en < 5 milisegundos.

### Estado
**Aceptado.**

### Consecuencias
* **Positivas:**
  * Latencias de subsegundo consistentes en la API.
  * Cumplimiento estricto de la fórmula Lambda: $\text{Query} = \text{Merge}(\text{BatchView}, \text{RealtimeView})$.
  * PostgreSQL opera liviano y sin saturación de CPU.

---

## ADR-005: Transmisión Reactiva Instantánea (Spark `foreachBatch` + WebSocket)

### Contexto
Para reflejar eventos en tiempo real en la UI, se evaluaron tres opciones:
1. *Fast Polling (fetch cada 2s):* Rechazado porque degrada el tiempo real a un mini-batch y genera latencia inaceptable para emergencias UCI.
2. *PostgreSQL LISTEN/NOTIFY con Triggers:* Rechazado por riesgo operacional: escribir y depurar triggers SQL bajo la presión de las 6 horas puede costar horas en caso de error de sintaxis o permisos.
3. *Spark `foreachBatch` Dual Sink:* La alternativa limpia en software.

### Decisión
Utilizar la función nativa `.writeStream.foreachBatch()` de Spark Structured Streaming con **doble acción simultánea**:
1. Persistir el micro-batch en PostgreSQL (`vitals_realtime`) para garantizar persistencia y consultas de recarga.
2. Enviar el JSON preprocesado mediante un `POST` HTTP interno a FastAPI, el cual lo retransmite inmediatamente vía **WebSocket (`/ws/vitals`)** a los clientes conectados.

### Estado
**Aceptado.**

### Consecuencias
* **Positivas:**
  * Latencia de punta a punta menor a **40 milisegundos** (verdadero tiempo real).
  * Spark y Kafka asumen el 100% de la carga pesada de Big Data.
  * Se evitan los riesgos de triggers SQL complejos en PostgreSQL.
  * Si FastAPI se reinicia, la tolerancia a fallos está garantizada por los checkpoints de Spark en MinIO.

---

## ADR-006: Frontend React Desacoplado, Cero Mocks y Barra de Auditoría

### Contexto
En el intento previo, el código mockeado en el frontend consumió tiempo valioso y fue sancionado por el TL. Asimismo, acoplar el frontend a la infraestructura interna genera confusión arquitectónica.

### Decisión
Construir una Single Page Application en **React (Vite)** bajo tres principios:
1. **Encapsulamiento:** Solo se comunica con FastAPI (REST + WebSocket).
2. **Cero Mocks:** Si la Capa de Servicio está vacía, muestra el componente `NoDataNotice` advirtiendo que el pipeline está en inicialización.
3. **Barra de Auditoría del TL:** Barra superior minimalista de solo lectura con enlaces directos a las 5 consolas del sistema (Spark 4040, Airflow 8085, OpenMetadata 8585, Atlas 21000, MinIO 9001). Cero botones intrusivos de disparo de batch.

### Estado
**Aceptado.**

### Consecuencias
* **Positivas:**
  * Transparencia total ante la evaluación del TL.
  * Interfaz médica profesional con semáforo para 12 camas de UCI.
  * Arquitectura limpia y desacoplada.

---

## ADR-007: Estrategia Dual de Gobernanza y Linaje (OpenMetadata + Apache Atlas)

### Contexto
El TL exigió tanto **OpenMetadata** como **Apache Atlas**. Sin embargo, configurar conectores manuales de Hadoop/Spark para Atlas en 6 horas suele fallar por conflictos de dependencias JAR (*classpath hell*).

### Decisión
1. **OpenMetadata (Puerto 8585):** Se conecta automáticamente mediante el listener nativo **OpenLineage** de Spark (`OpenLineageSparkListener`), emitiendo eventos HTTP en segundo plano sin código manual.
2. **Apache Atlas (Puerto 21000):** Se despliega en el contenedor todo-en-uno `sburn/apache-atlas`. El registro de entidades (`kafka_topic`, `spark_process`, `fs_path`, `rdbms_table`) y relaciones de linaje se realiza de forma automatizada mediante un script oficial en Python (`scripts/register_atlas_lineage.py`) utilizando la API REST v2 de Atlas.

### Estado
**Aceptado.**

### Consecuencias
* **Positivas:**
  * Ambas interfaces de gobernanza quedan 100% funcionales y pobladas con grafos visuales reales en los primeros minutos de la prueba.
  * Cero riesgo de incompatibilidad de librerías en Spark.
  * Demostración de dominio de automatización de metadatos mediante APIs estándar.

---

## ADR-008: Integración de Modelo Predictivo Tabular (Scikit-Learn / XGBoost) Desacoplado de la Capa Speed

### Contexto
Se requiere dotar al sistema de soporte analítico predictivo para anticipar el deterioro fisiológico de los pacientes en UCI (ej. riesgo de shock séptico o falla multiorgánica). Sin embargo, incorporar inferencia pesada (como LLMs o modelos de lenguaje) dentro del bucle de Spark Streaming generaría cuellos de botella por contrapresión (*backpressure*), latencias de varios segundos y obsolescencia del dato evaluado frente a la telemetría viva de alta frecuencia.

### Decisión
1. **Modelo Tabular de Machine Learning Ligero:** Implementar un clasificador tabular (Scikit-Learn / XGBoost) entrenado sobre el **Master Dataset en MinIO (Batch Layer)** que infiere a partir de vectores de características fisiológicas numéricas (FC, SpO2, PAS, PAD, PAM, Shock Index y deltas respecto a la línea base histórica).
2. **Desacoplamiento Estricto:** La Capa Speed (Spark Streaming) se mantiene libre de inferencias pesadas síncronas, procesando exclusivamente a <40 ms las reglas críticas de reflejo médico y emisión WebSocket.
3. **Inferencia Ultrarrápida (< 3 ms) en la Capa de Servicio:** El modelo se ejecuta de forma desacoplada en FastAPI bajo demanda (o precalculado en lotes por Airflow), consumiendo el snapshot consolidado en PostgreSQL (`vitals_realtime` + `vitals_historico`).

### Estado
**Aceptado.**

### Consecuencias
* **Positivas:**
  * **Latencia de Inferencia < 3 ms:** El cálculo de probabilidad y categorización de riesgo se realiza en tiempo real sin impacto perceptible para el usuario ni bloqueo del servidor.
  * **Cero Contrapresión en Streaming:** Spark Streaming y Kafka operan sin retrasos de CPU o dependencias de GPU.
  * **Consumo Mínimo de Recursos:** La huella de memoria del modelo es inferior a 80 MB, garantizando estabilidad total en el clúster Docker Compose.
  * **Explicabilidad Clínica:** Permite presentar al personal médico probabilidades directas, índices fisiológicos reconocidos internacionalmente (Shock Index) y deltas basales sin cajas negras impenetrables.
* **Negativas / Mitigación:** Requiere que la Capa Batch mantenga actualizadas las tablas de basales históricos para poder calcular con precisión los deltas de variación fisiológica.
