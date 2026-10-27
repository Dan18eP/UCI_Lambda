# 🗺️ Linaje del Dato (Data Lineage) - Arquitectura Lambda UCI

Documento técnico de trazabilidad, linaje de datos de extremo a extremo, contratos de interfaces y ciclo de vida de la información para el **Sistema de Monitoreo Crítico de Pacientes en UCI**.

---

## 🧭 1. Diagrama Global de Linaje (End-to-End Lineage)

```mermaid
flowchart TD
    subgraph S0["0. FUENTE & GENERACIÓN"]
        MON["Monitores UCI (12 Camas: UCI-BED-01..12)<br/>• Sensores de Signos Vitales<br/>• Protocolo: JSON Streaming"]
    end

    subgraph S1["1. INGESTA INMUTABLE"]
        KAFKA["Apache Kafka (Modo KRaft)<br/>Topic: vitals-stream<br/>• Particionado por patient_id<br/>• Commit log inmutable"]
    end

    subgraph S2_SPEED["2. CAPA SPEED (Tiempo Real Always-On)"]
        SPARK_STR["Spark Structured Streaming 24/7<br/>• Consumo streaming directo de Kafka<br/>• Ventana en memoria (1 minuto)<br/>• Detección de umbrales críticos<br/>• foreachBatch Dual Action:<br/>  1. JDBC Write ➔ PostgreSQL<br/>  2. HTTP Push ➔ FastAPI Broadcast<br/>• Checkpoint en MinIO (Tolerancia a fallos)"]
    end

    subgraph S2_BATCH["2. CAPA BATCH (Planificada & Idempotente)"]
        AIRFLOW["Apache Airflow (Scheduler)<br/>DAG: uci_batch_pipeline"]
        TASK_DUMP["Task 1: Volcado Inmutable<br/>Kafka ➔ MinIO Parquet"]
        MINIO_RAW["MinIO: Master Dataset<br/>s3a://data-lake/raw-vitals/<br/>year=YYYY/month=MM/day=DD/hour=HH/"]
        TASK_SPARK["Task 2: Cálculo Histórico<br/>Spark Batch Job"]
    end

    subgraph S3_SERVING["3. CAPA DE SERVICIO (PostgreSQL)"]
        PG_REALTIME["Tabla: vitals_realtime<br/>• Alertas instantáneas y último pulso<br/>• Latencia: &lt; 500 ms"]
        PG_HIST["Tabla: vitals_historico<br/>• Promedios y basales horarios<br/>• Modo: Overwrite (Idempotente)"]
    end

    subgraph S4_CONSUMO["4. CAPA DE CONSUMO & PRESENTACIÓN"]
        FASTAPI["FastAPI Gateway (Puerto 8000)<br/>• POST /api/v1/internal/stream-event (Receptor Spark)<br/>• WebSocket: /ws/vitals (Push reactivo &lt; 40ms)<br/>• REST: /api/v1/dashboard (The Merge Query)"]
        UI["Frontend React SPA (Puerto 3000)<br/>• Cuadrícula 12 Camas en vivo<br/>• Cero mocks (NoDataNotice)<br/>• Barra Auditoría TL (4040, 8085, 8585, 21000)"]
    end

    subgraph S5_GOBIERNO["5. GOBERNANZA & AUDITORÍA"]
        OM["OpenMetadata / OpenLineage (Puerto 8585)<br/>• Registro automático de datasets y jobs"]
        ATLAS["Apache Atlas (Puerto 21000)<br/>• Registro REST v2 de entidades y linaje"]
    end

    %% Conexiones Ingesta
    MON -->|POST / JSON| KAFKA

    %% Bifurcación Lambda
    KAFKA -->|Stream continuo (latest)| SPARK_STR
    KAFKA -->|Batch extract (offset range)| TASK_DUMP

    %% Speed Path (Dual Sink)
    SPARK_STR -->|1. JDBC Upsert| PG_REALTIME
    SPARK_STR -->|2. HTTP Event Push| FASTAPI
    SPARK_STR -.->|EventLog & OpenLineage| OM
    SPARK_STR -.->|Metadata REST v2| ATLAS

    %% Batch Path
    AIRFLOW --> TASK_DUMP
    TASK_DUMP -->|Append-Only Parquet| MINIO_RAW
    MINIO_RAW --> TASK_SPARK
    TASK_SPARK -->|JDBC Overwrite| PG_HIST
    TASK_SPARK -.->|OpenLineage Event| OM
    TASK_SPARK -.->|Metadata REST v2| ATLAS

    %% Serving to Gateway (Merge)
    PG_REALTIME -->|Query / Initial Load| FASTAPI
    PG_HIST -->|Query analítica baseline| FASTAPI

    %% Gateway to Frontend
    FASTAPI -->|WebSocket Push &lt; 40ms| UI
    FASTAPI -->|HTTP REST Dashboard| UI
```

---

## 📊 2. Matriz Detallada de Trazabilidad del Dato

| Etapa del Linaje | Entidad Origen | Entidad Destino | Transformación Aplicada | Modo de Escritura | Frecuencia / Latencia |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **0. Ingesta** | Sensor Monitores UCI | Kafka Topic `vitals-stream` | Serialización a JSON crudo. Validación de estructura mínima. | Append-Only (Log) | Streaming continuo (~100 ms) |
| **1. Speed Layer** | Kafka `vitals-stream` | PostgreSQL `vitals_realtime` | Filtrado en memoria, detección de umbrales clínicos (taquicardia, hipoxia, shock). | Upsert / Update por `patient_id` | Streaming continuo (< 500 ms) |
| **2. Batch Ingest** | Kafka `vitals-stream` | MinIO `raw-vitals/` | Deserialización de batch horario y volcado directo a Parquet particionado. | Append-Only | Programado (Cada 1 hora vía Airflow) |
| **3. Batch Compute** | MinIO `raw-vitals/` | PostgreSQL `vitals_historico` | Agregaciones estadísticas: `avg()`, `min()`, `max()`, `count()`, detección de fallas de sensor. | **`mode("overwrite")`** (Idempotente) | Programado (Cada 1 hora vía Spark Batch) |
| **4. Serving Realtime** | PostgreSQL `vitals_realtime` | FastAPI `/ws/alerts` | Lectura de estado activo y emisión push por WebSocket. | Read-Only | Reactivo push (< 50 ms) |
| **5. Serving Histórico** | PostgreSQL `vitals_historico` | FastAPI `/api/v1/patients/{id}` | Merge de basales históricos con el estado en vivo. | Read-Only | Bajo demanda HTTP (< 20 ms) |

---

## 🗃️ 3. Contratos de Datos y Esquemas por Capa

### A. Capa de Ingesta (Kafka Payload)
* **Topic:** `vitals-stream`
* **Particionado:** Clave `patient_id` (garantiza orden secuencial por paciente).
* **Formato:** JSON UTF-8.

```json
{
  "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "patient_id": "PAT-1082",
  "bed_id": "UCI-BED-04",
  "timestamp": "2026-10-26T21:15:30.120Z",
  "heart_rate_bpm": 135.0,
  "spo2_percent": 88.5,
  "systolic_bp": 85.0,
  "diastolic_bp": 55.0,
  "respiratory_rate": 24.0,
  "device_id": "MON-PHILIPS-09"
}
```

---

### B. Capa Batch: Master Dataset en MinIO (Lakehouse Inmutable)
* **Bucket:** `s3a://data-lake/`
* **Ruta de particionado estricto:**  
  `s3a://data-lake/raw-vitals/year=YYYY/month=MM/day=DD/hour=HH/`
* **Formato:** Apache Parquet (Snappy compression).
* **Propósito:** Almacén maestro crudo, inmutable y auditable (fuente de verdad permanente). Cero sobrecarga de capas intermedias.

---

### C. Capa de Servicio: Tablas en PostgreSQL (`serving_db`)

#### 1. Tabla de Tiempo Real (`vitals_realtime`)
Alimentada exclusivamente por la **Capa Speed**:
```sql
CREATE TABLE IF NOT EXISTS vitals_realtime (
    patient_id VARCHAR(50) PRIMARY KEY,
    bed_id VARCHAR(50) NOT NULL,
    window_end TIMESTAMP NOT NULL,
    current_heart_rate NUMERIC(5,2),
    current_spo2 NUMERIC(5,2),
    current_systolic_bp NUMERIC(5,2),
    current_diastolic_bp NUMERIC(5,2),
    current_respiratory_rate NUMERIC(5,2),
    is_critical_alert BOOLEAN DEFAULT FALSE,
    alert_reason VARCHAR(255),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_vitals_realtime_alert ON vitals_realtime(is_critical_alert);
```

#### 2. Tabla Histórica Consolidada (`vitals_historico`)
Alimentada exclusivamente por la **Capa Batch** en modo sobreescritura:
```sql
CREATE TABLE IF NOT EXISTS vitals_historico (
    patient_id VARCHAR(50),
    window_hour TIMESTAMP,
    avg_heart_rate NUMERIC(5,2),
    min_heart_rate NUMERIC(5,2),
    max_heart_rate NUMERIC(5,2),
    avg_spo2 NUMERIC(5,2),
    min_spo2 NUMERIC(5,2),
    avg_systolic_bp NUMERIC(5,2),
    total_samples INT,
    sensor_errors_detected INT,
    PRIMARY KEY (patient_id, window_hour)
);
CREATE INDEX idx_vitals_historico_patient ON vitals_historico(patient_id);
```

---

## 🛡️ 4. Tolerancia a Fallos y Garantías de Entrega

1. **Garantía en Speed Layer (At-Least-Once / Effectively-Once):**
   * Spark Structured Streaming utiliza checkpoints en MinIO (`s3a://data-lake/checkpoints/speed_vitals/`).
   * Si el contenedor de Spark Driver se reinicia, reanuda desde el último offset confirmado de Kafka sin perder datos.
   * La escritura en PostgreSQL sobre `patient_id` como Primary Key actúa como un `upsert` idempotente.

2. **Garantía en Batch Layer (Exactly-Once Semantics vía Idempotencia):**
   * Cada ejecución del DAG en Airflow procesa una partición horaria definida (`execution_date`).
   * La escritura del job de Spark hacia PostgreSQL utiliza `.mode("overwrite")` filtrando por la clave particionada.
   * Si el DAG se reintenta 3 veces por fallas temporales de red, el estado final en la Capa de Servicio es exactamente el mismo, sin duplicaciones.

---

## 🔍 5. Trazabilidad con OpenMetadata / OpenLineage

Para auditar el linaje frente al TL:
1. El listener `io.openlineage.spark.agent.OpenLineageSparkListener` intercepta los planes lógicos de ejecución en Spark.
2. Emite eventos con el esquema de entrada (`kafka://kafka:9092/vitals-stream`), las transformaciones intermedias y el esquema de salida (`postgresql://postgres-serving:5432/serving_db.vitals_realtime`).
3. En la UI de OpenMetadata (`http://localhost:8585`), se visualiza el grafo de dependencias de punta a punta.
