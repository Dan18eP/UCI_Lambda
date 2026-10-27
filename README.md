# Blueprint Arquitectura Lambda: Monitoreo Crítico en UCI (Tiempo Real + Histórico)

Guía técnica y de despliegue para la implementación estricta de una **Arquitectura Lambda** gobernada para el monitoreo de pacientes en UCI, utilizando **Apache Kafka (KRaft)**, **MinIO (Master Dataset Append-Only)**, **Apache Spark**, **Apache Airflow**, **PostgreSQL (Serving)**, **FastAPI (WebSockets + REST)** y **Frontend Cero Mocks**.

>  **Documentación Especializada:**
> * [ADRS.md](docs/ADRs/ADRS.md) - Registro formal de decisiones de arquitectura (ADRs 001 al 008).
> * [RULES.md](docs/RULES.md) - Reglas inmutables de arquitectura, código, gobernanza e infraestructura.
> * [DATA_LINEAGE.md](docs/DATA_LINEAGE.md) - Mapa detallado de trazabilidad, contratos de interfaces y ciclo de vida de punta a punta.
> * [BUSINESS_LOGIC.md](docs/BUSINESS_LOGIC.md) - Especificación del dominio clínico UCI, reglas fisiológicas, umbrales de alerta y validación de hardware.

---

## 1. Reglas de Oro Arquitectónicas (Criterio Estricto del TL)

1. **CERO Patrón Medallón en Lambda:**
   * **No existen capas de colores (Bronce, Plata, Oro).** Medallón es para Data Lakehouses analíticos pesados de BI.
   * En Lambda pura, la **Batch Layer almacena el Master Dataset Crudo e Inmutable (Append-Only)** en MinIO, particionado por tiempo (`year=/month=/day=/hour=`). Su fin es servir como la fuente única de verdad inmutable para auditoría, reconstrucción de estado y entrenamiento de modelos de Machine Learning.
2. **WebSocket Nativo para Alertas Críticas (Cero Polling Ineficiente):**
   * En monitoreo médico de UCI, un retraso de 3 segundos por polling (`setInterval`) es inaceptable.
   * FastAPI expone un canal **WebSocket (`/ws/alerts`)** de conexión abierta persistente que envía notificaciones push instantáneas al frontend en cuanto la Capa Speed detecta un umbral crítico (arritmia, hipoxia, caída de presión).
3. **Kafka en Modo KRaft (Sin ZooKeeper):**
   * Despliegue optimizado en recursos: ahorra 1.5 GB de RAM, elimina contenedores redundantes y acelera el arranque sin dependencias cruzadas de red.
4. **Desacoplamiento Estricto de Airflow:**
   * **Capa Batch:** Orquestada 100% por Airflow en intervalos planificados (cron horario) para volcar Kafka a MinIO y actualizar vistas analíticas.
   * **Capa Speed:** Proceso continuo autónomo (**Always-On**) en Spark Structured Streaming. **Airflow NUNCA orquesta streaming.**
5. **Cero Mocks en Frontend:**
   * El frontend solo consume la Capa de Servicio (vía WebSocket y REST).
   * Si el pipeline no ha recibido eventos o la base está vacía, se muestra un **mensaje de advertencia arquitectónico explícito**, jamás datos simulados.

---

## 2. Linaje del Dato (Data Lineage) y Flujo de Información

```
                                  ┌──► [CAPA SPEED: Spark Streaming 24/7] ──────────► [PostgreSQL: vitals_realtime] ──┐
                                  │    (Ventanas en memoria: detección umbrales)                                       │
                                  │                                                                                    ▼
[Monitores UCI / Simulador Real] ─┴──► [Apache Kafka: topic 'vitals-stream' (KRaft)]                             [FastAPI: REST + WebSocket] ──► [Frontend UCI]
                                  │                                                                                    ▲
                                  │                                                                                    │
                                  └──► [CAPA BATCH: Airflow DAG Horario]                                               │
                                       ├─► 1. Volcado Inmutable: Kafka ──► MinIO Parquet (Master Dataset Crudo)        │
                                       └─► 2. Vista Histórica: MinIO ──► [PostgreSQL: vitals_historico (Overwrite)] ───┘
```

### Rutas en MinIO (Master Dataset):
* `s3a://data-lake/raw-vitals/year=YYYY/month=MM/day=DD/hour=HH/` (Parquet crudo, inmutable y append-only).
* `s3a://data-lake/checkpoints/speed_vitals/` (Offset y control de Spark Streaming para tolerancia a fallos).

---

## 3. Esquema de Datos y Capa de Servicio (PostgreSQL)

### A. Payload del Evento en Kafka (`vitals-stream`)
```json
{
  "event_id": "a5d89f21-7e8c-4a11-b0e2-9f3b890a1234",
  "clinic_id": "CLI-CENTRAL-01",
  "patient_id": "PAT-1082",
  "bed_id": "UCI-BED-04",
  "timestamp": "2026-10-26T21:15:30.120Z",
  "heart_rate_bpm": 135.0,
  "spo2_percent": 88.5,
  "systolic_bp": 85.0,
  "diastolic_bp": 55.0,
  "respiratory_rate": 22.0,
  "device_id": "MON-PHILIPS-09"
}
```

### B. Tablas en Capa de Servicio
* **`vitals_realtime`:** Último estado y alertas activas por paciente (actualizada por la Speed Layer en milisegundos).
* **`vitals_historico`:** Promedios y muestras acumuladas por hora y paciente (sobrescrita por la Batch Layer con `.mode("overwrite")` para garantizar idempotencia).

---

## 4. Mapa de Infraestructura Docker

| Servicio | Contenedor | Puerto Local | Propósito de Auditoría |
| :--- | :--- | :--- | :--- |
| **Apache Kafka** | `kafka:9092` | `9092` / `29092` | Ingesta de signos vitales en tiempo real (**Modo KRaft**). |
| **MinIO Console** | `minio:9001` | `9001` (`9000` S3 API) | Almacenamiento Master Dataset (Parquet append-only). |
| **Spark Driver / UI** | `spark-driver` | `4040` | **Prueba visual en vivo:** Gráficos de Input Rate y Processing Rate. |
| **Spark Master UI** | `spark-master` | `8080` | Cluster y aplicaciones registradas. |
| **PostgreSQL** | `postgres-serving` | `5432` | Capa de Servicio (tablas `vitals_realtime` y `vitals_historico`). |
| **Apache Airflow** | `airflow-webserver`| `8085` | Orquestador de la capa Batch (volcado horario). |
| **Capa Servicio / API**| `fastapi-backend` | `8000` | Endpoints REST (`/api/v1/dashboard`, `/api/v1/clinical-assistant`) y **WebSocket (`/ws/alerts`)**. |
| **Frontend UCI** | `frontend-ui` | `3000` | Monitor médico en vivo (cero mocks). |

---

## 5. Guía de Defensa ante Preguntas del Team Leader (TL)

* **¿Por qué NO implementaste el patrón Medallón (Bronze/Silver/Gold)?**
  > *"Medallón es un patrón de arquitectura Lakehouse pensado para almacenes analíticos y data warehouses empresariales. En una Arquitectura Lambda canónica, la capa Batch tiene una responsabilidad distinta: almacenar el **Master Dataset inmutable y en crudo (Append-Only)** en MinIO, y precomputar vistas por lotes. Introducir Bronze/Silver/Gold aquí sería sobreingeniería redundante que agregaría latencia y complejidad innecesaria."*

* **¿Por qué usas WebSockets en lugar de HTTP Polling?**
  > *"En un escenario clínico crítico como una UCI, un paciente que entra en fibrilación o parada cardiorrespiratoria no puede esperar a que el cliente web haga una petición HTTP cada 5 segundos. Un canal WebSocket abierto proporciona comunicación full-duplex y permite que el backend empuje la alerta al navegador del médico de manera reactiva en menos de 100 milisegundos."*

* **¿Por qué tu Kafka no tiene ZooKeeper?**
  > *"Desplegué Kafka utilizando KRaft (Kafka Raft) en lugar de ZooKeeper por dos razones arquitectónicas: primero, para optimizar los recursos de hardware eliminando un contenedor redundante y ahorrando más de 1 GB de RAM; y segundo, porque KRaft es el estándar moderno de producción que consolida la metadata en el propio clúster de Kafka."*

* **¿Por qué Airflow no orquesta la capa Speed?**
  > *"Airflow está diseñado para workflows discretos, finitos e idempotentes bajo intervalos planificados. La capa Speed requiere procesamiento continuo de streaming 24/7 (Always-On). Poner streaming en Airflow bloquearía sus workers y saturaría el Scheduler. La capa Speed corre nativa y autónoma en Spark Structured Streaming."*

* **¿Cómo garantizas la Idempotencia?**
  > *"La capa Batch en Spark utiliza estrictamente `.mode('overwrite')` sobre las particiones de la tabla histórica. Si un DAG de Airflow falla y se reejecuta, el resultado final es idéntico sin generar duplicados."*
