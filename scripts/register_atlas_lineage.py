"""
Script Oficial de Registro de Linaje y Gobernanza en Apache Atlas (API REST v2).
Cumple: ADR-007 & DATA_LINEAGE.md (Automatización vía API REST v2 sin classpath hell).
Puerto Atlas: 21000.
"""

import os
import sys
import time
import json
import logging
import requests
from requests.auth import HTTPBasicAuth

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [ATLAS-LINEAGE] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("atlas_lineage")

ATLAS_URL = os.getenv("ATLAS_URL", "http://localhost:21000/api/atlas/v2")
ATLAS_USER = os.getenv("ATLAS_USER", "admin")
ATLAS_PASSWORD = os.getenv("ATLAS_PASSWORD", "admin")


def wait_for_atlas(max_retries: int = 30, delay_sec: int = 5):
    """Espera a que Apache Atlas esté disponible."""
    health_url = f"{ATLAS_URL}/types/typedefs/headers"
    logger.info(f"Comprobando disponibilidad de Apache Atlas en {ATLAS_URL}...")

    for i in range(1, max_retries + 1):
        try:
            res = requests.get(health_url, auth=HTTPBasicAuth(ATLAS_USER, ATLAS_PASSWORD), timeout=5)
            if res.status_code in [200, 204]:
                logger.info(f"✅ Apache Atlas está activo y respondiendo (intento {i}).")
                return True
        except Exception:
            pass
        logger.info(f"Esperando inicio de Atlas (intento {i}/{max_retries})...")
        time.sleep(delay_sec)

    logger.warning("Atlas no respondió dentro del tiempo límite. El script puede reejecutarse cuando Atlas esté listo.")
    return False


def build_atlas_entities():
    """Construye las entidades de la Arquitectura Lambda para el grafo de linaje."""
    return {
        "entities": [
            # 1. Ingesta: Kafka Topic
            {
                "typeName": "kafka_topic",
                "attributes": {
                    "qualifiedName": "vitals-stream@uci-kafka-cluster",
                    "name": "vitals-stream",
                    "description": "Topic de telemetría de signos vitales de 12 camas UCI (Modo KRaft)",
                    "topic": "vitals-stream",
                    "uri": "kafka://kafka:9092/vitals-stream"
                }
            },
            # 2. Capa Speed: Spark Structured Streaming
            {
                "typeName": "spark_process",
                "attributes": {
                    "qualifiedName": "spark_speed_processor@uci-spark",
                    "name": "Spark Structured Streaming: Speed Layer (24/7)",
                    "description": "Procesamiento de ventanas en memoria, evaluación de umbrales clínicos y WebSocket",
                    "inputs": [
                        {"typeName": "kafka_topic", "uniqueAttributes": {"qualifiedName": "vitals-stream@uci-kafka-cluster"}}
                    ],
                    "outputs": [
                        {"typeName": "rdbms_table", "uniqueAttributes": {"qualifiedName": "serving_db.vitals_realtime@postgres-serving"}}
                    ]
                }
            },
            # 3. Serving Layer: Tabla Realtime
            {
                "typeName": "rdbms_table",
                "attributes": {
                    "qualifiedName": "serving_db.vitals_realtime@postgres-serving",
                    "name": "vitals_realtime",
                    "description": "Vista precomputada del último estado vivo y alertas activas por cama UCI"
                }
            },
            # 4. Capa Batch - Tarea 1: Volcado a MinIO
            {
                "typeName": "spark_process",
                "attributes": {
                    "qualifiedName": "airflow_dump_kafka_to_minio@uci-airflow",
                    "name": "Airflow DAG: dump_kafka_to_minio_raw",
                    "description": "Volcado horario append-only desde Kafka hacia el Master Dataset de MinIO",
                    "inputs": [
                        {"typeName": "kafka_topic", "uniqueAttributes": {"qualifiedName": "vitals-stream@uci-kafka-cluster"}}
                    ],
                    "outputs": [
                        {"typeName": "fs_path", "uniqueAttributes": {"qualifiedName": "s3a://data-lake/raw-vitals/@minio"}}
                    ]
                }
            },
            # 5. Master Dataset: MinIO S3 Parquet
            {
                "typeName": "fs_path",
                "attributes": {
                    "qualifiedName": "s3a://data-lake/raw-vitals/@minio",
                    "name": "s3a://data-lake/raw-vitals/",
                    "path": "s3a://data-lake/raw-vitals/year=YYYY/month=MM/day=DD/hour=HH/",
                    "description": "Master Dataset crudo e inmutable (Append-Only) en formato Parquet"
                }
            },
            # 6. Capa Batch - Tarea 2: Cálculo Histórico
            {
                "typeName": "spark_process",
                "attributes": {
                    "qualifiedName": "airflow_compute_historical_vitals@uci-airflow",
                    "name": "Airflow DAG: compute_historical_vitals",
                    "description": "Cálculo de basales fisiológicos e inserción idempotente con .mode('overwrite')",
                    "inputs": [
                        {"typeName": "fs_path", "uniqueAttributes": {"qualifiedName": "s3a://data-lake/raw-vitals/@minio"}}
                    ],
                    "outputs": [
                        {"typeName": "rdbms_table", "uniqueAttributes": {"qualifiedName": "serving_db.vitals_historico@postgres-serving"}}
                    ]
                }
            },
            # 7. Serving Layer: Tabla Histórico
            {
                "typeName": "rdbms_table",
                "attributes": {
                    "qualifiedName": "serving_db.vitals_historico@postgres-serving",
                    "name": "vitals_historico",
                    "description": "Vista precomputada de promedios móviles, basales y estadísticas por paciente y hora"
                }
            },
            # 8. Modelo Predictivo Tabular ML (<3ms)
            {
                "typeName": "spark_process",
                "attributes": {
                    "qualifiedName": "clinical_tabular_ml_model@fastapi",
                    "name": "FastAPI: Modelo Tabular ML (<3ms)",
                    "description": "Evaluación de riesgo de deterioro (Shock Index, PAM, Delta FC) sobre la Serving Layer",
                    "inputs": [
                        {"typeName": "rdbms_table", "uniqueAttributes": {"qualifiedName": "serving_db.vitals_realtime@postgres-serving"}},
                        {"typeName": "rdbms_table", "uniqueAttributes": {"qualifiedName": "serving_db.vitals_historico@postgres-serving"}}
                    ],
                    "outputs": [
                        {"typeName": "rdbms_table", "uniqueAttributes": {"qualifiedName": "serving_db.clinical_predictions@postgres-serving"}}
                    ]
                }
            },
            # 9. Serving Layer: Tabla Predicciones Clínicas
            {
                "typeName": "rdbms_table",
                "attributes": {
                    "qualifiedName": "serving_db.clinical_predictions@postgres-serving",
                    "name": "clinical_predictions",
                    "description": "Registro de inferencias y scores de deterioro para trazabilidad médico-legal"
                }
            }
        ]
    }


def register_entities_in_atlas():
    """Registra entidades y linaje en lote mediante la API REST v2 de Atlas."""
    if not wait_for_atlas():
        logger.warning("Abortando registro de entidades por indisponibilidad de Atlas.")
        return

    payload = build_atlas_entities()
    url = f"{ATLAS_URL}/entity/bulk"

    try:
        res = requests.post(
            url,
            auth=HTTPBasicAuth(ATLAS_USER, ATLAS_PASSWORD),
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=15
        )

        if res.status_code in [200, 201]:
            logger.info("✅ Linaje y entidades registradas exitosamente en Apache Atlas:")
            data = res.json()
            created = data.get("mutatedEntities", {}).get("CREATE", [])
            updated = data.get("mutatedEntities", {}).get("UPDATE", [])
            logger.info(f"   • Entidades creadas: {len(created)}")
            logger.info(f"   • Entidades actualizadas: {len(updated)}")
            logger.info(f"🌐 Visualice el grafo en: http://localhost:21000")
        else:
            logger.error(f"Error registrando en Atlas (Status {res.status_code}): {res.text}")
    except Exception as e:
        logger.error(f"Error de conexión al registrar linaje en Atlas: {e}")


if __name__ == "__main__":
    register_entities_in_atlas()
