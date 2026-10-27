"""
Airflow DAG: Orquestación de la Capa Batch (Arquitectura Lambda - UCI).
Cumple: ADR-002 (Master Dataset Append-Only en MinIO),
        ADR-003 (Airflow orquesta exclusivamente procesos Batch discretos).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "uci_data_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="uci_batch_pipeline_dag",
    default_args=default_args,
    description="Pipeline horario de volcado a MinIO (Master Dataset) y cálculo de vistas históricas.",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["uci", "lambda", "batch", "minio", "postgres"]
) as dag:

    # Tarea 1: Volcado crudo desde Kafka hacia MinIO (Master Dataset Append-Only)
    dump_kafka_to_minio_raw = BashOperator(
        task_id="dump_kafka_to_minio_raw",
        bash_command="python /opt/airflow/batch_layer/kafka_to_minio_raw.py",
        env={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "KAFKA_TOPIC": "vitals-stream",
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ROOT_USER": "minioadmin",
            "MINIO_ROOT_PASSWORD": "minioadmin",
            "S3_BUCKET": "data-lake",
        }
    )

    # Tarea 2: Cálculo de basales fisiológicos y actualización idempotente de la Serving Layer
    compute_historical_vitals = BashOperator(
        task_id="compute_historical_vitals",
        bash_command="python /opt/airflow/batch_layer/compute_historical_vitals.py",
        env={
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ROOT_USER": "minioadmin",
            "MINIO_ROOT_PASSWORD": "minioadmin",
            "S3_BUCKET": "data-lake",
            "POSTGRES_HOST": "postgres-serving",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "serving_db",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres_password",
        }
    )

    # Tarea 3: Emisión de linaje y metadatos hacia Apache Atlas y OpenMetadata (ADR-007)
    register_governance_lineage = BashOperator(
        task_id="register_governance_lineage",
        bash_command="python /opt/airflow/scripts/register_atlas_lineage.py",
        env={
            "ATLAS_URL": "http://apache-atlas:21000/api/atlas/v2",
            "ATLAS_USER": "admin",
            "ATLAS_PASSWORD": "admin"
        }
    )

    # Orden de ejecución estricto: Ingesta Inmutable -> Agregación Analítica -> Linaje & Gobernanza
    dump_kafka_to_minio_raw >> compute_historical_vitals >> register_governance_lineage
