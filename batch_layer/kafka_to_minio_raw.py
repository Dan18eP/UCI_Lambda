"""
Capa Batch - Tarea 1: Volcado Inmutable de Kafka a MinIO (Master Dataset Append-Only).
Arquitectura Lambda - Monitoreo Crítico UCI.
Cumple: ADR-002 (Cero Medallón, Parquet Append-Only particionado por fecha/hora).
"""

import os
import sys
import json
import logging
import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [BATCH-INGEST] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("batch_ingest")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "vitals-stream")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "uci-batch-dumper-group")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "data-lake")
MAX_POLL_RECORDS = int(os.getenv("MAX_POLL_RECORDS", "1000"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "5000"))


def get_s3_filesystem():
    """Configura el cliente S3FS apuntando a MinIO."""
    return s3fs.S3FileSystem(
        key=MINIO_USER,
        secret=MINIO_PASSWORD,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT}
    )


def dump_stream_to_master_dataset():
    """Consume mensajes de Kafka y los almacena en MinIO en formato Parquet inmutable."""
    logger.info(f"Conectando a Kafka ({KAFKA_BOOTSTRAP_SERVERS}) para volcado Batch...")
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",")],
            group_id=KAFKA_CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=POLL_TIMEOUT_MS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
    except NoBrokersAvailable:
        logger.error("No se pudo conectar a Kafka. Verifique que el servicio esté arriba.")
        sys.exit(1)

    records = []
    for msg in consumer:
        records.append(msg.value)
        if len(records) >= MAX_POLL_RECORDS:
            break

    consumer.close()

    if not records:
        logger.info("ℹ️ No hay nuevos eventos en Kafka para volcar al Master Dataset.")
        return

    logger.info(f"📥 {len(records)} eventos consumidos. Preparando volcado a MinIO...")

    # Evaluación de Calidad en Memoria (Banderas booleanas sin carpetas de cuarentena)
    for r in records:
        hr = float(r.get("heart_rate_bpm", 0.0))
        spo2 = float(r.get("spo2_percent", 0.0))
        sbp = float(r.get("systolic_bp", 0.0))
        r["is_valid_sensor"] = bool(
            (hr > 0.0 and hr <= 280.0) and
            (spo2 > 0.0 and spo2 <= 100.0) and
            (sbp >= 20.0 and sbp <= 300.0)
        )

    # Conversión a PyArrow Table
    table = pa.Table.from_pylist(records)

    # Particionado cronológico estricto (ADR-002)
    now = datetime.datetime.now(datetime.timezone.utc)
    partition_path = f"{S3_BUCKET}/raw-vitals/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    file_id = now.strftime("%Y%m%d_%H%M%S")
    file_path = f"{partition_path}/vitals_{file_id}.parquet"

    fs = get_s3_filesystem()
    try:
        with fs.open(file_path, "wb") as f:
            pq.write_table(table, f, compression="snappy")
        logger.info(f"✅ Master Dataset actualizado con éxito (Append-Only): s3a://{file_path}")
    except Exception as e:
        logger.error(f"❌ Error al escribir en MinIO: {e}")
        sys.exit(1)


if __name__ == "__main__":
    dump_stream_to_master_dataset()
