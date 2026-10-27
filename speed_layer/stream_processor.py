"""
Capa Speed: Procesamiento de Telemetría en Tiempo Real con Spark Structured Streaming.
Arquitectura Lambda - Monitoreo Crítico UCI.
Cumple: ADR-003 (Always-On 24/7), ADR-005 (foreachBatch + WebSockets), RULES.md (Cero Cuarentena).
"""

import os
import sys
import json
import logging
import requests
import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, expr, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [SPARK-SPEED] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("speed_layer")

# Variables de Configuración
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "vitals-stream")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres-serving")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "serving_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_password")
FASTAPI_INTERNAL_URL = os.getenv("FASTAPI_INTERNAL_URL", "http://fastapi-backend:8000/api/v1/internal/vitals-stream")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "s3a://data-lake/checkpoints/speed_vitals/")

# Esquema Formal del Evento de Signos Vitales
VITALS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("clinic_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("bed_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("heart_rate_bpm", DoubleType(), False),
    StructField("spo2_percent", DoubleType(), False),
    StructField("systolic_bp", DoubleType(), False),
    StructField("diastolic_bp", DoubleType(), False),
    StructField("respiratory_rate", DoubleType(), False),
    StructField("device_id", StringType(), False),
])


def get_postgres_connection():
    """Crea una conexión con la Capa de Servicio (PostgreSQL)."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def process_micro_batch(batch_df, batch_id):
    """
    Función de doble acción simultánea por micro-batch (ADR-005):
    1. Persiste el estado más reciente en PostgreSQL (vitals_realtime) con UPSERT.
    2. Envía notificación push instantánea a FastAPI vía HTTP interno para retransmisión por WebSocket.
    """
    records = batch_df.collect()
    if not records:
        return

    logger.info(f" Procesando micro-batch {batch_id} con {len(records)} eventos.")

    # 1. Preparar datos para PostgreSQL
    upsert_query = """
    INSERT INTO vitals_realtime (
        patient_id, clinic_id, bed_id, timestamp, heart_rate_bpm,
        spo2_percent, systolic_bp, diastolic_bp, respiratory_rate,
        device_id, is_valid_sensor, is_critical_alert, alert_reason, updated_at
    ) VALUES (
        %(patient_id)s, %(clinic_id)s, %(bed_id)s, %(timestamp)s, %(heart_rate_bpm)s,
        %(spo2_percent)s, %(systolic_bp)s, %(diastolic_bp)s, %(respiratory_rate)s,
        %(device_id)s, %(is_valid_sensor)s, %(is_critical_alert)s, %(alert_reason)s, NOW()
    )
    ON CONFLICT (patient_id) DO UPDATE SET
        clinic_id = EXCLUDED.clinic_id,
        bed_id = EXCLUDED.bed_id,
        timestamp = EXCLUDED.timestamp,
        heart_rate_bpm = EXCLUDED.heart_rate_bpm,
        spo2_percent = EXCLUDED.spo2_percent,
        systolic_bp = EXCLUDED.systolic_bp,
        diastolic_bp = EXCLUDED.diastolic_bp,
        respiratory_rate = EXCLUDED.respiratory_rate,
        device_id = EXCLUDED.device_id,
        is_valid_sensor = EXCLUDED.is_valid_sensor,
        is_critical_alert = EXCLUDED.is_critical_alert,
        alert_reason = EXCLUDED.alert_reason,
        updated_at = NOW();
    """

    payload_list = []
    for row in records:
        row_dict = row.asDict()
        payload_list.append(row_dict)

    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        execute_batch(cursor, upsert_query, payload_list)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error escribiendo micro-batch en PostgreSQL: {e}")

    # 2. Notificar a FastAPI para emisión WebSocket reactiva (<40ms)
    try:
        response = requests.post(
            FASTAPI_INTERNAL_URL,
            json=payload_list,
            timeout=1.0
        )
        if response.status_code != 200:
            logger.warning(f"FastAPI respondió con código {response.status_code}")
    except requests.exceptions.RequestException:
        # Si el backend aún no ha levantado o se reinicia, Spark sigue corriendo sin bloquearse
        pass


def build_spark_speed_session():
    """Inicializa la sesión de Spark configurando conectores Kafka y S3/MinIO."""
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]

    spark = SparkSession.builder \
        .appName("UCI-Lambda-Speed-Layer") \
        .master("local[*]") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.ui.port", "4040") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    logger.info(" Iniciando Capa Speed de Spark Structured Streaming...")
    spark = build_spark_speed_session()

    # 1. Ingesta Continua desde Kafka
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 2. Deserialización JSON
    parsed_df = raw_kafka_df.select(
        from_json(col("value").cast("string"), VITALS_SCHEMA).alias("vitals")
    ).select("vitals.*")

    # 3. Evaluación de Calidad de Hardware y Reglas Clínicas (BUSINESS_LOGIC.md)
    # Regla de Calidad: No usar carpetas de cuarentena; clasificar con is_valid_sensor
    validated_df = parsed_df.withColumn(
        "is_valid_sensor",
        (col("heart_rate_bpm") > 0.0) & (col("heart_rate_bpm") <= 280.0) &
        (col("spo2_percent") > 0.0) & (col("spo2_percent") <= 100.0) &
        (col("systolic_bp") >= 20.0) & (col("systolic_bp") <= 300.0)
    )

    # Reglas Clínicas de Alerta Crítica (Reflejo Médico)
    scored_df = validated_df.withColumn(
        "is_critical_alert",
        when(~col("is_valid_sensor"), False)
        .when(col("heart_rate_bpm") > 140.0, True)
        .when(col("heart_rate_bpm") < 45.0, True)
        .when(col("spo2_percent") < 88.0, True)
        .when(col("systolic_bp") < 80.0, True)
        .when(col("systolic_bp") > 180.0, True)
        .otherwise(False)
    ).withColumn(
        "alert_reason",
        when(~col("is_valid_sensor"), "FALLO_SENSOR: Lectura física anómala o electrodo desconectado")
        .when(col("heart_rate_bpm") > 140.0, "Taquicardia severa crítica (>140 bpm)")
        .when(col("heart_rate_bpm") < 45.0, "Bradicardia severa extrema (<45 bpm)")
        .when(col("spo2_percent") < 88.0, "Hipoxia respiratoria aguda (<88%)")
        .when(col("systolic_bp") < 80.0, "Colapso hemodinámico / Shock (<80 mmHg)")
        .when(col("systolic_bp") > 180.0, "Crisis hipertensiva crítica (>180 mmHg)")
        .otherwise("Estable")
    )

    # 4. Salida en Streaming con foreachBatch
    query = scored_df.writeStream \
        .foreachBatch(process_micro_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    logger.info(" Capa Speed en ejecución continua (Spark UI disponible en puerto 4040).")
    query.awaitTermination()


if __name__ == "__main__":
    main()
