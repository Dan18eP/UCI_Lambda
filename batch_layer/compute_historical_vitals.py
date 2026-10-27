"""
Capa Batch - Tarea 2: Cálculo de Vistas Analíticas e Históricas.
Arquitectura Lambda - Monitoreo Crítico UCI.
Cumple: RULES.md (Garantía de Idempotencia: sobreescritura/UPSERT sin duplicación).
"""

import os
import sys
import logging
import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import s3fs
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [BATCH-ANALYTICS] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("batch_analytics")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres-serving")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "serving_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_password")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "data-lake")


def get_s3_filesystem():
    return s3fs.S3FileSystem(
        key=MINIO_USER,
        secret=MINIO_PASSWORD,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT}
    )


def compute_and_update_historical_views():
    """Lee el Master Dataset en MinIO, calcula estadísticas por paciente y hora, y actualiza vitals_historico."""
    logger.info("Iniciando cálculo de vistas históricas desde el Master Dataset en MinIO...")
    fs = get_s3_filesystem()
    master_path = f"{S3_BUCKET}/raw-vitals/"

    try:
        files = fs.glob(f"{master_path}*/*/*/*/*.parquet")
    except Exception as e:
        logger.warning(f"No se pudo consultar MinIO o la ruta no existe: {e}")
        return

    if not files:
        logger.info("ℹ No hay archivos Parquet en el Master Dataset aún. Finalizando.")
        return

    logger.info(f" Encontrados {len(files)} archivos Parquet en el Master Dataset. Consolidando...")

    # Cargar y concatenar DataFrames
    dfs = []
    for f in files:
        with fs.open(f, "rb") as fp:
            df_part = pq.read_table(fp).to_pandas()
            dfs.append(df_part)

    df_all = pd.concat(dfs, ignore_index=True)
    if df_all.empty:
        logger.info("Dataset vacío.")
        return

    # Convertir timestamps a datetime y redondear a la hora para particionamiento temporal
    df_all["timestamp_dt"] = pd.to_datetime(df_all["timestamp"])
    df_all["hour_timestamp"] = df_all["timestamp_dt"].dt.floor("h")

    # Agrupar por paciente, clínica, cama y hora
    historical_records = []
    grouped = df_all.groupby(["patient_id", "clinic_id", "bed_id", "hour_timestamp"])

    for (patient_id, clinic_id, bed_id, hour_ts), group in grouped:
        total_samples = len(group)
        valid_group = group[group["is_valid_sensor"] == True]
        valid_samples = len(valid_group)
        sensor_errors = total_samples - valid_samples

        if valid_samples > 0:
            avg_hr = round(float(valid_group["heart_rate_bpm"].mean()), 2)
            min_hr = round(float(valid_group["heart_rate_bpm"].min()), 2)
            max_hr = round(float(valid_group["heart_rate_bpm"].max()), 2)

            avg_spo2 = round(float(valid_group["spo2_percent"].mean()), 2)
            min_spo2 = round(float(valid_group["spo2_percent"].min()), 2)

            avg_sbp = round(float(valid_group["systolic_bp"].mean()), 2)
            avg_dbp = round(float(valid_group["diastolic_bp"].mean()), 2)
            avg_rr = round(float(valid_group["respiratory_rate"].mean()), 2)

            # Presión Arterial Media (PAM)
            avg_map = round((2.0 * avg_dbp + avg_sbp) / 3.0, 2)
        else:
            avg_hr = min_hr = max_hr = 0.0
            avg_spo2 = min_spo2 = 0.0
            avg_sbp = avg_dbp = avg_rr = avg_map = 0.0

        historical_records.append({
            "patient_id": patient_id,
            "clinic_id": clinic_id,
            "bed_id": bed_id,
            "hour_timestamp": hour_ts.isoformat(),
            "avg_heart_rate": avg_hr,
            "min_heart_rate": min_hr,
            "max_heart_rate": max_hr,
            "avg_spo2": avg_spo2,
            "min_spo2": min_spo2,
            "avg_systolic_bp": avg_sbp,
            "avg_diastolic_bp": avg_dbp,
            "avg_respiratory_rate": avg_rr,
            "avg_map": avg_map,
            "total_samples": total_samples,
            "valid_samples": valid_samples,
            "sensor_errors_detected": sensor_errors
        })

    # Escritura Idempotente en PostgreSQL (RULES.md: Idempotencia estricta en Batch)
    upsert_query = """
    INSERT INTO vitals_historico (
        patient_id, clinic_id, bed_id, hour_timestamp,
        avg_heart_rate, min_heart_rate, max_heart_rate,
        avg_spo2, min_spo2, avg_systolic_bp, avg_diastolic_bp,
        avg_respiratory_rate, avg_map, total_samples, valid_samples,
        sensor_errors_detected, created_at
    ) VALUES (
        %(patient_id)s, %(clinic_id)s, %(bed_id)s, %(hour_timestamp)s,
        %(avg_heart_rate)s, %(min_heart_rate)s, %(max_heart_rate)s,
        %(avg_spo2)s, %(min_spo2)s, %(avg_systolic_bp)s, %(avg_diastolic_bp)s,
        %(avg_respiratory_rate)s, %(avg_map)s, %(total_samples)s, %(valid_samples)s,
        %(sensor_errors_detected)s, NOW()
    )
    ON CONFLICT (patient_id, hour_timestamp) DO UPDATE SET
        avg_heart_rate = EXCLUDED.avg_heart_rate,
        min_heart_rate = EXCLUDED.min_heart_rate,
        max_heart_rate = EXCLUDED.max_heart_rate,
        avg_spo2 = EXCLUDED.avg_spo2,
        min_spo2 = EXCLUDED.min_spo2,
        avg_systolic_bp = EXCLUDED.avg_systolic_bp,
        avg_diastolic_bp = EXCLUDED.avg_diastolic_bp,
        avg_respiratory_rate = EXCLUDED.avg_respiratory_rate,
        avg_map = EXCLUDED.avg_map,
        total_samples = EXCLUDED.total_samples,
        valid_samples = EXCLUDED.valid_samples,
        sensor_errors_detected = EXCLUDED.sensor_errors_detected,
        created_at = NOW();
    """

    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        execute_batch(cur, upsert_query, historical_records)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f" {len(historical_records)} registros históricos consolidados con idempotencia en PostgreSQL.")
    except Exception as e:
        logger.error(f" Error al persistir vistas históricas en PostgreSQL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    compute_and_update_historical_views()
