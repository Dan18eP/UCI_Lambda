"""
Capa de Servicio (Serving Layer) - Gestor de Consultas PostgreSQL.
Monitoreo Crítico UCI - Arquitectura Lambda.
"""

import os
from contextlib import contextmanager
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres-serving")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "serving_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_password")

_pool = None


def init_db():
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )


@contextmanager
def get_db():
    global _pool
    if _pool is None:
        init_db()
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)


def check_health():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            return True


def fetch_dashboard_beds(clinic_id=None):
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT 
                    r.patient_id, r.clinic_id, r.bed_id, r.timestamp,
                    r.heart_rate_bpm, r.spo2_percent, r.systolic_bp, r.diastolic_bp,
                    r.respiratory_rate, r.device_id, r.is_valid_sensor,
                    r.is_critical_alert, r.alert_reason, r.updated_at,
                    h.avg_heart_rate AS baseline_avg_hr,
                    h.avg_spo2 AS baseline_avg_spo2,
                    h.avg_systolic_bp AS baseline_avg_sbp
                FROM vitals_realtime r
                LEFT JOIN LATERAL (
                    SELECT avg_heart_rate, avg_spo2, avg_systolic_bp
                    FROM vitals_historico
                    WHERE patient_id = r.patient_id
                    ORDER BY hour_timestamp DESC
                    LIMIT 1
                ) h ON TRUE
            """
            params = ()
            if clinic_id:
                query += " WHERE r.clinic_id = %s"
                params = (clinic_id,)

            query += " ORDER BY r.bed_id ASC;"
            cur.execute(query, params)
            return cur.fetchall()


def fetch_patient_snapshot(patient_id):
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT patient_id, clinic_id, bed_id, heart_rate_bpm, spo2_percent,
                       systolic_bp, diastolic_bp, respiratory_rate, is_valid_sensor
                FROM vitals_realtime
                WHERE patient_id = %s;
            """, (patient_id,))
            live = cur.fetchone()
            if not live:
                return None, None

            cur.execute("""
                SELECT avg_heart_rate
                FROM vitals_historico
                WHERE patient_id = %s
                ORDER BY hour_timestamp DESC
                LIMIT 1;
            """, (patient_id,))
            hist = cur.fetchone()
            baseline_hr = float(hist["avg_heart_rate"]) if hist else float(live["heart_rate_bpm"])
            return live, baseline_hr


def save_prediction(eval_data):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO clinical_predictions (
                    prediction_id, patient_id, clinic_id, bed_id, evaluation_timestamp,
                    inference_latency_ms, risk_score, risk_level, mean_arterial_pressure,
                    shock_index, hr_delta_from_baseline, prob_bajo, prob_moderado,
                    prob_alto, prob_critico, recommendation, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                );
            """, (
                eval_data["prediction_id"],
                eval_data["patient_id"],
                eval_data["clinic_id"],
                eval_data["bed_id"],
                eval_data["evaluation_timestamp"],
                eval_data["inference_latency_ms"],
                eval_data["risk_score"],
                eval_data["risk_level"],
                eval_data["clinical_factors"]["mean_arterial_pressure"],
                eval_data["clinical_factors"]["shock_index"],
                eval_data["clinical_factors"]["hr_delta_from_baseline"],
                eval_data["probabilities"]["bajo"],
                eval_data["probabilities"]["moderado"],
                eval_data["probabilities"]["alto"],
                eval_data["probabilities"]["critico"],
                eval_data["recommendation"]
            ))
            conn.commit()
