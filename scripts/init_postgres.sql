-- ==============================================================================
-- SERVING LAYER INITIALIZATION SCRIPT (PostgreSQL)
-- Monitoreo Crítico UCI - Arquitectura Lambda
-- Regla de Oro: Solo contiene vistas precomputadas e indexadas. Cero Staging.
-- ==============================================================================

-- Base de datos de metastore para Apache Airflow
SELECT 'CREATE DATABASE airflow_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec

-- 1. Capa Speed: vitals_realtime
-- Actualizada en milisegundos por Spark Structured Streaming.
-- Mantiene el último estado vivo y las alertas activas de cada paciente.
CREATE TABLE IF NOT EXISTS vitals_realtime (
    patient_id VARCHAR(50) PRIMARY KEY,
    clinic_id VARCHAR(50) NOT NULL,
    bed_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    heart_rate_bpm NUMERIC(5, 2) NOT NULL,
    spo2_percent NUMERIC(5, 2) NOT NULL,
    systolic_bp NUMERIC(5, 2) NOT NULL,
    diastolic_bp NUMERIC(5, 2) NOT NULL,
    respiratory_rate NUMERIC(5, 2) NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    is_valid_sensor BOOLEAN NOT NULL DEFAULT TRUE,
    is_critical_alert BOOLEAN NOT NULL DEFAULT FALSE,
    alert_reason TEXT DEFAULT 'Estable',
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_vitals_realtime_clinic ON vitals_realtime (clinic_id);
CREATE INDEX IF NOT EXISTS idx_vitals_realtime_bed ON vitals_realtime (bed_id);
CREATE INDEX IF NOT EXISTS idx_vitals_realtime_alert ON vitals_realtime (is_critical_alert);

-- 2. Capa Batch: vitals_historico
-- Poblada de forma horaria por Airflow + Spark Batch (.mode("overwrite")).
-- Almacena promedios móviles, métricas estadísticas y basales fisiológicos.
CREATE TABLE IF NOT EXISTS vitals_historico (
    patient_id VARCHAR(50) NOT NULL,
    clinic_id VARCHAR(50) NOT NULL,
    bed_id VARCHAR(50) NOT NULL,
    hour_timestamp TIMESTAMPTZ NOT NULL,
    avg_heart_rate NUMERIC(5, 2) NOT NULL,
    min_heart_rate NUMERIC(5, 2) NOT NULL,
    max_heart_rate NUMERIC(5, 2) NOT NULL,
    avg_spo2 NUMERIC(5, 2) NOT NULL,
    min_spo2 NUMERIC(5, 2) NOT NULL,
    avg_systolic_bp NUMERIC(5, 2) NOT NULL,
    avg_diastolic_bp NUMERIC(5, 2) NOT NULL,
    avg_respiratory_rate NUMERIC(5, 2) NOT NULL,
    avg_map NUMERIC(5, 2) NOT NULL,
    total_samples INT NOT NULL,
    valid_samples INT NOT NULL,
    sensor_errors_detected INT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (patient_id, hour_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_vitals_historico_clinic ON vitals_historico (clinic_id);
CREATE INDEX IF NOT EXISTS idx_vitals_historico_patient ON vitals_historico (patient_id);
CREATE INDEX IF NOT EXISTS idx_vitals_historico_hour ON vitals_historico (hour_timestamp);

-- 3. Soporte Diagnóstico: clinical_predictions
-- Almacena las evaluaciones del Modelo Tabular ML (Scikit-Learn/XGBoost).
CREATE TABLE IF NOT EXISTS clinical_predictions (
    prediction_id VARCHAR(64) PRIMARY KEY,
    patient_id VARCHAR(50) NOT NULL,
    clinic_id VARCHAR(50) NOT NULL,
    bed_id VARCHAR(50) NOT NULL,
    evaluation_timestamp TIMESTAMPTZ NOT NULL,
    inference_latency_ms NUMERIC(6, 2) NOT NULL,
    risk_score NUMERIC(5, 2) NOT NULL,
    risk_level VARCHAR(20) NOT NULL,
    mean_arterial_pressure NUMERIC(5, 2) NOT NULL,
    shock_index NUMERIC(5, 2) NOT NULL,
    hr_delta_from_baseline NUMERIC(5, 2) NOT NULL,
    prob_bajo NUMERIC(4, 3) NOT NULL,
    prob_moderado NUMERIC(4, 3) NOT NULL,
    prob_alto NUMERIC(4, 3) NOT NULL,
    prob_critico NUMERIC(4, 3) NOT NULL,
    recommendation TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_clinical_predictions_patient ON clinical_predictions (patient_id);
CREATE INDEX IF NOT EXISTS idx_clinical_predictions_clinic ON clinical_predictions (clinic_id);
