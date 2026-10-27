"""
FastAPI Service Layer & WebSocket Gateway - Monitoreo Crítico UCI.
Arquitectura Lambda - Capa de Servicio.
Cumple: ADR-004 (Serving Layer), ADR-005 (WebSockets <40ms), ADR-006 (Cero Mocks), ADR-008 (ML <3ms).
"""

import logging
from typing import Optional, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2.extras

from .database import get_db_connection, init_db_pool
from .websocket_manager import ws_manager
from .ml_model import clinical_model

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [FASTAPI-BACKEND] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("fastapi_service")

app = FastAPI(
    title="UCI Lambda Serving API",
    description="Capa de Servicio de Alta Concurrencia para Monitoreo Crítico en UCI y Asistencia Diagnóstica ML.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup_event():
    logger.info("Inicializando Pool de conexiones de base de datos...")
    try:
        init_db_pool()
        logger.info("✅ Pool de PostgreSQL conectado correctamente.")
    except Exception as e:
        logger.error(f"Error conectando a PostgreSQL al iniciar: {e}")


@app.get("/api/v1/health")
def health_check():
    """Endpoint de auditoría de salud de la Capa de Servicio."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
        return {
            "status": "UP",
            "service": "fastapi-serving-layer",
            "serving_database": "connected",
            "message": "Serving Layer 100% operativa."
        }
    except Exception as e:
        return {
            "status": "DEGRADED",
            "service": "fastapi-serving-layer",
            "serving_database": f"error: {str(e)}"
        }


@app.get("/api/v1/dashboard")
def get_dashboard(clinic_id: Optional[str] = Query(None, description="Filtrar por sede hospitalaria")):
    """
    Retorna el estado actual de las camas UCI consultando la Serving Layer (vitals_realtime).
    Cumple la Regla de Cero Mocks: si la base está vacía, devuelve NO_DATA explícito.
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 1. Consultar camas en tiempo real
            if clinic_id:
                cur.execute("""
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
                    WHERE r.clinic_id = %s
                    ORDER BY r.bed_id ASC;
                """, (clinic_id,))
            else:
                cur.execute("""
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
                    ORDER BY r.bed_id ASC;
                """)
            rows = cur.fetchall()

    # CERO MOCKS: Validación arquitectónica
    if not rows:
        return {
            "status": "NO_DATA",
            "message": "⚠️ Aviso de Auditoría: Capa de Servicio vacía. Esperando que el pipeline ingiera eventos reales.",
            "beds": []
        }

    return {
        "status": "SUCCESS",
        "total_beds": len(rows),
        "clinic_filter": clinic_id or "TODAS",
        "beds": rows
    }


@app.post("/api/v1/internal/vitals-stream")
async def receive_stream_batch(events: List[dict]):
    """
    Canal interno recibido desde Spark foreachBatch (ADR-005).
    Retransmite inmediatamente las alertas críticas y actualizaciones por WebSocket.
    """
    for event in events:
        # Difundir por WebSocket a clientes conectados
        await ws_manager.broadcast({
            "type": "VITALS_UPDATE",
            "data": event
        })
    return {"status": "broadcasted", "count": len(events)}


@app.websocket("/ws/alerts")
async def websocket_alerts_endpoint(websocket: WebSocket):
    """Canal WebSocket reactivo (<40ms) para notificaciones push de signos vitales UCI."""
    await ws_manager.connect(websocket)
    try:
        while True:
            # Mantener conexión abierta; los mensajes son empujados por broadcast
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as e:
        logger.warning(f"Excepción en WebSocket: {e}")
        ws_manager.disconnect(websocket)


class EvaluationRequest(BaseModel):
    patient_id: str


@app.post("/api/v1/clinical-assistant/evaluate")
def evaluate_clinical_status(req: EvaluationRequest):
    """
    Evaluación diagnóstica con Modelo Tabular ML (<3 ms de latencia) (ADR-008).
    Cruza el último registro vivo de vitals_realtime con el basal histórico de vitals_historico.
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 1. Obtener estado en tiempo real
            cur.execute("""
                SELECT patient_id, clinic_id, bed_id, heart_rate_bpm, spo2_percent,
                       systolic_bp, diastolic_bp, respiratory_rate, is_valid_sensor
                FROM vitals_realtime
                WHERE patient_id = %s;
            """, (req.patient_id,))
            live_record = cur.fetchone()

            if not live_record:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No se encontró registro en tiempo real para el paciente {req.patient_id}."
                )

            # 2. Obtener basal histórico (promedio móvil)
            cur.execute("""
                SELECT avg_heart_rate
                FROM vitals_historico
                WHERE patient_id = %s
                ORDER BY hour_timestamp DESC
                LIMIT 1;
            """, (req.patient_id,))
            hist_record = cur.fetchone()
            baseline_hr = float(hist_record["avg_heart_rate"]) if hist_record else float(live_record["heart_rate_bpm"])

    # 3. Inferencia Ultrarrápida con el Modelo Tabular (<3 ms)
    evaluation = clinical_model.evaluate_patient(
        patient_id=live_record["patient_id"],
        clinic_id=live_record["clinic_id"],
        bed_id=live_record["bed_id"],
        heart_rate_bpm=float(live_record["heart_rate_bpm"]),
        spo2_percent=float(live_record["spo2_percent"]),
        systolic_bp=float(live_record["systolic_bp"]),
        diastolic_bp=float(live_record["diastolic_bp"]),
        respiratory_rate=float(live_record["respiratory_rate"]),
        baseline_avg_hr=baseline_hr
    )

    # 4. Registrar la predicción en la base de datos para trazabilidad
    try:
        with get_db_connection() as conn:
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
                    evaluation["prediction_id"],
                    evaluation["patient_id"],
                    evaluation["clinic_id"],
                    evaluation["bed_id"],
                    evaluation["evaluation_timestamp"],
                    evaluation["inference_latency_ms"],
                    evaluation["risk_score"],
                    evaluation["risk_level"],
                    evaluation["clinical_factors"]["mean_arterial_pressure"],
                    evaluation["clinical_factors"]["shock_index"],
                    evaluation["clinical_factors"]["hr_delta_from_baseline"],
                    evaluation["probabilities"]["bajo"],
                    evaluation["probabilities"]["moderado"],
                    evaluation["probabilities"]["alto"],
                    evaluation["probabilities"]["critico"],
                    evaluation["recommendation"]
                ))
                conn.commit()
    except Exception as e:
        logger.error(f"Error al guardar predicción en PostgreSQL: {e}")

    return evaluation
