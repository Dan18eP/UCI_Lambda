"""
Capa de Servicio (Serving Layer) - API Gateway y WebSocket.
FastAPI de Alta Concurrencia para Monitoreo UCI, Asistente Batch ML y Telemetria de Auditoria.
Cero emojis en codigo y respuestas.
"""

import json
import logging
from typing import Optional, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from db import (
    init_db,
    check_health,
    fetch_dashboard_beds,
    fetch_patient_snapshot,
    save_prediction,
    fetch_historical_analytics,
    fetch_audit_metrics,
    query_batch_assistant
)
from clinical_model import clinical_model

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [SERVING-API] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("serving_api")

app = FastAPI(
    title="UCI Serving Layer API",
    description="Capa de Servicio para Monitoreo de UCI, Alertas Reactivas y Modelo Tabular ML (<3ms)",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class WebSocketManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active_connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active_connections:
            self.active_connections.remove(ws)

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return
        payload = json.dumps(message)
        dead = []
        for ws in self.active_connections:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for d in dead:
            self.disconnect(d)


ws_manager = WebSocketManager()


@app.on_event("startup")
def on_startup():
    init_db()
    logger.info("Base de datos de Serving Layer inicializada.")


@app.get("/api/v1/health")
def health_endpoint():
    db_ok = check_health()
    return {
        "status": "UP" if db_ok else "DEGRADED",
        "service": "serving-layer-api",
        "database": "connected" if db_ok else "unreachable"
    }


@app.get("/api/v1/dashboard")
def dashboard_endpoint(clinic_id: Optional[str] = Query(None)):
    beds = fetch_dashboard_beds(clinic_id)
    if not beds:
        return {
            "status": "NO_DATA",
            "message": "Aviso de Auditoria: Capa de Servicio vacia. Esperando ingestion de eventos desde el pipeline Lambda.",
            "beds": []
        }
    return {
        "status": "SUCCESS",
        "total_beds": len(beds),
        "clinic_filter": clinic_id or "TODAS",
        "beds": beds
    }


@app.post("/api/v1/internal/vitals-stream")
async def internal_stream_endpoint(events: List[dict]):
    """Canal interno invocado por Spark foreachBatch para empujar alertas hacia WebSocket."""
    for ev in events:
        await ws_manager.broadcast({
            "type": "VITALS_UPDATE",
            "data": ev
        })
    return {"status": "broadcasted", "count": len(events)}


@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)


class EvaluationRequest(BaseModel):
    patient_id: str


@app.post("/api/v1/clinical-assistant/evaluate")
def evaluate_patient_endpoint(req: EvaluationRequest):
    live_record, baseline_hr = fetch_patient_snapshot(req.patient_id)
    if not live_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Paciente {req.patient_id} no encontrado en vitals_realtime."
        )

    eval_result = clinical_model.evaluate_patient(
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

    try:
        save_prediction(eval_result)
    except Exception as e:
        logger.error(f"Error al registrar prediccion en base de datos: {e}")

    return eval_result


@app.get("/api/v1/analytics/history")
def analytics_history_endpoint(
    clinic_id: Optional[str] = Query(None),
    patient_id: Optional[str] = Query(None),
    hours: int = Query(24, ge=1, le=168)
):
    """
    Consulta analitica del Batch Layer: vitals_historico en PostgreSQL precomputado desde MinIO Parquet.
    Permite examinar tendencias horarias agregadas y comparar sedes sin latencia en la capa streaming.
    """
    data = fetch_historical_analytics(clinic_id=clinic_id, patient_id=patient_id, limit_hours=hours)
    return {
        "status": "SUCCESS",
        "layer": "Batch Layer (vitals_historico)",
        "source": "MinIO Data Lake (Raw Parquet Master Dataset) consolidado por Airflow",
        "data": data
    }


@app.get("/api/v1/audit/metrics")
def audit_metrics_endpoint():
    """
    Retorna metricas y contadores de auditoria en vivo del pipeline Lambda.
    Permite a la consola de auditoria y linaje mostrar actividad en tiempo real.
    """
    metrics = fetch_audit_metrics()
    return {
        "status": "SUCCESS",
        "pipeline": "UCI Lambda Architecture",
        "metrics": metrics
    }


class AssistantChatRequest(BaseModel):
    patient_id: Optional[str] = None
    query: str


@app.post("/api/v1/assistant/chat")
def assistant_chat_endpoint(req: AssistantChatRequest):
    """
    Chatbot Asistente Clinico basado en Batch Data.
    Responde consultas diagnosticas y proyecciones de riesgo usando los registros consolidados
    de la Capa Batch (vitals_historico) para no sobrecargar el Speed Layer.
    """
    res = query_batch_assistant(patient_id=req.patient_id, query_text=req.query)
    return {
        "status": "SUCCESS",
        "query": req.query,
        "response": res
    }
