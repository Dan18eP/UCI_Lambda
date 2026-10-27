"""
Simulador de Telemetría Biomédica de Alta Fidelidad para Red de Clínicas UCI.
Arquitectura Lambda - Monitoreo Crítico en Tiempo Real.
Política: Cero Mocks. Generación basada en dinámicas fisiológicas reales.
"""

import os
import sys
import time
import json
import uuid
import random
import datetime
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [ICU-PRODUCER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("icu_producer")

# Variables de Entorno
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "vitals-stream")
EMISSION_INTERVAL_SEC = float(os.getenv("EMISSION_INTERVAL_SEC", "1.0"))

# Definición de las 12 Camas de la Red Hospitalaria Metropolitana
# Sede Central (Camas 1 a 6) y Sede Norte (Camas 7 a 12)
BED_PROFILES = [
    # --- SEDE CENTRAL (CLI-CENTRAL-01) ---
    {
        "bed_id": "UCI-BED-01",
        "clinic_id": "CLI-CENTRAL-01",
        "patient_id": "PAT-1001",
        "device_id": "MON-PHILIPS-01",
        "condition": "ESTABLE",
        "base_hr": 72.0, "base_spo2": 98.0, "base_sbp": 115.0, "base_dbp": 75.0, "base_rr": 15.0
    },
    {
        "bed_id": "UCI-BED-02",
        "clinic_id": "CLI-CENTRAL-01",
        "patient_id": "PAT-1002",
        "device_id": "MON-PHILIPS-02",
        "condition": "ESTABLE",
        "base_hr": 68.0, "base_spo2": 97.5, "base_sbp": 120.0, "base_dbp": 78.0, "base_rr": 16.0
    },
    {
        "bed_id": "UCI-BED-03",
        "clinic_id": "CLI-CENTRAL-01",
        "patient_id": "PAT-1003",
        "device_id": "MON-PHILIPS-03",
        "condition": "ESTABLE",
        "base_hr": 80.0, "base_spo2": 96.0, "base_sbp": 110.0, "base_dbp": 70.0, "base_rr": 17.0
    },
    {
        # Cama Crítica: Taquicardia ventricular y colapso hemodinámico (Dispara WebSocket)
        "bed_id": "UCI-BED-04",
        "clinic_id": "CLI-CENTRAL-01",
        "patient_id": "PAT-1082",
        "device_id": "MON-PHILIPS-04",
        "condition": "CRITICO_ARRITMIA",
        "base_hr": 148.0, "base_spo2": 87.0, "base_sbp": 78.0, "base_dbp": 48.0, "base_rr": 28.0
    },
    {
        "bed_id": "UCI-BED-05",
        "clinic_id": "CLI-CENTRAL-01",
        "patient_id": "PAT-1005",
        "device_id": "MON-PHILIPS-05",
        "condition": "ESTABLE",
        "base_hr": 75.0, "base_spo2": 99.0, "base_sbp": 118.0, "base_dbp": 76.0, "base_rr": 14.0
    },
    {
        # Cama en Advertencia: Pre-hipertensión y taquicardia moderada
        "bed_id": "UCI-BED-06",
        "clinic_id": "CLI-CENTRAL-01",
        "patient_id": "PAT-1006",
        "device_id": "MON-PHILIPS-06",
        "condition": "ADVERTENCIA_HIPERTENSION",
        "base_hr": 105.0, "base_spo2": 94.0, "base_sbp": 138.0, "base_dbp": 88.0, "base_rr": 21.0
    },

    # --- SEDE NORTE (CLI-NORTE-02) ---
    {
        "bed_id": "UCI-BED-07",
        "clinic_id": "CLI-NORTE-02",
        "patient_id": "PAT-2007",
        "device_id": "MON-MINDRAY-07",
        "condition": "ESTABLE",
        "base_hr": 65.0, "base_spo2": 98.5, "base_sbp": 112.0, "base_dbp": 72.0, "base_rr": 15.0
    },
    {
        "bed_id": "UCI-BED-08",
        "clinic_id": "CLI-NORTE-02",
        "patient_id": "PAT-2008",
        "device_id": "MON-MINDRAY-08",
        "condition": "ESTABLE",
        "base_hr": 78.0, "base_spo2": 96.5, "base_sbp": 122.0, "base_dbp": 80.0, "base_rr": 16.0
    },
    {
        "bed_id": "UCI-BED-09",
        "clinic_id": "CLI-NORTE-02",
        "patient_id": "PAT-2009",
        "device_id": "MON-MINDRAY-09",
        "condition": "ESTABLE",
        "base_hr": 82.0, "base_spo2": 97.0, "base_sbp": 116.0, "base_dbp": 74.0, "base_rr": 18.0
    },
    {
        "bed_id": "UCI-BED-10",
        "clinic_id": "CLI-NORTE-02",
        "patient_id": "PAT-2010",
        "device_id": "MON-MINDRAY-10",
        "condition": "ESTABLE",
        "base_hr": 70.0, "base_spo2": 98.0, "base_sbp": 114.0, "base_dbp": 75.0, "base_rr": 14.0
    },
    {
        # Cama en Advertencia: Hipoxia respiratoria moderada
        "bed_id": "UCI-BED-11",
        "clinic_id": "CLI-NORTE-02",
        "patient_id": "PAT-2011",
        "device_id": "MON-MINDRAY-11",
        "condition": "ADVERTENCIA_HIPOXIA",
        "base_hr": 92.0, "base_spo2": 91.5, "base_sbp": 125.0, "base_dbp": 82.0, "base_rr": 23.0
    },
    {
        # Cama con Falla de Sensor: Electrodo de pulso caído / lectura anómala
        "bed_id": "UCI-BED-12",
        "clinic_id": "CLI-NORTE-02",
        "patient_id": "PAT-2012",
        "device_id": "MON-MINDRAY-12",
        "condition": "FALLO_HARDWARE",
        "base_hr": 0.0, "base_spo2": 0.0, "base_sbp": 115.0, "base_dbp": 75.0, "base_rr": 15.0
    }
]


def create_kafka_producer(servers: str, max_retries: int = 20, delay_sec: int = 3):
    """Establece conexión resiliente con Apache Kafka KRaft."""
    server_list = [s.strip() for s in servers.split(",")]
    logger.info(f"Intentando conectar con Kafka en {server_list}...")
    
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=server_list,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=10
            )
            logger.info(f"✅ Conexión establecida con Kafka (intento {attempt}).")
            return producer
        except NoBrokersAvailable as e:
            logger.warning(f"Kafka aún no disponible (intento {attempt}/{max_retries}). Reintentando en {delay_sec}s...")
            time.sleep(delay_sec)
        except Exception as e:
            logger.error(f"Error inesperado al conectar con Kafka: {e}")
            time.sleep(delay_sec)

    logger.critical("❌ No fue posible conectar con Kafka tras múltiples intentos.")
    sys.exit(1)


def generate_biometric_sample(profile: dict) -> dict:
    """
    Genera una muestra fisiológica continua con variabilidad real (ruido gaussiano)
    o patrones patológicos según el perfil de la cama.
    """
    condition = profile["condition"]
    
    if condition == "FALLO_HARDWARE":
        # Simulación de falla física de electrodos: valores 0.0 o fuera de escala biológica
        hr = 0.0
        spo2 = 0.0
        sbp = round(random.gauss(profile["base_sbp"], 2.0), 1)
        dbp = round(random.gauss(profile["base_dbp"], 1.5), 1)
        rr = round(random.gauss(profile["base_rr"], 1.0), 1)
    elif condition == "CRITICO_ARRITMIA":
        # Taquicardia ventricular paroxística + Colapso de Presión (Shock)
        hr = round(random.gauss(profile["base_hr"], 4.5), 1)
        spo2 = round(max(78.0, min(89.0, random.gauss(profile["base_spo2"], 1.2))), 1)
        sbp = round(max(65.0, min(84.0, random.gauss(profile["base_sbp"], 2.5))), 1)
        dbp = round(max(40.0, min(56.0, random.gauss(profile["base_dbp"], 2.0))), 1)
        rr = round(max(24.0, min(34.0, random.gauss(profile["base_rr"], 1.5))), 1)
    else:
        # Fisiología normal con variabilidad natural (Heart Rate Variability - HRV)
        hr = round(max(40.0, min(200.0, random.gauss(profile["base_hr"], 2.0))), 1)
        spo2 = round(max(85.0, min(100.0, random.gauss(profile["base_spo2"], 0.4))), 1)
        sbp = round(max(70.0, min(220.0, random.gauss(profile["base_sbp"], 2.5))), 1)
        dbp = round(max(40.0, min(130.0, random.gauss(profile["base_dbp"], 1.8))), 1)
        rr = round(max(8.0, min(40.0, random.gauss(profile["base_rr"], 0.8))), 1)

    event_id = str(uuid.uuid4())
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    return {
        "event_id": event_id,
        "clinic_id": profile["clinic_id"],
        "patient_id": profile["patient_id"],
        "bed_id": profile["bed_id"],
        "timestamp": timestamp,
        "heart_rate_bpm": hr,
        "spo2_percent": spo2,
        "systolic_bp": sbp,
        "diastolic_bp": dbp,
        "respiratory_rate": rr,
        "device_id": profile["device_id"]
    }


def run_producer():
    """Bucle principal de emisión de telemetría de las 12 camas."""
    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    logger.info(f"Iniciando flujo de telemetría continua hacia topic '{KAFKA_TOPIC}'...")
    logger.info(f"Camas activas: {len(BED_PROFILES)} en red hospitalaria (Central y Norte). Cero Mocks.")

    total_emitted = 0
    try:
        while True:
            cycle_start = time.time()
            for profile in BED_PROFILES:
                payload = generate_biometric_sample(profile)
                # La clave del mensaje es el patient_id para garantizar orden por paciente en la partición de Kafka
                producer.send(
                    topic=KAFKA_TOPIC,
                    key=payload["patient_id"],
                    value=payload
                )
                total_emitted += 1

            producer.flush()
            if total_emitted % 120 == 0:
                logger.info(f"📡 [Telemetría Activa] {total_emitted} eventos emitidos a Kafka KRaft.")

            elapsed = time.time() - cycle_start
            sleep_time = max(0.0, EMISSION_INTERVAL_SEC - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Detención manual del productor.")
    finally:
        producer.close()
        logger.info("Conexión con Kafka cerrada limpiamente.")


if __name__ == "__main__":
    run_producer()
