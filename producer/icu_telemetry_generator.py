"""
Simulador de Telemetria Biomedica de Alta Fidelidad para Red de Clinicas UCI.
Arquitectura Lambda - Monitoreo Critico en Tiempo Real.
Politica: Cero Mocks. Generacion basada en dinamicas fisiologicas reales y Cadenas de Markov continuas.
Sin botones de simulacion manual en UI: El productor evoluciona de manera 100% autonoma.
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

# Configuracion de Logging (Cero Emojis)
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

# Definicion Inicial de las 12 Camas de la Red Hospitalaria Metropolitana
# Sede Central (Camas 1 a 6) y Sede Norte (Camas 7 a 12)
INITIAL_BEDS = [
    # --- SEDE CENTRAL (CLI-CENTRAL-01) ---
    {
        "bed_id": "UCI-BED-01", "clinic_id": "CLI-CENTRAL-01", "patient_id": "PAT-1001",
        "device_id": "MON-PHILIPS-01", "base_hr": 72.0, "base_spo2": 98.0, "base_sbp": 115.0, "base_dbp": 75.0, "base_rr": 15.0
    },
    {
        "bed_id": "UCI-BED-02", "clinic_id": "CLI-CENTRAL-01", "patient_id": "PAT-1002",
        "device_id": "MON-PHILIPS-02", "base_hr": 68.0, "base_spo2": 97.5, "base_sbp": 120.0, "base_dbp": 78.0, "base_rr": 16.0
    },
    {
        "bed_id": "UCI-BED-03", "clinic_id": "CLI-CENTRAL-01", "patient_id": "PAT-1003",
        "device_id": "MON-PHILIPS-03", "base_hr": 80.0, "base_spo2": 96.0, "base_sbp": 110.0, "base_dbp": 70.0, "base_rr": 17.0
    },
    {
        "bed_id": "UCI-BED-04", "clinic_id": "CLI-CENTRAL-01", "patient_id": "PAT-1004",
        "device_id": "MON-PHILIPS-04", "base_hr": 76.0, "base_spo2": 97.0, "base_sbp": 118.0, "base_dbp": 74.0, "base_rr": 15.0
    },
    {
        "bed_id": "UCI-BED-05", "clinic_id": "CLI-CENTRAL-01", "patient_id": "PAT-1005",
        "device_id": "MON-PHILIPS-05", "base_hr": 74.0, "base_spo2": 99.0, "base_sbp": 116.0, "base_dbp": 76.0, "base_rr": 14.0
    },
    {
        "bed_id": "UCI-BED-06", "clinic_id": "CLI-CENTRAL-01", "patient_id": "PAT-1006",
        "device_id": "MON-PHILIPS-06", "base_hr": 78.0, "base_spo2": 96.5, "base_sbp": 124.0, "base_dbp": 80.0, "base_rr": 16.0
    },

    # --- SEDE NORTE (CLI-NORTE-02) ---
    {
        "bed_id": "UCI-BED-07", "clinic_id": "CLI-NORTE-02", "patient_id": "PAT-2007",
        "device_id": "MON-MINDRAY-07", "base_hr": 65.0, "base_spo2": 98.5, "base_sbp": 112.0, "base_dbp": 72.0, "base_rr": 15.0
    },
    {
        "bed_id": "UCI-BED-08", "clinic_id": "CLI-NORTE-02", "patient_id": "PAT-2008",
        "device_id": "MON-MINDRAY-08", "base_hr": 70.0, "base_spo2": 97.0, "base_sbp": 122.0, "base_dbp": 78.0, "base_rr": 16.0
    },
    {
        "bed_id": "UCI-BED-09", "clinic_id": "CLI-NORTE-02", "patient_id": "PAT-2009",
        "device_id": "MON-MINDRAY-09", "base_hr": 82.0, "base_spo2": 96.0, "base_sbp": 116.0, "base_dbp": 74.0, "base_rr": 18.0
    },
    {
        "bed_id": "UCI-BED-10", "clinic_id": "CLI-NORTE-02", "patient_id": "PAT-2010",
        "device_id": "MON-MINDRAY-10", "base_hr": 73.0, "base_spo2": 98.0, "base_sbp": 114.0, "base_dbp": 75.0, "base_rr": 14.0
    },
    {
        "bed_id": "UCI-BED-11", "clinic_id": "CLI-NORTE-02", "patient_id": "PAT-2011",
        "device_id": "MON-MINDRAY-11", "base_hr": 75.0, "base_spo2": 97.5, "base_sbp": 118.0, "base_dbp": 76.0, "base_rr": 15.0
    },
    {
        "bed_id": "UCI-BED-12", "clinic_id": "CLI-NORTE-02", "patient_id": "PAT-2012",
        "device_id": "MON-MINDRAY-12", "base_hr": 69.0, "base_spo2": 98.0, "base_sbp": 120.0, "base_dbp": 76.0, "base_rr": 15.0
    }
]


class BedSimulationState:
    """
    Modelo de Estado Clinico Continuo basado en Cadenas de Markov para cada Cama UCI.
    Simula dinámicas de deterioro, intervenciones medicas espontaneas y fallas/reconexiones transitorias de sensores.
    """
    def __init__(self, config: dict):
        self.config = config
        self.bed_id = config["bed_id"]
        self.clinic_id = config["clinic_id"]
        self.patient_id = config["patient_id"]
        self.device_id = config["device_id"]
        
        # Linea Base Fisiologica
        self.base_hr = config["base_hr"]
        self.base_spo2 = config["base_spo2"]
        self.base_sbp = config["base_sbp"]
        self.base_dbp = config["base_dbp"]
        self.base_rr = config["base_rr"]
        
        # Estado Fisiologico Inicial: ESTABLE, ADVERTENCIA, CRITICO, FALLO_SENSOR
        # Para realismo inicial, inicializamos cama 4 en advertencia leve y el resto estables
        if self.bed_id == "UCI-BED-04":
            self.state = "ADVERTENCIA"
        else:
            self.state = "ESTABLE"

        self.cycles_in_state = 0
        self.max_cycles_in_state = random.randint(25, 40)
        self.sub_type = None  # Tipo de crisis (e.g. ARRITMIA, SHOCK, HIPOXIA)

    def transition_state(self):
        """Evoluciona el estado clinico de la cama mediante transicion estocastica de Markov."""
        self.cycles_in_state += 1

        if self.state == "ESTABLE":
            # 94% de probabilidad de permanecer estable en cada ciclo
            roll = random.random()
            if roll < 0.04:
                # Transicion a advertencia
                self.state = "ADVERTENCIA"
                self.sub_type = random.choice(["TAQUICARDIA", "HIPOXIA_MODERADA", "PRE_HIPERTENSION"])
                self.cycles_in_state = 0
                self.max_cycles_in_state = random.randint(20, 35)
                logger.info(f"[{self.bed_id}] Transicion clinica: ESTABLE -> ADVERTENCIA ({self.sub_type}).")
            elif roll < 0.05:
                # Transicion directa a evento agudo critico (1%)
                self.state = "CRITICO"
                self.sub_type = random.choice(["ARRITMIA_VENTRICULAR", "SHOCK_HEMODINAMICO"])
                self.cycles_in_state = 0
                self.max_cycles_in_state = random.randint(25, 40)  # Dura 25-40s hasta intervencion medica
                logger.info(f"[{self.bed_id}] ALERTA CRITICA: Paciente entra en crisis aguda ({self.sub_type}).")
            elif roll < 0.055:
                # Glitch transitorio de electrodo/sensor (0.5%)
                self.state = "FALLO_SENSOR"
                self.cycles_in_state = 0
                self.max_cycles_in_state = random.randint(15, 25)  # 15-25s hasta reconexion por enfermeria
                logger.info(f"[{self.bed_id}] ANOMALIA HARDWARE: Electrodo desconectado o artefacto de senal.")

        elif self.state == "ADVERTENCIA":
            # Si se cumplio el tiempo en advertencia
            if self.cycles_in_state >= self.max_cycles_in_state:
                # 80% responde al tratamiento profilactico y se estabiliza, 20% empeora a critico
                if random.random() < 0.80:
                    self.state = "ESTABLE"
                    self.sub_type = None
                    logger.info(f"[{self.bed_id}] Paciente estabilizado: ADVERTENCIA -> ESTABLE.")
                else:
                    self.state = "CRITICO"
                    self.sub_type = "SHOCK_HEMODINAMICO"
                    logger.info(f"[{self.bed_id}] Deterioro del paciente: ADVERTENCIA -> CRITICO.")
                self.cycles_in_state = 0
                self.max_cycles_in_state = random.randint(25, 40)

        elif self.state == "CRITICO":
            # Simula intervencion medica de reanimacion / soporte inotropico
            if self.cycles_in_state >= self.max_cycles_in_state:
                # La intervencion medica surte efecto: paciente sale del colapso
                if random.random() < 0.60:
                    self.state = "ADVERTENCIA"
                    self.sub_type = "RECUPERACION_POST_CRISIS"
                    logger.info(f"[{self.bed_id}] Intervencion medica exitosa: CRITICO -> ADVERTENCIA.")
                else:
                    self.state = "ESTABLE"
                    self.sub_type = None
                    logger.info(f"[{self.bed_id}] Paciente reanimado y estabilizado: CRITICO -> ESTABLE.")
                self.cycles_in_state = 0
                self.max_cycles_in_state = random.randint(25, 40)

        elif self.state == "FALLO_SENSOR":
            # Simula reconexion del electrodo por personal biomedico / enfermeria
            if self.cycles_in_state >= self.max_cycles_in_state:
                self.state = "ESTABLE"
                self.sub_type = None
                self.cycles_in_state = 0
                self.max_cycles_in_state = random.randint(25, 40)
                logger.info(f"[{self.bed_id}] Electrodo reajustado correctamente. Senal normalizada.")

    def generate_sample(self) -> dict:
        """Calcula los signos vitales segun el estado fisiologico actual y ruido biometrico."""
        self.transition_state()

        if self.state == "FALLO_SENSOR":
            # Falla de hardware: electrodo caido (0.0 en FC y SpO2)
            hr = 0.0
            spo2 = 0.0
            sbp = round(random.gauss(self.base_sbp, 1.5), 1)
            dbp = round(random.gauss(self.base_dbp, 1.0), 1)
            rr = round(random.gauss(self.base_rr, 0.8), 1)

        elif self.state == "CRITICO":
            if self.sub_type == "ARRITMIA_VENTRICULAR":
                # Taquicardia severa y caida de saturacion
                hr = round(random.gauss(152.0, 4.0), 1)
                spo2 = round(max(75.0, min(88.0, random.gauss(84.0, 1.5))), 1)
                sbp = round(max(60.0, min(85.0, random.gauss(75.0, 2.5))), 1)
                dbp = round(max(38.0, min(55.0, random.gauss(46.0, 2.0))), 1)
                rr = round(max(26.0, min(36.0, random.gauss(30.0, 1.5))), 1)
            else:
                # Shock hemodinamico / colapso circulatorio (PAM muy baja)
                hr = round(random.gauss(138.0, 3.5), 1)
                spo2 = round(max(80.0, min(89.0, random.gauss(86.0, 1.2))), 1)
                sbp = round(max(62.0, min(78.0, random.gauss(70.0, 2.0))), 1)
                dbp = round(max(35.0, min(48.0, random.gauss(42.0, 1.8))), 1)
                rr = round(max(24.0, min(32.0, random.gauss(28.0, 1.2))), 1)

        elif self.state == "ADVERTENCIA":
            if self.sub_type == "TAQUICARDIA":
                hr = round(random.gauss(108.0, 2.5), 1)
                spo2 = round(max(92.0, min(97.0, random.gauss(94.5, 0.8))), 1)
                sbp = round(random.gauss(self.base_sbp + 10.0, 2.0), 1)
                dbp = round(random.gauss(self.base_dbp + 5.0, 1.5), 1)
                rr = round(random.gauss(21.0, 1.0), 1)
            elif self.sub_type == "HIPOXIA_MODERADA":
                hr = round(random.gauss(96.0, 2.0), 1)
                spo2 = round(max(89.0, min(92.5, random.gauss(91.0, 0.8))), 1)
                sbp = round(random.gauss(self.base_sbp, 2.0), 1)
                dbp = round(random.gauss(self.base_dbp, 1.5), 1)
                rr = round(random.gauss(24.0, 1.2), 1)
            else:
                # Pre-hipertension o recuperacion
                hr = round(random.gauss(self.base_hr + 8.0, 2.0), 1)
                spo2 = round(max(93.0, min(98.0, random.gauss(95.5, 0.5))), 1)
                sbp = round(random.gauss(142.0, 2.5), 1)
                dbp = round(random.gauss(92.0, 2.0), 1)
                rr = round(random.gauss(19.0, 1.0), 1)

        else:
            # Fisiologia normal con variabilidad de frecuencia cardiaca (HRV gaussiana)
            hr = round(max(50.0, min(100.0, random.gauss(self.base_hr, 1.5))), 1)
            spo2 = round(max(95.0, min(100.0, random.gauss(self.base_spo2, 0.3))), 1)
            sbp = round(max(95.0, min(135.0, random.gauss(self.base_sbp, 2.0))), 1)
            dbp = round(max(60.0, min(88.0, random.gauss(self.base_dbp, 1.5))), 1)
            rr = round(max(12.0, min(20.0, random.gauss(self.base_rr, 0.6))), 1)

        event_id = str(uuid.uuid4())
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        return {
            "event_id": event_id,
            "clinic_id": self.clinic_id,
            "patient_id": self.patient_id,
            "bed_id": self.bed_id,
            "timestamp": timestamp,
            "heart_rate_bpm": hr,
            "spo2_percent": spo2,
            "systolic_bp": sbp,
            "diastolic_bp": dbp,
            "respiratory_rate": rr,
            "device_id": self.device_id
        }


def create_kafka_producer(servers: str, max_retries: int = 20, delay_sec: int = 3):
    """Establece conexion resiliente con Apache Kafka KRaft."""
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
            logger.info(f"Conexion establecida con Kafka (intento {attempt}).")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka aun no disponible (intento {attempt}/{max_retries}). Reintentando en {delay_sec}s...")
            time.sleep(delay_sec)
        except Exception as e:
            logger.error(f"Error inesperado al conectar con Kafka: {e}")
            time.sleep(delay_sec)

    logger.critical("No fue posible conectar con Kafka tras multiples intentos.")
    sys.exit(1)


def run_producer():
    """Bucle principal de emision autonoma de telemetria de las 12 camas."""
    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    
    # Instanciamos los modelos de estado fisiologico para cada cama
    active_beds = [BedSimulationState(b) for b in INITIAL_BEDS]
    
    logger.info(f"Iniciando emision de telemetria fisiologica autonoma hacia topic '{KAFKA_TOPIC}'...")
    logger.info(f"Camas activas: {len(active_beds)} en red hospitalaria (Central y Norte). Cero Mocks.")
    logger.info("Motor de Markov activo: dinamicas espontaneas de crisis, reanimacion y recuperacion.")

    total_emitted = 0
    try:
        while True:
            cycle_start = time.time()
            for bed_model in active_beds:
                payload = bed_model.generate_sample()
                # Orden por paciente en la particion de Kafka
                producer.send(
                    topic=KAFKA_TOPIC,
                    key=payload["patient_id"],
                    value=payload
                )
                total_emitted += 1

            producer.flush()
            if total_emitted % 120 == 0:
                logger.info(f"[Telemetria Activa] {total_emitted} eventos emitidos a Kafka KRaft.")

            elapsed = time.time() - cycle_start
            sleep_time = max(0.0, EMISSION_INTERVAL_SEC - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Detencion manual del productor.")
    finally:
        producer.close()
        logger.info("Conexion con Kafka cerrada limpiamente.")


if __name__ == "__main__":
    run_producer()
