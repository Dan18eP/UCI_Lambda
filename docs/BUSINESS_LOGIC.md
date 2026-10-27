# 🩺 Lógica de Negocio (Business Logic) - Monitoreo Crítico UCI

Documento de especificación funcional, reglas clínicas, validaciones fisiológicas y comportamiento del sistema ante eventos para el **Monitoreo de Signos Vitales en Tiempo Real y Análisis Histórico en UCI**.

---

## 🎯 1. Propósito Clínico y Desafío de Negocio

En las Unidades de Cuidados Intensivos (UCI) y salas de reanimación, la velocidad de respuesta médica ante el deterioro agudo de un paciente determina su supervivencia:
* **Paro cardiorrespiratorio y arritmias malignas:** El daño cerebral irreversible comienza tras 3-4 minutos de hipoxia o colapso hemodinámico.
* **El problema de las arquitecturas tradicionales:** Los sistemas basados en lotes (batch) analizan la información con retrasos de 15 a 60 minutos, lo que resulta fatal en emergencias agudas.
* **Por qué Arquitectura Lambda:**
  * **Capa Speed:** Detecta en menos de 500 milisegundos anomalías fisiológicas agudas y empuja alertas críticas vía WebSocket directo a la pantalla del personal médico de guardia.
  * **Capa Batch:** Procesa el volumen acumulado en el Data Lake (MinIO) para calcular los valores basales del paciente (su "normalidad individual"), tendencias de deterioro paulatino (ej. evolución de sepsis a lo largo de 24 horas) y almacenar la verdad inmutable para auditoría médico-legal y entrenamiento de algoritmos predictivos.

---

## 👤 2. Entidades del Dominio

```
┌────────────────────────────────┐       ┌────────────────────────────────┐
│            PACIENTE            │       │         UBICACIÓN / CAMA       │
├────────────────────────────────┤       ├────────────────────────────────┤
│ • patient_id: ID único clínico │◄─────►│ • bed_id: Código de cama UCI   │
│ • clinic_id: Código de clínica │       │ • clinic_id: Red hospitalaria  │
│ • admission_time: Ingreso UCI  │       │ • unit_id: Unidad (UCI-Adultos)│
└────────────────────────────────┘       └────────────────────────────────┘
                 ▲                                       ▲
                 │                                       │
                 └───────────────┬───────────────────────┘
                                 │ Genera eventos continuos
                                 ▼
                 ┌────────────────────────────────┐
                 │       EVENTO DE SIGNOS VITALES │
                 ├────────────────────────────────┤
                 │ • event_id (UUID)              │
                 │ • clinic_id (Sede hospitalaria)│
                 │ • patient_id                   │
                 │ • bed_id                       │
                 │ • timestamp (ISO-8601 UTC)     │
                 │ • heart_rate_bpm (lpm)         │
                 │ • spo2_percent (%)             │
                 │ • systolic_bp (mmHg)           │
                 │ • diastolic_bp (mmHg)          │
                 │ • respiratory_rate (rpm)       │
                 │ • device_id (Monitor físico)   │
                 └────────────────────────────────┘
```

---

## 🚨 3. Reglas Fisiológicas y Umbrales de Alerta (Speed Layer)

La Capa Speed evalúa cada evento recibido contra los siguientes parámetros estandarizados de medicina intensiva:

### Tabla de Umbrales Clínicos

| Signo Vital | Rango Normal (Verde) | Advertencia (Amarillo) | Alerta Crítica (Rojo) | Acción Inmediata del Sistema |
| :--- | :--- | :--- | :--- | :--- |
| **Frecuencia Cardíaca (`heart_rate_bpm`)** | 60 - 100 bpm | 50 - 59 bpm (Bradicardia leve)<br/>101 - 130 bpm (Taquicardia leve) | **< 45 bpm** (Bradicardia severa / Bloqueo)<br/>**> 140 bpm** (Taquicardia ventricular/supra) | Notificación push WebSocket inmediata a estación médica. |
| **Saturación de Oxígeno (`spo2_percent`)** | 95% - 100% | 90% - 94% (Hipoxia moderada) | **< 88%** (Insuficiencia respiratoria aguda) | Notificación push roja con alerta sonora visual. |
| **Presión Arterial Sistólica (`systolic_bp`)** | 90 - 120 mmHg | 121 - 140 mmHg (Pre-hipertensión)<br/>85 - 89 mmHg (Hipotensión limítrofe) | **< 80 mmHg** (Shock hemodinámico/séptico)<br/>**> 180 mmHg** (Crisis hipertensiva) | Disparo de bandera crítica `is_critical_alert = true`. |
| **Frecuencia Respiratoria (`respiratory_rate`)** | 12 - 20 rpm | 21 - 25 rpm (Taquipnea) | **< 8 rpm** (Depresión respiratoria)<br/>**> 30 rpm** (Fallo ventilatorio inminente) | Alerta respiratoria prioritaria. |

### Lógica de Disparo en la Capa Speed
```python
def evaluate_clinical_alert(vitals):
    hr = vitals.get("heart_rate_bpm", 0)
    spo2 = vitals.get("spo2_percent", 0)
    sys_bp = vitals.get("systolic_bp", 0)
    
    # Detección de estados críticos
    if hr > 140:
        return True, f"Taquicardia severa crítica: {hr} bpm"
    if hr < 45 and hr > 0:
        return True, f"Bradicardia severa extrema: {hr} bpm"
    if spo2 < 88.0 and spo2 > 0:
        return True, f"Hipoxia aguda severa: SpO2 {spo2}%"
    if sys_bp < 80.0 and sys_bp > 0:
        return True, f"Colapso hemodinámico / Shock: Sistólica {sys_bp} mmHg"
    if sys_bp > 180.0:
        return True, f"Crisis hipertensiva crítica: Sistólica {sys_bp} mmHg"
        
    return False, "Estable"
```

---

## 🛠️ 4. Reglas de Calidad y Fallos de Sensor (Cero Cuarentena)

Para evitar la contaminación de métricas sin utilizar el anti-patrón de "carpetas de cuarentena":

### Clasificación entre Fallo de Hardware vs Emergencia Médica
* **Fallo de Hardware (Sensor desconectado o electrodo caído):**
  * `heart_rate_bpm <= 0` o `heart_rate_bpm > 280`
  * `spo2_percent <= 0` o `spo2_percent > 100`
  * `systolic_bp < 20` o `systolic_bp > 300`
* **Tratamiento Arquitectónico:**
  1. El evento crudo **se almacena inmutablemente en el Master Dataset de MinIO** para trazabilidad técnica del dispositivo.
  2. En el procesamiento, se le asigna el indicador `is_valid_sensor = false`.
  3. **La Capa Speed:** NO genera alerta roja médica de taquicardia si el sensor marca 999 bpm; en su lugar, actualiza el estado a `FALLO_SENSOR` para avisar al personal de enfermería que revise el cable.
  4. **La Capa Batch:** Excluye estos registros al calcular promedios y basales fisiológicos, pero totaliza la métrica `sensor_errors_detected` para el informe de mantenimiento de equipos biomédicos.

---

## 📈 5. Lógica Analítica de la Capa Batch (Airflow + Spark)

Mientras la Capa Speed evalúa instantes puntuales, la Capa Batch genera la **línea base (baseline)** del paciente cada hora:

1. **Cálculo de Línea Base:**
   * Promedio móvil de frecuencia cardíaca y presión arterial en ventanas de 1 hora y 24 horas.
   * Rango dinámico: `min_heart_rate`, `max_heart_rate`, `stddev_heart_rate`.
2. **Contexto para la Toma de Decisiones:**
   * Una frecuencia cardíaca de 110 bpm en un paciente con marcapasos o taquicardia crónica puede no ser una emergencia.
   * Sin embargo, si su promedio histórico de las últimas 24 horas era de 65 bpm, un salto repentino a 110 bpm evidencia deterioro clínico que la Capa de Servicio resalta al contrastar ambas vistas.
3. **Escritura Idempotente:**
   * La Batch Layer escribe en `vitals_historico` utilizando `.mode("overwrite")` sobre la partición del paciente y la hora calculada.

---

## 🌐 6. Política de Integración Frontend y "Cero Mocks"

El frontend opera bajo tres estados formales:

1. **Estado `NO_DATA` (Pipeline inicializando):**
   * Si la tabla de servicio está vacía porque ni Spark ni Airflow han procesado datos aún, el backend responde:
     ```json
     {
       "status": "NO_DATA",
       "message": "Sistema en inicialización. Esperando conexión con monitores UCI y consolidación de eventos."
     }
     ```
   * El frontend muestra un panel amarillo de alerta arquitectónica para auditoría del TL.
2. **Estado `ESTABLE` (Conexión activa, sin alertas críticas):**
   * Tarjetas de pacientes con fondo verde y métricas en vivo actualizándose.
3. **Estado `ALERTA_CRÍTICA` (Disparo inmediato):**
   * El WebSocket emite un payload tipo `CRITICAL_ALERT`:
     ```json
     {
       "type": "CRITICAL_ALERT",
       "patient_id": "PAT-1082",
       "bed_id": "UCI-BED-04",
       "vital_type": "HEART_RATE",
       "current_value": 145.0,
       "baseline_avg": 72.4,
       "alert_reason": "Taquicardia severa crítica: 145 bpm",
       "timestamp": "2026-10-26T21:18:00.000Z"
     }
     ```
   * La interfaz parpadea en rojo y sitúa al paciente al inicio de la lista de prioridad médica.

---

## 🤖 7. Modelo Predictivo Tabular de Deterioro Clínico (Scikit-Learn / XGBoost)

Para apoyar al personal médico sin introducir latencias en el streaming de Spark, el sistema incorpora un clasificador tabular ultrarrápido (< 3 ms de inferencia) desacoplado en FastAPI.

### A. Vector de Características (Features)
El modelo procesa 8 variables numéricas obtenidas del cruce entre `vitals_realtime` y `vitals_historico`:

1. **Frecuencia Cardíaca Actual (`heart_rate_bpm`):** Ritmo en el instante evaluado.
2. **Saturación Actual (`spo2_percent`):** Nivel de oxigenación periférica.
3. **Presión Sistólica Actual (`systolic_bp`):** Presión máxima arterial.
4. **Presión Diastólica Actual (`diastolic_bp`):** Presión mínima arterial.
5. **Frecuencia Respiratoria Actual (`respiratory_rate`):** Ventilación por minuto.
6. **Presión Arterial Media (PAM / MAP):** Indicador clave de perfusión de órganos vitales:
   $$\text{MAP} = \frac{2 \times \text{diastolic\_bp} + \text{systolic\_bp}}{3}$$
7. **Índice de Shock (Shock Index - SI):** Marcador temprano de shock hemodinámico o hipovolemia:
   $$\text{Shock Index} = \frac{\text{heart\_rate\_bpm}}{\text{systolic\_bp}}$$
   *(Normal: 0.5 - 0.7; Alerta temprana: > 0.8; Shock severo: > 1.0)*
8. **Delta de Frecuencia Cardíaca respecto al Basal 24h (`hr_delta_from_baseline`):**
   $$\Delta \text{HR} = \text{heart\_rate\_bpm} - \text{baseline\_avg\_hr}$$

### B. Contrato de Respuesta de la API (`POST /api/v1/clinical-assistant/evaluate`)
```json
{
  "patient_id": "PAT-1082",
  "clinic_id": "CLI-CENTRAL-01",
  "bed_id": "UCI-BED-04",
  "evaluation_timestamp": "2026-10-26T22:08:00.150Z",
  "inference_latency_ms": 1.85,
  "risk_score": 82.4,
  "risk_level": "CRITICO",
  "probabilities": {
    "bajo": 0.03,
    "moderado": 0.05,
    "alto": 0.10,
    "critico": 0.82
  },
  "clinical_factors": {
    "mean_arterial_pressure": 65.0,
    "shock_index": 1.58,
    "hr_delta_from_baseline": 62.6
  },
  "recommendation": "Alerta de inestabilidad hemodinámica aguda y sospecha de shock (Shock Index: 1.58). Notificar de inmediato al médico intensivista."
}
```
