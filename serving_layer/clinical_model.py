"""
Modelo Predictivo Tabular de Deterioro Fisiológico en UCI.
Cumple: ADR-008 (Scikit-Learn, Inferencia < 3 ms, Desacoplado de la Speed Layer).
"""

import time
import uuid
import datetime
import numpy as np
from sklearn.ensemble import RandomForestClassifier

RISK_LEVELS = ["BAJO", "MODERADO", "ALTO", "CRITICO"]


class ClinicalDeteriorationModel:
    def __init__(self):
        self.model = self._train_calibrated_model()

    def _train_calibrated_model(self) -> RandomForestClassifier:
        np.random.seed(42)
        n_samples = 2000
        X = []
        y = []

        for _ in range(n_samples):
            tier = np.random.choice([0, 1, 2, 3], p=[0.45, 0.25, 0.18, 0.12])
            if tier == 0:
                hr = np.random.normal(72, 8)
                spo2 = np.random.normal(98, 1)
                sbp = np.random.normal(118, 10)
                dbp = np.random.normal(75, 6)
                rr = np.random.normal(15, 2)
                hr_delta = np.random.normal(2, 5)
            elif tier == 1:
                hr = np.random.normal(95, 10)
                spo2 = np.random.normal(93, 1.5)
                sbp = np.random.normal(132, 12)
                dbp = np.random.normal(84, 8)
                rr = np.random.normal(21, 2.5)
                hr_delta = np.random.normal(15, 8)
            elif tier == 2:
                hr = np.random.normal(125, 12)
                spo2 = np.random.normal(89, 2)
                sbp = np.random.normal(88, 8)
                dbp = np.random.normal(55, 6)
                rr = np.random.normal(27, 3)
                hr_delta = np.random.normal(35, 10)
            else:
                hr = np.random.normal(148, 15)
                spo2 = np.random.normal(84, 3)
                sbp = np.random.normal(72, 8)
                dbp = np.random.normal(45, 6)
                rr = np.random.normal(32, 4)
                hr_delta = np.random.normal(55, 12)

            hr = max(30.0, hr)
            spo2 = max(50.0, min(100.0, spo2))
            sbp = max(40.0, sbp)
            dbp = max(20.0, dbp)
            rr = max(5.0, rr)
            
            map_val = (2.0 * dbp + sbp) / 3.0
            shock_idx = hr / sbp if sbp > 0 else 2.0

            X.append([hr, spo2, sbp, dbp, rr, map_val, shock_idx, hr_delta])
            y.append(tier)

        X = np.array(X)
        y = np.array(y)
        clf = RandomForestClassifier(n_estimators=30, max_depth=6, random_state=42)
        clf.fit(X, y)
        return clf

    def evaluate_patient(
        self,
        patient_id: str,
        clinic_id: str,
        bed_id: str,
        heart_rate_bpm: float,
        spo2_percent: float,
        systolic_bp: float,
        diastolic_bp: float,
        respiratory_rate: float,
        baseline_avg_hr: float
    ) -> dict:
        start_time = time.perf_counter()

        mean_arterial_pressure = round((2.0 * diastolic_bp + systolic_bp) / 3.0, 2)
        shock_index = round(heart_rate_bpm / systolic_bp if systolic_bp > 0 else 2.0, 2)
        hr_delta_from_baseline = round(heart_rate_bpm - baseline_avg_hr, 2)

        feature_vector = np.array([[
            heart_rate_bpm,
            spo2_percent,
            systolic_bp,
            diastolic_bp,
            respiratory_rate,
            mean_arterial_pressure,
            shock_index,
            hr_delta_from_baseline
        ]])

        probs = self.model.predict_proba(feature_vector)[0]
        predicted_tier_idx = int(np.argmax(probs))
        risk_level = RISK_LEVELS[predicted_tier_idx]

        risk_score = round(float(
            probs[0] * 10.0 +
            probs[1] * 40.0 +
            probs[2] * 75.0 +
            probs[3] * 98.0
        ), 1)

        end_time = time.perf_counter()
        latency_ms = round((end_time - start_time) * 1000.0, 2)

        if risk_level == "CRITICO":
            recommendation = (
                f"ALERTA CRITICA: Inestabilidad hemodinamica y sospecha de shock "
                f"(Indice de Shock: {shock_index:.2f}, Delta FC: +{hr_delta_from_baseline:.1f} lpm sobre basal). "
                f"Notificar de inmediato al intensivista."
            )
        elif risk_level == "ALTO":
            recommendation = (
                f"RIESGO ALTO: Perfusion comprometida (PAM: {mean_arterial_pressure:.1f} mmHg, "
                f"SpO2: {spo2_percent:.1f}%). Preparar soporte hemodinamico o ventilatorio."
            )
        elif risk_level == "MODERADO":
            recommendation = (
                f"ATENCION: Desviacion de parametros basales (Delta FC: +{hr_delta_from_baseline:.1f} lpm). "
                f"Incrementar vigilancia continua."
            )
        else:
            recommendation = "ESTABLE: Parametros dentro de rangos basales normales. Continuar protocolo estandar."

        return {
            "prediction_id": str(uuid.uuid4()),
            "patient_id": patient_id,
            "clinic_id": clinic_id,
            "bed_id": bed_id,
            "evaluation_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "inference_latency_ms": latency_ms,
            "risk_score": risk_score,
            "risk_level": risk_level,
            "probabilities": {
                "bajo": round(float(probs[0]), 3),
                "moderado": round(float(probs[1]), 3),
                "alto": round(float(probs[2]), 3),
                "critico": round(float(probs[3]), 3)
            },
            "clinical_factors": {
                "mean_arterial_pressure": mean_arterial_pressure,
                "shock_index": shock_index,
                "hr_delta_from_baseline": hr_delta_from_baseline
            },
            "recommendation": recommendation
        }


clinical_model = ClinicalDeteriorationModel()
