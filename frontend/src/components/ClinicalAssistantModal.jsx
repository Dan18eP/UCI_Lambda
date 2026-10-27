import React, { useState, useEffect } from 'react';
import { BrainIcon, ZapIcon } from './Icons';

/**
 * Modal de Asistencia Diagnostica con Modelo Tabular ML (<3ms).
 * Cumple: ADR-008 & BUSINESS_LOGIC.md Seccion 7 (Cero emojis).
 */
export default function ClinicalAssistantModal({ bed, onClose }) {
  const [loading, setLoading] = useState(true);
  const [evaluation, setEvaluation] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function runEvaluation() {
      try {
        setLoading(true);
        const res = await fetch('http://localhost:8000/api/v1/clinical-assistant/evaluate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ patient_id: bed.patient_id })
        });

        if (!res.ok) {
          throw new Error(`Error en evaluacion: ${res.statusText}`);
        }

        const data = await res.json();
        setEvaluation(data);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    }

    if (bed?.patient_id) {
      runEvaluation();
    }
  }, [bed]);

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <div>
            <h3 style={{ fontSize: '1.1rem', fontWeight: 700, display: 'flex', alignItems: 'center', gap: '8px' }}>
              <BrainIcon size={18} /> ASISTENTE CLINICO ML - {bed.bed_id}
            </h3>
            <span style={{ fontSize: '0.8rem', color: '#94a3b8' }}>
              Paciente: {bed.patient_id} | Sede: {bed.clinic_id}
            </span>
          </div>
          <button className="modal-close" onClick={onClose}>&times;</button>
        </div>

        <div className="modal-body">
          {loading && (
            <div style={{ textAlign: 'center', padding: '30px' }}>
              <p style={{ color: '#94a3b8' }}>Ejecutando inferencia con clasificador tabular...</p>
            </div>
          )}

          {error && (
            <div style={{ padding: '14px', backgroundColor: 'rgba(239, 68, 68, 0.15)', border: '1px solid #ef4444', borderRadius: '6px' }}>
              <strong>Error:</strong> {error}
            </div>
          )}

          {evaluation && (
            <>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                  <span style={{ fontSize: '0.75rem', color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.5px' }}>NIVEL DE RIESGO</span>
                  <div style={{ fontSize: '1.3rem', fontWeight: 800, color: evaluation.risk_level === 'CRITICO' ? '#ef4444' : evaluation.risk_level === 'ALTO' ? '#f59e0b' : '#10b981' }}>
                    {evaluation.risk_level} ({evaluation.risk_score}/100)
                  </div>
                </div>
                <div style={{ textAlign: 'right' }}>
                  <span className="latency-tag" style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
                    <ZapIcon size={12} /> Latencia: {evaluation.inference_latency_ms} ms
                  </span>
                  <div style={{ fontSize: '0.75rem', color: '#64748b', marginTop: '4px' }}>
                    *Cumple ADR-008 (&lt; 3 ms)
                  </div>
                </div>
              </div>

              {/* Factores Fisiologicos */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '10px' }}>
                <div style={{ backgroundColor: 'rgba(0,0,0,0.3)', padding: '10px', borderRadius: '6px' }}>
                  <div style={{ fontSize: '0.75rem', color: '#94a3b8' }}>PAM (PRESION MEDIA)</div>
                  <div style={{ fontSize: '1.1rem', fontWeight: 700 }}>
                    {evaluation.clinical_factors.mean_arterial_pressure} <span style={{ fontSize: '0.75rem' }}>mmHg</span>
                  </div>
                </div>
                <div style={{ backgroundColor: 'rgba(0,0,0,0.3)', padding: '10px', borderRadius: '6px' }}>
                  <div style={{ fontSize: '0.75rem', color: '#94a3b8' }}>INDICE DE SHOCK (SI)</div>
                  <div style={{ fontSize: '1.1rem', fontWeight: 700, color: evaluation.clinical_factors.shock_index > 0.9 ? '#ef4444' : '#f8fafc' }}>
                    {evaluation.clinical_factors.shock_index}
                  </div>
                </div>
                <div style={{ backgroundColor: 'rgba(0,0,0,0.3)', padding: '10px', borderRadius: '6px' }}>
                  <div style={{ fontSize: '0.75rem', color: '#94a3b8' }}>DELTA FC (vs BASAL)</div>
                  <div style={{ fontSize: '1.1rem', fontWeight: 700 }}>
                    {evaluation.clinical_factors.hr_delta_from_baseline > 0 ? `+${evaluation.clinical_factors.hr_delta_from_baseline}` : evaluation.clinical_factors.hr_delta_from_baseline} <span style={{ fontSize: '0.75rem' }}>lpm</span>
                  </div>
                </div>
              </div>

              {/* Recomendacion Clinica */}
              <div style={{
                backgroundColor: evaluation.risk_level === 'CRITICO' ? 'rgba(239, 68, 68, 0.12)' : 'rgba(59, 130, 246, 0.12)',
                border: `1px solid ${evaluation.risk_level === 'CRITICO' ? 'rgba(239, 68, 68, 0.35)' : 'rgba(59, 130, 246, 0.35)'}`,
                padding: '12px 14px',
                borderRadius: '6px',
                fontSize: '0.85rem',
                lineHeight: '1.5'
              }}>
                <strong style={{ display: 'block', marginBottom: '4px' }}>Recomendacion Diagnostica:</strong>
                <p>{evaluation.recommendation}</p>
              </div>

              {/* Distribucion de Probabilidades */}
              <div>
                <span style={{ fontSize: '0.75rem', color: '#94a3b8' }}>DISTRIBUCION DE PROBABILIDAD (SCIKIT-LEARN):</span>
                <div style={{ display: 'flex', gap: '8px', marginTop: '6px', fontSize: '0.75rem' }}>
                  <span style={{ color: '#10b981' }}>Bajo: {(evaluation.probabilities.bajo * 100).toFixed(1)}%</span>
                  <span style={{ color: '#38bdf8' }}>Moderado: {(evaluation.probabilities.moderado * 100).toFixed(1)}%</span>
                  <span style={{ color: '#f59e0b' }}>Alto: {(evaluation.probabilities.alto * 100).toFixed(1)}%</span>
                  <span style={{ color: '#ef4444' }}>Critico: {(evaluation.probabilities.critico * 100).toFixed(1)}%</span>
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
