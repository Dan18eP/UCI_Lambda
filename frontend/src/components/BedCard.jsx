import React from 'react';

/**
 * Tarjeta de Monitoreo Individual por Cama UCI.
 * Cumple: BUSINESS_LOGIC.md (Visualización de 12 camas, semaforización clínica y alertas).
 */
export default function BedCard({ bed, onOpenAssistant }) {
  const isSensorFault = !bed.is_valid_sensor;
  const isCritical = bed.is_critical_alert;
  const isWarning = !isCritical && !isSensorFault && (
    bed.heart_rate_bpm > 100 || bed.spo2_percent < 95 || bed.systolic_bp > 130
  );

  let cardClass = 'bed-card';
  let statusBadge = <span className="status-badge stable">ESTABLE</span>;

  if (isSensorFault) {
    cardClass += ' sensor-fault';
    statusBadge = <span className="status-badge sensor">FALLO SENSOR</span>;
  } else if (isCritical) {
    cardClass += ' critical';
    statusBadge = <span className="status-badge critical">CRÍTICO</span>;
  } else if (isWarning) {
    cardClass += ' warning';
    statusBadge = <span className="status-badge warning">ADVERTENCIA</span>;
  }

  return (
    <div className={cardClass}>
      <div className="card-header">
        <div>
          <div className="bed-title">{bed.bed_id}</div>
          <span className="patient-badge">{bed.patient_id} • {bed.clinic_id}</span>
        </div>
        {statusBadge}
      </div>

      <div className="card-body">
        <div className="vitals-row">
          <div className="vital-metric">
            <div className="vital-label">Frec. Cardíaca</div>
            <div className="vital-value" style={{ color: isCritical && bed.heart_rate_bpm > 140 ? '#ef4444' : '#f8fafc' }}>
              {isSensorFault ? '--' : bed.heart_rate_bpm}
              <span className="vital-unit">bpm</span>
            </div>
            {bed.baseline_avg_hr && (
              <div style={{ fontSize: '0.7rem', color: '#94a3b8', marginTop: '2px' }}>
                Basal 24h: {bed.baseline_avg_hr} bpm
              </div>
            )}
          </div>

          <div className="vital-metric">
            <div className="vital-label">Saturación SpO2</div>
            <div className="vital-value" style={{ color: isCritical && bed.spo2_percent < 88 ? '#ef4444' : '#f8fafc' }}>
              {isSensorFault ? '--' : `${bed.spo2_percent}%`}
            </div>
            {bed.baseline_avg_spo2 && (
              <div style={{ fontSize: '0.7rem', color: '#94a3b8', marginTop: '2px' }}>
                Basal: {bed.baseline_avg_spo2}%
              </div>
            )}
          </div>
        </div>

        <div className="vitals-row">
          <div className="vital-metric">
            <div className="vital-label">Presión Arterial</div>
            <div className="vital-value" style={{ fontSize: '1.25rem' }}>
              {isSensorFault ? '--/--' : `${Math.round(bed.systolic_bp)}/${Math.round(bed.diastolic_bp)}`}
              <span className="vital-unit">mmHg</span>
            </div>
            {bed.baseline_avg_sbp && (
              <div style={{ fontSize: '0.7rem', color: '#94a3b8', marginTop: '2px' }}>
                Basal SIS: {bed.baseline_avg_sbp}
              </div>
            )}
          </div>

          <div className="vital-metric">
            <div className="vital-label">Frec. Respiratoria</div>
            <div className="vital-value" style={{ fontSize: '1.25rem' }}>
              {isSensorFault ? '--' : bed.respiratory_rate}
              <span className="vital-unit">rpm</span>
            </div>
          </div>
        </div>

        {/* Notificación de Alerta o Diagnóstico */}
        {(isCritical || isSensorFault || isWarning) && (
          <div className="alert-box" style={{
            backgroundColor: isCritical ? 'rgba(239, 68, 68, 0.15)' : isSensorFault ? 'rgba(139, 92, 246, 0.15)' : 'rgba(245, 158, 11, 0.15)',
            borderColor: isCritical ? 'rgba(239, 68, 68, 0.4)' : isSensorFault ? 'rgba(139, 92, 246, 0.4)' : 'rgba(245, 158, 11, 0.4)',
            color: isCritical ? '#fca5a5' : isSensorFault ? '#d8b4fe' : '#fde68a'
          }}>
            <strong>{isSensorFault ? '🔧 Sensor:' : '🚨 Alerta:'}</strong> {bed.alert_reason}
          </div>
        )}
      </div>

      <div className="card-footer">
        <span style={{ fontSize: '0.75rem', color: '#64748b' }}>
          {bed.device_id}
        </span>
        <button
          className="ml-button"
          onClick={() => onOpenAssistant(bed)}
          title="Ejecutar inferencia del modelo tabular (<3ms)"
        >
          🤖 Asistente ML
        </button>
      </div>
    </div>
  );
}
