import React, { useState, useEffect } from 'react';
import { DatabaseIcon, ClockIcon, BrainIcon, RefreshCwIcon, AlertTriangleIcon } from './Icons';

const API_BASE = 'http://localhost:8000';

export default function HistoricalAnalyticsView({ onOpenAssistant }) {
  const [loading, setLoading] = useState(true);
  const [analyticsData, setAnalyticsData] = useState(null);
  const [error, setError] = useState(null);
  const [selectedClinic, setSelectedClinic] = useState('');

  const fetchAnalytics = async () => {
    try {
      setLoading(true);
      setError(null);
      const url = selectedClinic
        ? `${API_BASE}/api/v1/analytics/history?clinic_id=${selectedClinic}&hours=24`
        : `${API_BASE}/api/v1/analytics/history?hours=24`;
      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }
      const data = await res.json();
      setAnalyticsData(data.data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchAnalytics();
  }, [selectedClinic]);

  return (
    <div className="historical-analytics-view">
      {/* Cabecera Explicativa de la Capa Batch */}
      <div className="analytics-banner">
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}>
            <DatabaseIcon size={20} color="#3b82f6" />
            <h2 style={{ fontSize: '1.2rem', margin: 0, color: '#f8fafc' }}>
              Analitica Historica Consolidada (Batch Layer)
            </h2>
          </div>
          <p style={{ margin: 0, fontSize: '0.85rem', color: '#94a3b8' }}>
            Consultando la vista inmutable <code>vitals_historico</code> precomputada periodicamente por Apache Airflow desde el Master Dataset en MinIO (Parquet).
          </p>
        </div>

        <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
          <select
            value={selectedClinic}
            onChange={(e) => setSelectedClinic(e.target.value)}
            className="filter-select"
          >
            <option value="">Todas las Sedes</option>
            <option value="CLI-CENTRAL-01">Sede Central</option>
            <option value="CLI-NORTE-02">Sede Norte</option>
          </select>
          <button onClick={fetchAnalytics} className="refresh-button" title="Refrescar vista historica">
            <RefreshCwIcon size={14} /> Refrescar
          </button>
        </div>
      </div>

      {loading && (
        <div style={{ textAlign: 'center', padding: '40px', color: '#94a3b8' }}>
          Cargando metricas consolidadas desde la Capa Batch...
        </div>
      )}

      {error && (
        <div className="error-box">
          <strong>Error al consultar Capa Batch:</strong> {error}
        </div>
      )}

      {!loading && analyticsData && (
        <>
          {/* Tarjetas Comparativas por Sede Institucional */}
          <div className="clinic-summary-grid">
            {analyticsData.institutional_summary?.map((c) => (
              <div key={c.clinic_id} className="clinic-summary-card">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <h3 style={{ margin: 0, fontSize: '1rem', color: '#e2e8f0' }}>
                    {c.clinic_id === 'CLI-CENTRAL-01' ? 'Sede Central (Metropolitana)' : 'Sede Norte (Regional)'}
                  </h3>
                  <span className="badge-tag">{c.clinic_id}</span>
                </div>

                <div className="summary-metrics-row">
                  <div>
                    <span className="summary-metric-label">Pacientes Auditados:</span>
                    <strong className="summary-metric-value">{c.total_patients_recorded}</strong>
                  </div>
                  <div>
                    <span className="summary-metric-label">FC Media:</span>
                    <strong className="summary-metric-value">{c.institutional_avg_hr} bpm</strong>
                  </div>
                  <div>
                    <span className="summary-metric-label">SpO2 Promedio:</span>
                    <strong className="summary-metric-value">{c.institutional_avg_spo2}%</strong>
                  </div>
                  <div>
                    <span className="summary-metric-label">PAM Media:</span>
                    <strong className="summary-metric-value" style={{ color: c.institutional_avg_map < 70 ? '#ef4444' : '#10b981' }}>
                      {c.institutional_avg_map} mmHg
                    </strong>
                  </div>
                  <div>
                    <span className="summary-metric-label">Errores de Sensor:</span>
                    <strong className="summary-metric-value" style={{ color: c.total_sensor_errors > 0 ? '#f59e0b' : '#94a3b8' }}>
                      {c.total_sensor_errors}
                    </strong>
                  </div>
                </div>

                <div style={{ fontSize: '0.75rem', color: '#64748b', marginTop: '10px' }}>
                  Total muestras en Parquet: {c.total_samples_processed?.toLocaleString()} eventos
                </div>
              </div>
            ))}
          </div>

          {/* Tabla de Tendencias Historicas por Paciente */}
          <div className="historical-table-container">
            <div style={{ padding: '16px', borderBottom: '1px solid #334155', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h3 style={{ margin: 0, fontSize: '1rem', color: '#f8fafc', display: 'flex', alignItems: 'center', gap: '8px' }}>
                <ClockIcon size={16} /> Ventanas Horarias Consolidadas por Paciente
              </h3>
              <span style={{ fontSize: '0.8rem', color: '#64748b' }}>
                Registros: {analyticsData.series?.length || 0} lotes
              </span>
            </div>

            <div className="table-responsive">
              <table className="historical-table">
                <thead>
                  <tr>
                    <th>Cama / Paciente</th>
                    <th>Sede</th>
                    <th>Ventana Batch</th>
                    <th>Frec. Cardiaca (Min / Prom / Max)</th>
                    <th>SpO2 Prom (Min)</th>
                    <th>Presion Arterial</th>
                    <th>PAM (Media)</th>
                    <th>Calidad Sensor</th>
                    <th>Acciones</th>
                  </tr>
                </thead>
                <tbody>
                  {analyticsData.series?.map((row, idx) => (
                    <tr key={`${row.patient_id}-${idx}`}>
                      <td>
                        <strong>{row.bed_id}</strong>
                        <div style={{ fontSize: '0.75rem', color: '#94a3b8' }}>{row.patient_id}</div>
                      </td>
                      <td>
                        <span className="clinic-badge">{row.clinic_id}</span>
                      </td>
                      <td style={{ fontSize: '0.8rem', color: '#cbd5e1' }}>
                        {row.hour_timestamp}
                      </td>
                      <td>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                          <span style={{ color: '#94a3b8', fontSize: '0.8rem' }}>{row.min_heart_rate}</span>
                          <strong style={{ color: row.avg_heart_rate > 100 ? '#f59e0b' : '#f8fafc' }}>{row.avg_heart_rate}</strong>
                          <span style={{ color: '#94a3b8', fontSize: '0.8rem' }}>{row.max_heart_rate}</span>
                          <span style={{ fontSize: '0.7rem', color: '#64748b' }}>bpm</span>
                        </div>
                      </td>
                      <td>
                        <strong style={{ color: row.avg_spo2 < 92 ? '#ef4444' : '#f8fafc' }}>{row.avg_spo2}%</strong>
                        <span style={{ fontSize: '0.75rem', color: '#94a3b8', marginLeft: '4px' }}>
                          (min {row.min_spo2}%)
                        </span>
                      </td>
                      <td style={{ fontSize: '0.85rem' }}>
                        {Math.round(row.avg_systolic_bp)} / {Math.round(row.diastolic_bp || row.avg_diastolic_bp)} mmHg
                      </td>
                      <td>
                        <span style={{
                          padding: '3px 8px',
                          borderRadius: '4px',
                          fontWeight: 700,
                          fontSize: '0.85rem',
                          backgroundColor: row.avg_map < 70 ? 'rgba(239, 68, 68, 0.2)' : 'rgba(16, 185, 129, 0.2)',
                          color: row.avg_map < 70 ? '#fca5a5' : '#6ee7b7'
                        }}>
                          {row.avg_map} mmHg
                        </span>
                      </td>
                      <td>
                        {row.sensor_errors_detected > 0 ? (
                          <span style={{ color: '#f59e0b', fontSize: '0.8rem', display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <AlertTriangleIcon size={12} /> {row.sensor_errors_detected} anomalias
                          </span>
                        ) : (
                          <span style={{ color: '#10b981', fontSize: '0.8rem' }}>
                            Optima ({row.valid_samples} ev)
                          </span>
                        )}
                      </td>
                      <td>
                        <button
                          className="consult-btn"
                          onClick={() => onOpenAssistant({ patient_id: row.patient_id, bed_id: row.bed_id, clinic_id: row.clinic_id })}
                          title="Consultar historial de este paciente con el Asistente Batch"
                        >
                          <BrainIcon size={13} /> Consultar
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
