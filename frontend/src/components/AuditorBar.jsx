import React from 'react';

/**
 * Barra Superior de Auditoría para el Team Leader (TL).
 * Cumple: ADR-006 (Enlaces directos a infraestructura, Cero botones de disparo batch).
 */
export default function AuditorBar() {
  return (
    <div className="auditor-bar">
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        <span style={{ fontSize: '1rem' }}>🏛️</span>
        <strong style={{ color: '#cbd5e1' }}>Consola de Auditoría Lambda</strong>
        <span style={{ color: '#64748b' }}>| Monitoreo UCI (Cero Mocks)</span>
      </div>

      <div className="auditor-links">
        <a href="http://localhost:4040" target="_blank" rel="noreferrer" className="auditor-link" title="Métricas de Input y Processing Rate">
          ⚡ Spark UI (4040)
        </a>
        <a href="http://localhost:8080" target="_blank" rel="noreferrer" className="auditor-link" title="Cluster Spark Master">
          🚀 Spark Master (8080)
        </a>
        <a href="http://localhost:8085" target="_blank" rel="noreferrer" className="auditor-link" title="Orquestación Horaria Batch">
          🌪️ Airflow (8085)
        </a>
        <a href="http://localhost:9001" target="_blank" rel="noreferrer" className="auditor-link" title="Master Dataset Parquet">
          📦 MinIO Data Lake (9001)
        </a>
        <a href="http://localhost:21000" target="_blank" rel="noreferrer" className="auditor-link" title="Linaje y Gobernanza">
          🛡️ Apache Atlas (21000)
        </a>
      </div>
    </div>
  );
}
