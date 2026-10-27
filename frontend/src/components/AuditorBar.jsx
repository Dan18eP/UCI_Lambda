import React from 'react';
import { ZapIcon, ServerIcon, ClockIcon, DatabaseIcon, ShieldIcon } from './Icons';

/**
 * Barra Superior de Auditoria para el Team Leader (TL).
 * Cumple: ADR-006 (Enlaces directos a infraestructura, Cero botones de disparo batch, Cero emojis).
 */
export default function AuditorBar() {
  return (
    <div className="auditor-bar">
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        <strong style={{ color: '#cbd5e1', letterSpacing: '0.5px' }}>CONSOLA DE AUDITORIA LAMBDA</strong>
        <span style={{ color: '#64748b' }}>| Telemetria UCI</span>
      </div>

      <div className="auditor-links">
        <a href="http://localhost:4040" target="_blank" rel="noreferrer" className="auditor-link" title="Metricas de Input y Processing Rate">
          <ZapIcon size={14} /> Spark UI (4040)
        </a>
        <a href="http://localhost:8080" target="_blank" rel="noreferrer" className="auditor-link" title="Cluster Spark Master">
          <ServerIcon size={14} /> Spark Master (8080)
        </a>
        <a href="http://localhost:8085" target="_blank" rel="noreferrer" className="auditor-link" title="Orquestacion Horaria Batch">
          <ClockIcon size={14} /> Airflow (8085)
        </a>
        <a href="http://localhost:9001" target="_blank" rel="noreferrer" className="auditor-link" title="Master Dataset Parquet">
          <DatabaseIcon size={14} /> MinIO Data Lake (9001)
        </a>
        <a href="http://localhost:21000" target="_blank" rel="noreferrer" className="auditor-link" title="Linaje y Gobernanza">
          <ShieldIcon size={14} /> Apache Atlas (21000)
        </a>
      </div>
    </div>
  );
}
