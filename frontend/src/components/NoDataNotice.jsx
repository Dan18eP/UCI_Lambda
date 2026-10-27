import React from 'react';

/**
 * Componente de Aviso de Auditoría cuando la Serving Layer no tiene registros aún.
 * Cumple: ADR-006 & RULES.md (Cero datos falsos o simulados en memoria).
 */
export default function NoDataNotice({ message }) {
  return (
    <div style={{
      backgroundColor: 'rgba(245, 158, 11, 0.1)',
      border: '2px dashed #f59e0b',
      borderRadius: '12px',
      padding: '40px 24px',
      textAlign: 'center',
      margin: '40px auto',
      maxWidth: '800px'
    }}>
      <div style={{ fontSize: '3rem', marginBottom: '16px' }}>⚠️</div>
      <h2 style={{ color: '#fbbf24', fontSize: '1.4rem', marginBottom: '12px' }}>
        Aviso de Auditoría de Arquitectura
      </h2>
      <p style={{ color: '#e2e8f0', fontSize: '1rem', lineHeight: '1.6', maxWidth: '600px', margin: '0 auto' }}>
        {message || 'Capa de Servicio vacía. Esperando que el pipeline distribuido (Kafka ➔ Spark ➔ PostgreSQL) ingiera eventos reales.'}
      </p>
      <div style={{ marginTop: '20px', fontSize: '0.85rem', color: '#94a3b8' }}>
        <em>* Cumpliendo la política estricta de Cero Mocks: la interfaz no inventa telemetría ni dibuja datos ficticios.</em>
      </div>
    </div>
  );
}
