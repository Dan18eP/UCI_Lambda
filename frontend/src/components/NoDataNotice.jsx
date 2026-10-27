import React from 'react';
import { AlertTriangleIcon } from './Icons';

/**
 * Componente de Aviso de Auditoria cuando la Serving Layer no tiene registros aun.
 * Cumple: ADR-006 & RULES.md (Cero datos falsos o simulados en memoria, Cero emojis).
 */
export default function NoDataNotice({ message }) {
  return (
    <div style={{
      backgroundColor: 'rgba(245, 158, 11, 0.08)',
      border: '1px solid #d97706',
      borderRadius: '8px',
      padding: '36px 24px',
      textAlign: 'center',
      margin: '40px auto',
      maxWidth: '750px'
    }}>
      <div style={{ display: 'flex', justifyContent: 'center', marginBottom: '14px' }}>
        <AlertTriangleIcon size={44} color="#f59e0b" />
      </div>
      <h2 style={{ color: '#fbbf24', fontSize: '1.25rem', marginBottom: '10px', fontWeight: 600 }}>
        Aviso de Auditoria de Arquitectura
      </h2>
      <p style={{ color: '#e2e8f0', fontSize: '0.95rem', lineHeight: '1.5', maxWidth: '600px', margin: '0 auto' }}>
        {message || 'Capa de Servicio vacia. Esperando ingestion de eventos desde el pipeline Lambda.'}
      </p>
      <div style={{ marginTop: '16px', fontSize: '0.8rem', color: '#94a3b8' }}>
        * Politica estricta de Cero Mocks: la interfaz no genera datos ficticios ni emula estados en memoria.
      </div>
    </div>
  );
}
