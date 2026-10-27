import React from 'react';

/**
 * Selector de Clínica para soporte de red hospitalaria metropolitana.
 * Permite auditar sedes sin pantallas de login artificiales.
 */
export default function ClinicSelector({ selectedClinic, onSelectClinic }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
      <label htmlFor="clinic-select" style={{ fontSize: '0.85rem', color: '#94a3b8', fontWeight: 600 }}>
        SEDE CLÍNICA:
      </label>
      <select
        id="clinic-select"
        value={selectedClinic || ''}
        onChange={(e) => onSelectClinic(e.target.value || null)}
        style={{
          backgroundColor: '#1e293b',
          color: '#f8fafc',
          border: '1px solid #334155',
          borderRadius: '6px',
          padding: '8px 14px',
          fontSize: '0.9rem',
          fontWeight: 600,
          cursor: 'pointer',
          outline: 'none'
        }}
      >
        <option value="">🏥 Todas las Clínicas (Red Completa - 12 Camas)</option>
        <option value="CLI-CENTRAL-01">📍 Sede Central (Camas 01 a 06)</option>
        <option value="CLI-NORTE-02">📍 Sede Norte (Camas 07 a 12)</option>
      </select>
    </div>
  );
}
