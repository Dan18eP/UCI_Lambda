import React, { useState, useEffect, useRef } from 'react';
import {
  BrainIcon,
  SendIcon,
  DatabaseIcon,
  RefreshCwIcon,
  CheckCircleIcon,
  AlertTriangleIcon,
  ZapIcon
} from './Icons';

const API_BASE = 'http://localhost:8000';

const PREDEFINED_PROMPTS = [
  'Cual es el resumen de estabilidad en las ultimas ventanas horarias?',
  'Cual es la tendencia hemodinamica y el riesgo de colapso de PAM?',
  'Presenta episodios de desaturacion o fallas de sensor en el historico?',
  'Cual es la comparativa de este paciente frente al promedio de su sede?'
];

export default function ClinicalAssistantDrawer({
  isOpen,
  onClose,
  initialPatientId = null,
  beds = []
}) {
  const [selectedPatientId, setSelectedPatientId] = useState(initialPatientId || '');
  const [messages, setMessages] = useState([
    {
      sender: 'assistant',
      text: 'Bienvenido al Asistente Clinico Inteligente (Batch AI). Consulto exclusivamente la Capa Batch (vitals_historico) precomputada desde el Master Dataset en MinIO Parquet para entregar analisis de tendencia sin afectar la latencia del streaming. Puedes seleccionar un paciente o consultar a nivel de toda la red.',
      findings: [
        'Motor de inferencia tabular conectado a PostgreSQL vitals_historico.',
        'Analisis de lineas base fisiologicas, variabilidad y riesgo de choque hemodinamico.',
        'Sin interferencia en los micro-batches de Spark Streaming.'
      ],
      metrics: null,
      timestamp: new Date().toLocaleTimeString()
    }
  ]);
  const [inputText, setInputText] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef(null);

  // Sincronizar paciente seleccionado si cambia la propiedad inicial
  useEffect(() => {
    if (initialPatientId) {
      setSelectedPatientId(initialPatientId);
    }
  }, [initialPatientId]);

  // Auto-scroll al final del chat
  useEffect(() => {
    if (messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages, loading]);

  const activeBed = beds.find((b) => b.patient_id === selectedPatientId);

  const sendMessage = async (queryText) => {
    const textToSend = queryText || inputText;
    if (!textToSend.trim() || loading) return;

    // Agregar mensaje del usuario
    const userMsg = {
      sender: 'user',
      text: textToSend,
      timestamp: new Date().toLocaleTimeString()
    };
    setMessages((prev) => [...prev, userMsg]);
    setInputText('');
    setLoading(true);

    try {
      const res = await fetch(`${API_BASE}/api/v1/assistant/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          patient_id: selectedPatientId || null,
          query: textToSend
        })
      });

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }

      const data = await res.json();
      const respPayload = data.response;

      const assistantMsg = {
        sender: 'assistant',
        text: respPayload.answer,
        findings: respPayload.key_findings || [],
        metrics: respPayload.metrics || null,
        source: respPayload.data_source,
        timestamp: new Date().toLocaleTimeString()
      };

      setMessages((prev) => [...prev, assistantMsg]);
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        {
          sender: 'assistant',
          text: `Error al procesar consulta en la Capa Batch: ${err.message}`,
          findings: ['Verificar conectividad con el servicio FastAPI y la base de datos PostgreSQL.'],
          metrics: null,
          timestamp: new Date().toLocaleTimeString()
        }
      ]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter') {
      sendMessage();
    }
  };

  if (!isOpen) return null;

  return (
    <div className="assistant-drawer-container">
      {/* Cabecera del Asistente */}
      <div className="drawer-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <BrainIcon size={20} color="#3b82f6" />
          <div>
            <h3 style={{ margin: 0, fontSize: '1rem', color: '#f8fafc' }}>
              Asistente Clinico Batch AI
            </h3>
            <span style={{ fontSize: '0.75rem', color: '#94a3b8' }}>
              Analitica Historica &bull; Cero impacto en Speed Layer
            </span>
          </div>
        </div>

        <button className="drawer-close-btn" onClick={onClose} title="Ocultar panel">
          &times;
        </button>
      </div>

      {/* Selector de Paciente y Cinta de Estado en Vivo (Cero perdida de vista) */}
      <div className="drawer-context-section">
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
          <span style={{ fontSize: '0.8rem', color: '#cbd5e1', fontWeight: 600 }}>
            Foco Clinico:
          </span>
          <select
            className="patient-context-select"
            value={selectedPatientId}
            onChange={(e) => setSelectedPatientId(e.target.value)}
          >
            <option value="">Toda la Red UCI (Multisede)</option>
            {beds.map((b) => (
              <option key={b.patient_id} value={b.patient_id}>
                {b.bed_id} ({b.patient_id} - {b.clinic_id})
              </option>
            ))}
          </select>
        </div>

        {/* Cinta de Contexto en Tiempo Real del Paciente */}
        {activeBed ? (
          <div className="live-patient-ribbon">
            <div className="ribbon-col">
              <span className="ribbon-label">Cama:</span>
              <strong>{activeBed.bed_id}</strong>
            </div>
            <div className="ribbon-col">
              <span className="ribbon-label">FC en Vivo:</span>
              <strong style={{ color: activeBed.is_critical_alert ? '#ef4444' : '#10b981' }}>
                {activeBed.is_valid_sensor ? `${activeBed.heart_rate_bpm} bpm` : 'Sensor Caido'}
              </strong>
            </div>
            <div className="ribbon-col">
              <span className="ribbon-label">SpO2:</span>
              <strong>{activeBed.is_valid_sensor ? `${activeBed.spo2_percent}%` : '--'}</strong>
            </div>
            <div className="ribbon-col">
              <span className="ribbon-label">Presion:</span>
              <strong>
                {activeBed.is_valid_sensor ? `${Math.round(activeBed.systolic_bp)}/${Math.round(activeBed.diastolic_bp)}` : '--/--'}
              </strong>
            </div>
            <div className="ribbon-col">
              <span className="ribbon-label">Estado:</span>
              <span className={`status-pill ${activeBed.is_critical_alert ? 'crit' : !activeBed.is_valid_sensor ? 'sens' : 'stab'}`}>
                {activeBed.is_critical_alert ? 'CRITICO' : !activeBed.is_valid_sensor ? 'SENSOR' : 'ESTABLE'}
              </span>
            </div>
          </div>
        ) : (
          <div style={{ fontSize: '0.75rem', color: '#64748b', fontStyle: 'italic', padding: '4px 0' }}>
            Consultando analitica agregada de ambas sedes metropolitanas.
          </div>
        )}
      </div>

      {/* Area de Conversacion */}
      <div className="drawer-messages-area">
        {messages.map((m, i) => (
          <div key={i} className={`chat-message ${m.sender}`}>
            <div className="message-content">
              <p style={{ margin: 0, lineHeight: 1.45 }}>{m.text}</p>

              {/* Hallazgos Clave de la Capa Batch */}
              {m.findings && m.findings.length > 0 && (
                <ul className="message-findings">
                  {m.findings.map((f, fIdx) => (
                    <li key={fIdx}>{f}</li>
                  ))}
                </ul>
              )}

              {/* Pastillas de Metricas Batch */}
              {m.metrics && Object.keys(m.metrics).length > 0 && (
                <div className="message-metrics-tags">
                  {m.metrics.avg_heart_rate !== undefined && (
                    <span className="metric-tag">
                      FC Media: <strong>{m.metrics.avg_heart_rate} bpm</strong>
                    </span>
                  )}
                  {m.metrics.min_spo2 !== undefined && (
                    <span className="metric-tag">
                      SpO2 Min: <strong>{m.metrics.min_spo2}%</strong>
                    </span>
                  )}
                  {m.metrics.avg_map !== undefined && (
                    <span className="metric-tag" style={{ color: m.metrics.avg_map < 70 ? '#ef4444' : '#10b981' }}>
                      PAM: <strong>{m.metrics.avg_map} mmHg</strong>
                    </span>
                  )}
                  {m.metrics.total_sensor_errors !== undefined && (
                    <span className="metric-tag">
                      Errores Sensor: <strong>{m.metrics.total_sensor_errors}</strong>
                    </span>
                  )}
                </div>
              )}

              {m.source && (
                <div className="message-source">
                  <DatabaseIcon size={12} color="#3b82f6" /> {m.source}
                </div>
              )}

              <span className="message-time">{m.timestamp}</span>
            </div>
          </div>
        ))}

        {loading && (
          <div className="chat-message assistant">
            <div className="message-content loading">
              <span style={{ fontSize: '0.8rem', color: '#94a3b8' }}>
                Consultando lote historico en PostgreSQL...
              </span>
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Sugerencias de Preguntas Predefinidas (Chips) */}
      <div className="drawer-suggestions">
        <span style={{ fontSize: '0.75rem', color: '#94a3b8', display: 'block', marginBottom: '6px' }}>
          Preguntas sugeridas (Batch):
        </span>
        <div className="suggestions-scroll">
          {PREDEFINED_PROMPTS.map((p, idx) => (
            <button
              key={idx}
              className="suggestion-chip"
              onClick={() => sendMessage(p)}
              disabled={loading}
            >
              {p}
            </button>
          ))}
        </div>
      </div>

      {/* Campo de Entrada de Mensaje */}
      <div className="drawer-input-area">
        <input
          type="text"
          className="drawer-input"
          placeholder={selectedPatientId ? `Preguntar sobre ${selectedPatientId}...` : 'Consultar a la red UCI...'}
          value={inputText}
          onChange={(e) => setInputText(e.target.value)}
          onKeyDown={handleKeyPress}
          disabled={loading}
        />
        <button
          className="drawer-send-btn"
          onClick={() => sendMessage()}
          disabled={loading || !inputText.trim()}
          title="Enviar consulta"
        >
          <SendIcon size={16} />
        </button>
      </div>
    </div>
  );
}
