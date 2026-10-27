import React, { useState, useEffect, useCallback } from 'react';
import AuditorBar from './components/AuditorBar';
import ClinicSelector from './components/ClinicSelector';
import LiveMonitoringView from './components/LiveMonitoringView';
import HistoricalAnalyticsView from './components/HistoricalAnalyticsView';
import AuditLineageView from './components/AuditLineageView';
import ClinicalAssistantDrawer from './components/ClinicalAssistantDrawer';
import { PulseIcon, DatabaseIcon, ShieldIcon, BrainIcon } from './components/Icons';

const API_BASE = 'http://localhost:8000';
const WS_URL = 'ws://localhost:8000/ws/alerts';

export default function App() {
  const [activeTab, setActiveTab] = useState('speed'); // 'speed' | 'batch' | 'audit'
  const [beds, setBeds] = useState([]);
  const [selectedClinic, setSelectedClinic] = useState(null);
  const [noDataMessage, setNoDataMessage] = useState(null);
  const [wsConnected, setWsConnected] = useState(false);
  
  // Estado del Asistente Clinico Batch (Panel Lateral Acoplado)
  const [isAssistantOpen, setIsAssistantOpen] = useState(false);
  const [assistantPatientId, setAssistantPatientId] = useState(null);

  // 1. Carga inicial desde REST API (Capa de Servicio)
  const fetchDashboard = useCallback(async () => {
    try {
      const url = selectedClinic
        ? `${API_BASE}/api/v1/dashboard?clinic_id=${selectedClinic}`
        : `${API_BASE}/api/v1/dashboard`;
      const res = await fetch(url);
      const data = await res.json();

      if (data.status === 'NO_DATA') {
        setNoDataMessage(data.message);
        setBeds([]);
      } else {
        setNoDataMessage(null);
        setBeds(data.beds || []);
      }
    } catch (err) {
      console.warn('Backend inicializando:', err);
      setNoDataMessage('Conectando con la Capa de Servicio...');
    }
  }, [selectedClinic]);

  useEffect(() => {
    fetchDashboard();
  }, [fetchDashboard]);

  // 2. Conexion WebSocket para Telemetria Reactiva en Vivo (<40ms)
  useEffect(() => {
    let socket = null;
    let reconnectTimer = null;

    function connectWs() {
      socket = new WebSocket(WS_URL);

      socket.onopen = () => {
        setWsConnected(true);
      };

      socket.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          if (message.type === 'VITALS_UPDATE' && message.data) {
            const updated = message.data;
            setBeds((prevBeds) => {
              const index = prevBeds.findIndex((b) => b.patient_id === updated.patient_id);
              if (index >= 0) {
                const next = [...prevBeds];
                next[index] = { ...next[index], ...updated };
                return next;
              }
              if (!selectedClinic || updated.clinic_id === selectedClinic) {
                return [...prevBeds, updated];
              }
              return prevBeds;
            });
            setNoDataMessage(null);
          }
        } catch (e) {
          console.error('Error parseando payload de WebSocket:', e);
        }
      };

      socket.onclose = () => {
        setWsConnected(false);
        reconnectTimer = setTimeout(connectWs, 3000);
      };

      socket.onerror = () => {
        socket.close();
      };
    }

    connectWs();

    return () => {
      if (socket) socket.close();
      if (reconnectTimer) clearTimeout(reconnectTimer);
    };
  }, [selectedClinic]);

  // Handler para abrir el asistente con un paciente especifico
  const handleOpenAssistantForBed = (bed) => {
    setAssistantPatientId(bed.patient_id);
    setIsAssistantOpen(true);
  };

  return (
    <div className="app-root">
      {/* 1. Barra de Auditoria Superior con Enlaces de Infraestructura */}
      <AuditorBar />

      {/* 2. Cabecera del Centro de Mando Hospitalario */}
      <header className="dashboard-header">
        <div className="brand-section">
          <h1>
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: '8px' }}>
              <PulseIcon size={24} color="#3b82f6" /> Red UCI Metropolitana
            </span>
            <span
              style={{
                fontSize: '0.75rem',
                backgroundColor: wsConnected ? '#065f46' : '#7f1d1d',
                color: wsConnected ? '#6ee7b7' : '#fca5a5',
                padding: '2px 8px',
                borderRadius: '12px'
              }}
            >
              {wsConnected ? 'WebSocket Activo (<40ms)' : 'Reconectando...'}
            </span>
          </h1>
          <p className="brand-subtitle">
            Arquitectura Lambda Medica: Ingestion KRaft, Spark Streaming, MinIO Parquet, Airflow Batch y Soporte Tabular ML.
          </p>
        </div>

        {/* Acciones de Cabecera: Selector de Sede y Boton de Asistente Clinico */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          {activeTab === 'speed' && (
            <ClinicSelector
              selectedClinic={selectedClinic}
              onSelectClinic={setSelectedClinic}
            />
          )}

          <button
            className={`assistant-toggle-btn ${isAssistantOpen ? 'active' : ''}`}
            onClick={() => setIsAssistantOpen(!isAssistantOpen)}
            title="Abrir Asistente Clinico Batch (Cero perdida de vista a los datos reales)"
          >
            <BrainIcon size={16} />
            <span>Asistente Batch AI</span>
          </button>
        </div>
      </header>

      {/* 3. Barra de Navegacion por Pestanas Principales */}
      <nav className="tab-navigation-bar">
        <button
          className={`nav-tab-btn ${activeTab === 'speed' ? 'active' : ''}`}
          onClick={() => setActiveTab('speed')}
        >
          <PulseIcon size={16} />
          <span>Monitoreo en Vivo (Speed Layer)</span>
          <span className="tab-badge-live">12 Camas</span>
        </button>

        <button
          className={`nav-tab-btn ${activeTab === 'batch' ? 'active' : ''}`}
          onClick={() => setActiveTab('batch')}
        >
          <DatabaseIcon size={16} />
          <span>Analitica Historica (Batch Layer)</span>
        </button>

        <button
          className={`nav-tab-btn ${activeTab === 'audit' ? 'active' : ''}`}
          onClick={() => setActiveTab('audit')}
        >
          <ShieldIcon size={16} />
          <span>Auditoria y Linaje (Pipeline Telemetry)</span>
        </button>
      </nav>

      {/* 4. Contenedor de Trabajo Principal con Diseno Split (No hace perder de vista los datos) */}
      <div className={`workspace-split-layout ${isAssistantOpen ? 'drawer-expanded' : ''}`}>
        <main className="main-viewport-content">
          {activeTab === 'speed' && (
            <LiveMonitoringView
              beds={beds}
              noDataMessage={noDataMessage}
              onOpenAssistant={handleOpenAssistantForBed}
            />
          )}

          {activeTab === 'batch' && (
            <HistoricalAnalyticsView
              onOpenAssistant={(info) => {
                setAssistantPatientId(info.patient_id);
                setIsAssistantOpen(true);
              }}
            />
          )}

          {activeTab === 'audit' && (
            <AuditLineageView />
          )}
        </main>

        {/* 5. Asistente Clinico Batch (Panel Lateral Integrado y Acoplado) */}
        {isAssistantOpen && (
          <aside className="assistant-sidebar-dock">
            <ClinicalAssistantDrawer
              isOpen={isAssistantOpen}
              onClose={() => setIsAssistantOpen(false)}
              initialPatientId={assistantPatientId}
              beds={beds}
            />
          </aside>
        )}
      </div>
    </div>
  );
}
