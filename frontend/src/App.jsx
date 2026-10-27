import React, { useState, useEffect, useCallback } from 'react';
import AuditorBar from './components/AuditorBar';
import ClinicSelector from './components/ClinicSelector';
import BedCard from './components/BedCard';
import NoDataNotice from './components/NoDataNotice';
import ClinicalAssistantModal from './components/ClinicalAssistantModal';
import { PulseIcon } from './components/Icons';

const API_BASE = 'http://localhost:8000';
const WS_URL = 'ws://localhost:8000/ws/alerts';

export default function App() {
  const [beds, setBeds] = useState([]);
  const [selectedClinic, setSelectedClinic] = useState(null);
  const [noDataMessage, setNoDataMessage] = useState(null);
  const [activeAssistantBed, setActiveAssistantBed] = useState(null);
  const [wsConnected, setWsConnected] = useState(false);

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
      console.warn('Backend aún no disponible o inicializando:', err);
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

  return (
    <div>
      {/* 1. Barra de Auditoria Superior para el Team Leader */}
      <AuditorBar />

      {/* 2. Cabecera del Centro de Monitoreo */}
      <header className="dashboard-header">
        <div className="brand-section">
          <h1>
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: '8px' }}>
              <PulseIcon size={24} color="#3b82f6" /> Red UCI Metropolitana
            </span>
            <span style={{
              fontSize: '0.75rem',
              backgroundColor: wsConnected ? '#065f46' : '#7f1d1d',
              color: wsConnected ? '#6ee7b7' : '#fca5a5',
              padding: '2px 8px',
              borderRadius: '12px'
            }}>
              {wsConnected ? 'WebSocket Activo (<40ms)' : 'Reconectando...'}
            </span>
          </h1>
          <p className="brand-subtitle">
            Monitoreo en Tiempo Real (Spark Speed) + Historico (MinIO Parquet Batch) + Soporte ML Tabular (&lt;3ms).
          </p>
        </div>

        <ClinicSelector
          selectedClinic={selectedClinic}
          onSelectClinic={setSelectedClinic}
        />
      </header>

      {/* 3. Contenedor Principal */}
      <main className="main-container">
        {noDataMessage ? (
          <NoDataNotice message={noDataMessage} />
        ) : (
          <div className="beds-grid">
            {beds.map((bed) => (
              <BedCard
                key={bed.patient_id}
                bed={bed}
                onOpenAssistant={setActiveAssistantBed}
              />
            ))}
          </div>
        )}
      </main>

      {/* 4. Modal de Inferencia del Asistente Clinico */}
      {activeAssistantBed && (
        <ClinicalAssistantModal
          bed={activeAssistantBed}
          onClose={() => setActiveAssistantBed(null)}
        />
      )}
    </div>
  );
}
