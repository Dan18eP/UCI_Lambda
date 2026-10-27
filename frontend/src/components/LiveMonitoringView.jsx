import React from 'react';
import BedCard from './BedCard';
import NoDataNotice from './NoDataNotice';
import { ActivityIcon, AlertTriangleIcon, CheckCircleIcon, WrenchIcon } from './Icons';

export default function LiveMonitoringView({ beds, noDataMessage, onOpenAssistant, onQuickEvaluate }) {
  if (noDataMessage) {
    return <NoDataNotice message={noDataMessage} />;
  }

  const totalBeds = beds.length;
  const criticalCount = beds.filter((b) => b.is_critical_alert).length;
  const sensorFaultCount = beds.filter((b) => !b.is_valid_sensor).length;
  const warningCount = beds.filter((b) => {
    return !b.is_critical_alert && b.is_valid_sensor && (
      b.heart_rate_bpm > 100 || b.spo2_percent < 95 || b.systolic_bp > 130
    );
  }).length;
  const stableCount = totalBeds - criticalCount - sensorFaultCount - warningCount;

  return (
    <div className="live-monitoring-view">
      {/* Barra de Estadisticas en Vivo de Speed Layer */}
      <div className="speed-stats-bar">
        <div className="stat-pill">
          <ActivityIcon size={16} color="#3b82f6" />
          <span className="stat-label">Camas Monitoreadas:</span>
          <span className="stat-value">{totalBeds}</span>
        </div>
        <div className="stat-pill">
          <CheckCircleIcon size={16} color="#10b981" />
          <span className="stat-label">Estables:</span>
          <span className="stat-value" style={{ color: '#10b981' }}>{stableCount}</span>
        </div>
        <div className="stat-pill">
          <AlertTriangleIcon size={16} color="#f59e0b" />
          <span className="stat-label">Advertencias:</span>
          <span className="stat-value" style={{ color: '#f59e0b' }}>{warningCount}</span>
        </div>
        <div className="stat-pill">
          <AlertTriangleIcon size={16} color="#ef4444" />
          <span className="stat-label">Criticos:</span>
          <span className="stat-value" style={{ color: '#ef4444' }}>{criticalCount}</span>
        </div>
        <div className="stat-pill">
          <WrenchIcon size={16} color="#a855f7" />
          <span className="stat-label">Fallas Sensor:</span>
          <span className="stat-value" style={{ color: '#a855f7' }}>{sensorFaultCount}</span>
        </div>
        <div style={{ marginLeft: 'auto', fontSize: '0.75rem', color: '#64748b' }}>
          Latencia Speed: &lt;40ms (Spark Streaming foreachBatch)
        </div>
      </div>

      {/* Grilla de Camas UCI */}
      <div className="beds-grid">
        {beds.map((bed) => (
          <BedCard
            key={bed.patient_id}
            bed={bed}
            onOpenAssistant={onOpenAssistant}
          />
        ))}
      </div>
    </div>
  );
}
