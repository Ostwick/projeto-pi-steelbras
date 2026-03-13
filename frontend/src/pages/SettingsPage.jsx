import React, { useEffect, useState } from 'react';
import { AlertCircle, CheckCircle, Loader } from 'lucide-react';
import {
  healthService,
  queryService,
  syncService,
  costMapService,
} from '../services/api';
import './SettingsPage.css';

export const SettingsPage = () => {
  const [apiStatus, setApiStatus] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    checkApiHealth();
    const interval = setInterval(checkApiHealth, 30000); // Verificar a cada 30s
    return () => clearInterval(interval);
  }, []);

  const checkApiHealth = async () => {
    try {
      const [healthRes, queryRes, syncRes, costMapRes] = await Promise.all([
        healthService.checkAPI().catch(() => ({ data: { status: 'offline' } })),
        queryService.healthCheck().catch(() => ({ data: { status: 'offline' } })),
        syncService.getHealth().catch(() => ({ data: { status: 'offline' } })),
        costMapService.healthCheck().catch(() => ({ data: { status: 'offline' } })),
      ]);

      setApiStatus({
        api: healthRes.data?.status || 'unknown',
        queries: queryRes.data?.status || 'unknown',
        sync: syncRes.data?.status,
        costMap: costMapRes.data?.status || 'unknown',
      });
      setError(null);
    } catch (err) {
      setError('Erro ao verificar status da API');
    } finally {
      setLoading(false);
    }
  };

  const ServiceStatus = ({ name, status }) => (
    <div className="service-item">
      <div className="service-info">
        <span className="service-name">{name}</span>
        <span className={`status-badge ${status}`}>
          {status === 'ok' ? <CheckCircle size={14} /> : <AlertCircle size={14} />}
          {status === 'ok' ? 'Online' : 'Offline'}
        </span>
      </div>
    </div>
  );

  return (
    <div className="settings-page">
      <div className="settings-header">
        <h1>⚙️ Configurações</h1>
        <p>Gerenciar configurações e monitorar status dos serviços</p>
      </div>

      {error && (
        <div className="error-banner">
          <AlertCircle size={20} />
          <span>{error}</span>
        </div>
      )}

      <div className="settings-grid">
        {/* Seção de Status */}
        <section className="settings-section">
          <h2>📡 Status dos Serviços</h2>
          <p className="section-description">
            Monitore a saúde dos serviços backend
          </p>

          {loading ? (
            <div className="loading-status">
              <Loader size={24} className="animate-spin" />
              <span>Verificando...</span>
            </div>
          ) : (
            <div className="services-list">
              <ServiceStatus name="API Principal" status={apiStatus.api} />
              <ServiceStatus name="Serviço de Queries" status={apiStatus.queries} />
              <ServiceStatus
                name="Serviço de Sincronização"
                status={apiStatus.sync ? 'ok' : 'unknown'}
              />
              <ServiceStatus name="Mapa de Custos" status={apiStatus.costMap} />
            </div>
          )}
        </section>

        {/* Seção de Informações */}
        <section className="settings-section">
          <h2>ℹ️ Informações da Aplicação</h2>
          <p className="section-description">
            Detalhes sobre a aplicação e configuração
          </p>

          <div className="info-list">
            <div className="info-item">
              <span className="info-label">Versão:</span>
              <span className="info-value">1.0.0</span>
            </div>
            <div className="info-item">
              <span className="info-label">API URL:</span>
              <span className="info-value">
                {import.meta.env.VITE_API_URL || 'http://localhost:8000'}
              </span>
            </div>
            <div className="info-item">
              <span className="info-label">Ambiente:</span>
              <span className="info-value">
                {import.meta.env.MODE === 'development' ? 'Desenvolvimento' : 'Produção'}
              </span>
            </div>
            <div className="info-item">
              <span className="info-label">Data/Hora:</span>
              <span className="info-value">
                {new Date().toLocaleString('pt-BR')}
              </span>
            </div>
          </div>
        </section>

        {/* Seção de Atalhos */}
        <section className="settings-section">
          <h2>🔗 Atalhos Úteis</h2>
          <p className="section-description">
            Acesse rapidamente documentação e ferramentas
          </p>

          <div className="shortcuts-list">
            <a
              href={`${import.meta.env.VITE_API_URL}/docs`}
              target="_blank"
              rel="noopener noreferrer"
              className="shortcut-link"
            >
              📖 Documentação da API
            </a>
            <a
              href={`${import.meta.env.VITE_API_URL}/redoc`}
              target="_blank"
              rel="noopener noreferrer"
              className="shortcut-link"
            >
              📚 ReDoc
            </a>
          </div>
        </section>

        {/* Seção de Sobre */}
        <section className="settings-section">
          <h2>📄 Sobre</h2>
          <p className="section-description">
            Informações sobre o projeto
          </p>

          <div className="about-content">
            <p>
              <strong>Cost Analysis System</strong> é uma plataforma de análise de custos
              desenvolvida para controlar e otimizar os custos de produção.
            </p>
            <p>
              Desenvolvido com <strong>FastAPI</strong>, <strong>React</strong>,{' '}
              <strong>SQL Server</strong> e <strong>PostgreSQL</strong>.
            </p>
            <p>© 2026 Data-Driven Steelbras</p>
          </div>
        </section>
      </div>
    </div>
  );
};

export default SettingsPage;
