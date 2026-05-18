import React, { useEffect, useState } from 'react';
import { AlertCircle, CheckCircle, Loader, Mail, Plus, Trash2 } from 'lucide-react';
import {
  healthService,
  queryService,
  syncService,
  costMapService,
  settingsService,
} from '../services/api';
import './SettingsPage.css';

export const SettingsPage = () => {
  const [apiStatus, setApiStatus] = useState({});
  const [loading, setLoading] = useState(true);
  const [loadingSettings, setLoadingSettings] = useState(false);
  const [savingSettings, setSavingSettings] = useState(false);
  const [sendingTestEmail, setSendingTestEmail] = useState(false);
  const [error, setError] = useState(null);
  const [formMessage, setFormMessage] = useState(null);
  const [newRecipient, setNewRecipient] = useState('');

  const [alertSettings, setAlertSettings] = useState({
    smtp: {
      server: '',
      port: 465,
      user: '',
      password: '',
      has_password: false,
    },
    recipients: [],
    thresholds: {
      invoice_increase_pct: 5,
      avg_cost_increase_pct: 3,
      avg_cost_vs_last_invoice_pct: 10,
    },
    split_finished_goods: true,
  });

  const [alertPreview, setAlertPreview] = useState({
    has_new_invoice: false,
    items: [],
    message: '',
    rule_hits: {},
    generated_at: null,
  });

  useEffect(() => {
    checkApiHealth();
    loadAlertSettings();
    loadAlertPreview();

    const interval = setInterval(checkApiHealth, 30000); // Verificar a cada 30s
    return () => clearInterval(interval);
  }, []);

  const normalizeStatus = (value) => {
    if (value === 'ok' || value === 'healthy') return 'ok';
    if (value === 'offline') return 'offline';
    return 'unknown';
  };

  const isValidEmail = (value) => value && value.includes('@') && value.includes('.');

  const loadAlertSettings = async () => {
    setLoadingSettings(true);
    try {
      const response = await settingsService.getAlertSettings();
      const data = response.data || {};
      setAlertSettings({
        smtp: {
          server: data.smtp?.server || '',
          port: data.smtp?.port || 465,
          user: data.smtp?.user || '',
          password: '',
          has_password: Boolean(data.smtp?.has_password),
        },
        recipients: Array.isArray(data.recipients) ? data.recipients : [],
        thresholds: {
          invoice_increase_pct: Number(data.thresholds?.invoice_increase_pct ?? 5),
          avg_cost_increase_pct: Number(data.thresholds?.avg_cost_increase_pct ?? 3),
          avg_cost_vs_last_invoice_pct: Number(data.thresholds?.avg_cost_vs_last_invoice_pct ?? 10),
        },
        split_finished_goods: Boolean(data.split_finished_goods ?? true),
      });
    } catch (err) {
      setFormMessage({
        type: 'error',
        text: 'Falha ao carregar configuração de alertas.',
      });
    } finally {
      setLoadingSettings(false);
    }
  };

  const loadAlertPreview = async () => {
    try {
      const response = await settingsService.getAlertPreview();
      setAlertPreview(response.data || { has_new_invoice: false, items: [], message: '' });
    } catch (err) {
      setAlertPreview({
        has_new_invoice: false,
        items: [],
        message: 'Falha ao carregar preview de alertas.',
        rule_hits: {},
        generated_at: null,
      });
    }
  };

  const checkApiHealth = async () => {
    try {
      const [healthRes, queryRes, syncRes, costMapRes] = await Promise.all([
        healthService.checkAPI().catch(() => ({ data: { status: 'offline' } })),
        queryService.healthCheck().catch(() => ({ data: { status: 'offline' } })),
        syncService.getHealth().catch(() => ({ data: { status: 'offline' } })),
        costMapService.healthCheck().catch(() => ({ data: { status: 'offline' } })),
      ]);

      setApiStatus({
        api: normalizeStatus(healthRes.data?.status),
        queries: normalizeStatus(queryRes.data?.status),
        sync: normalizeStatus(syncRes.data?.status),
        costMap: normalizeStatus(costMapRes.data?.status),
      });
      setError(null);
    } catch (err) {
      setError('Erro ao verificar status da API');
    } finally {
      setLoading(false);
    }
  };

  const handleSmtpFieldChange = (field, value) => {
    setAlertSettings((prev) => ({
      ...prev,
      smtp: {
        ...prev.smtp,
        [field]: value,
      },
    }));
  };

  const handleThresholdChange = (field, value) => {
    const numeric = Number(value);
    setAlertSettings((prev) => ({
      ...prev,
      thresholds: {
        ...prev.thresholds,
        [field]: Number.isNaN(numeric) ? 0 : numeric,
      },
    }));
  };

  const addRecipient = () => {
    const email = newRecipient.trim().toLowerCase();
    if (!isValidEmail(email)) return;

    const alreadyExists = alertSettings.recipients.some((item) => item.email === email);
    if (alreadyExists) {
      setNewRecipient('');
      return;
    }

    setAlertSettings((prev) => ({
      ...prev,
      recipients: [...prev.recipients, { email }],
    }));
    setNewRecipient('');
  };

  const removeRecipient = (emailToRemove) => {
    setAlertSettings((prev) => ({
      ...prev,
      recipients: prev.recipients.filter((item) => item.email !== emailToRemove),
    }));
  };

  const saveAlertSettings = async () => {
    setSavingSettings(true);
    setFormMessage(null);

    try {
      const invalidRecipients = alertSettings.recipients.filter(
        (item) => !isValidEmail(item.email)
      );
      if (invalidRecipients.length > 0) {
        setFormMessage({
          type: 'error',
          text: 'Há destinatários com e-mail inválido.',
        });
        return;
      }

      const thresholds = alertSettings.thresholds;
      const thresholdValues = [
        thresholds.invoice_increase_pct,
        thresholds.avg_cost_increase_pct,
        thresholds.avg_cost_vs_last_invoice_pct,
      ];
      const invalidThreshold = thresholdValues.some(
        (value) => Number.isNaN(Number(value)) || Number(value) < 0 || Number(value) > 1000
      );
      if (invalidThreshold) {
        setFormMessage({
          type: 'error',
          text: 'Os percentuais precisam ser números entre 0 e 1000.',
        });
        return;
      }

      const passwordValue = alertSettings.smtp.password.trim();
      const payload = {
        smtp: {
          server: alertSettings.smtp.server || null,
          port: Number(alertSettings.smtp.port || 465),
          user: alertSettings.smtp.user || null,
          password: passwordValue === '' ? null : passwordValue,
        },
        recipients: alertSettings.recipients,
        thresholds: {
          invoice_increase_pct: Number(thresholds.invoice_increase_pct),
          avg_cost_increase_pct: Number(thresholds.avg_cost_increase_pct),
          avg_cost_vs_last_invoice_pct: Number(thresholds.avg_cost_vs_last_invoice_pct),
        },
        split_finished_goods: alertSettings.split_finished_goods,
      };

      const response = await settingsService.updateAlertSettings(payload);
      const data = response.data || {};
      setAlertSettings({
        smtp: {
          server: data.smtp?.server || '',
          port: data.smtp?.port || 465,
          user: data.smtp?.user || '',
          password: '',
          has_password: Boolean(data.smtp?.has_password),
        },
        recipients: Array.isArray(data.recipients) ? data.recipients : [],
        thresholds: {
          invoice_increase_pct: Number(data.thresholds?.invoice_increase_pct ?? 5),
          avg_cost_increase_pct: Number(data.thresholds?.avg_cost_increase_pct ?? 3),
          avg_cost_vs_last_invoice_pct: Number(data.thresholds?.avg_cost_vs_last_invoice_pct ?? 10),
        },
        split_finished_goods: Boolean(data.split_finished_goods ?? true),
      });
      await loadAlertPreview();
      setFormMessage({
        type: 'success',
        text: 'Configuração salva com sucesso.',
      });
    } catch (err) {
      const errorMessage = err.response?.data?.detail || 'Erro ao salvar configuração.';
      setFormMessage({ type: 'error', text: errorMessage });
    } finally {
      setSavingSettings(false);
    }
  };

  const sendTestEmail = async () => {
    setSendingTestEmail(true);
    setFormMessage(null);
    try {
      const response = await settingsService.sendTestEmail();
      const data = response.data || {};
      const successCount = data.success_count ?? 0;
      const failCount = data.fail_count ?? 0;
      const failedRecipients = (data.failed_recipients || []).join(', ');
      setFormMessage({
        type: failCount > 0 ? 'warning' : 'success',
        text: `Teste finalizado: ${successCount} sucesso(s), ${failCount} falha(s).${failedRecipients ? ` Falhas: ${failedRecipients}` : ''}`,
      });
      await loadAlertPreview();
    } catch (err) {
      const errorMessage = err.response?.data?.detail || 'Falha ao solicitar teste de e-mail.';
      setFormMessage({ type: 'error', text: errorMessage });
    } finally {
      setSendingTestEmail(false);
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
              <ServiceStatus name="Serviço de Sincronização" status={apiStatus.sync} />
              <ServiceStatus name="Mapa de Custos" status={apiStatus.costMap} />
            </div>
          )}
        </section>

        {/* Seção de Alertas por E-mail */}
        <section className="settings-section settings-section-wide">
          <h2>📧 Alertas por E-mail (SMTP)</h2>
          <p className="section-description">
            Configure destinatários e percentuais para avisos automáticos
          </p>

          {loadingSettings ? (
            <div className="loading-status">
              <Loader size={24} className="animate-spin" />
              <span>Carregando configuração de alertas...</span>
            </div>
          ) : (
            <div className="alerts-config">
              <div className="settings-note">
                As configurações abaixo são salvas no PostgreSQL e usadas nos alertas.
              </div>

              <div className="form-grid">
                <label className="field-group">
                  <span>Servidor SMTP</span>
                  <input
                    type="text"
                    value={alertSettings.smtp.server}
                    onChange={(e) => handleSmtpFieldChange('server', e.target.value)}
                    placeholder="smtp.seudominio.com"
                  />
                </label>

                <label className="field-group">
                  <span>Porta SMTP</span>
                  <input
                    type="number"
                    value={alertSettings.smtp.port}
                    onChange={(e) => handleSmtpFieldChange('port', e.target.value)}
                    placeholder="465"
                  />
                </label>

                <label className="field-group">
                  <span>Usuário SMTP</span>
                  <input
                    type="text"
                    value={alertSettings.smtp.user}
                    onChange={(e) => handleSmtpFieldChange('user', e.target.value)}
                    placeholder="contato@empresa.com"
                  />
                </label>

                <label className="field-group">
                  <span>Senha SMTP</span>
                  <input
                    type="password"
                    value={alertSettings.smtp.password}
                    onChange={(e) => handleSmtpFieldChange('password', e.target.value)}
                    placeholder={
                      alertSettings.smtp.has_password
                        ? 'Senha já cadastrada (digite para alterar)'
                        : 'Digite a senha SMTP'
                    }
                  />
                </label>
              </div>

              <div className="threshold-grid">
                <label className="field-group">
                  <span>Alerta de nota fiscal maior que (%)</span>
                  <input
                    type="number"
                    step="0.1"
                    value={alertSettings.thresholds.invoice_increase_pct}
                    onChange={(e) =>
                      handleThresholdChange('invoice_increase_pct', e.target.value)
                    }
                  />
                </label>

                <label className="field-group">
                  <span>Alerta de aumento de custo médio (%)</span>
                  <input
                    type="number"
                    step="0.1"
                    value={alertSettings.thresholds.avg_cost_increase_pct}
                    onChange={(e) =>
                      handleThresholdChange('avg_cost_increase_pct', e.target.value)
                    }
                  />
                </label>

                <label className="field-group">
                  <span>Listar itens com diferença &gt; (%)</span>
                  <input
                    type="number"
                    step="0.1"
                    value={alertSettings.thresholds.avg_cost_vs_last_invoice_pct}
                    onChange={(e) =>
                      handleThresholdChange('avg_cost_vs_last_invoice_pct', e.target.value)
                    }
                  />
                </label>
              </div>

              <div className="toggle-row">
                <label className="checkbox-field">
                  <input
                    type="checkbox"
                    checked={alertSettings.split_finished_goods}
                    onChange={(e) =>
                      setAlertSettings((prev) => ({
                        ...prev,
                        split_finished_goods: e.target.checked,
                      }))
                    }
                  />
                  <span>Separar itens acabados dos demais</span>
                </label>
                <small>
                  IDEIA: alinhar com backend o critério oficial de item acabado para manter consistência.
                </small>
              </div>

              <div className="recipients-section">
                <h3>
                  <Mail size={14} /> Destinatários dos alertas
                </h3>
                <div className="recipient-input-row">
                  <input
                    type="email"
                    value={newRecipient}
                    onChange={(e) => setNewRecipient(e.target.value)}
                    placeholder="email@empresa.com"
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        addRecipient();
                      }
                    }}
                  />
                  <button type="button" onClick={addRecipient}>
                    <Plus size={14} /> Adicionar
                  </button>
                </div>

                <div className="recipient-list">
                  {alertSettings.recipients.length === 0 ? (
                    <p className="empty-hint">Nenhum destinatário cadastrado.</p>
                  ) : (
                    alertSettings.recipients.map((item) => (
                      <div key={item.email} className="recipient-item">
                        <span>{item.email}</span>
                        <button
                          type="button"
                          className="danger"
                          onClick={() => removeRecipient(item.email)}
                        >
                          <Trash2 size={14} />
                        </button>
                      </div>
                    ))
                  )}
                </div>
              </div>

              <div className="action-row">
                <button
                  type="button"
                  className="primary"
                  onClick={saveAlertSettings}
                  disabled={savingSettings}
                >
                  {savingSettings ? 'Salvando...' : 'Salvar Configuração'}
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={sendTestEmail}
                  disabled={sendingTestEmail}
                >
                  {sendingTestEmail ? 'Enviando...' : 'Enviar E-mail de Teste (IDEIA)'}
                </button>
              </div>

              {formMessage && (
                <div className={`form-message ${formMessage.type}`}>{formMessage.text}</div>
              )}

              <div className="preview-section">
                <h3>Itens com variação acima do limite</h3>
                {!alertPreview.has_new_invoice ? (
                  <p className="empty-hint">
                    {alertPreview.message || 'Sem nota fiscal nova. Lista não exibida.'}
                  </p>
                ) : alertPreview.items?.length ? (
                  <div className="preview-table-wrapper">
                    <table className="preview-table">
                      <thead>
                        <tr>
                          <th>Código</th>
                          <th>Descrição</th>
                          <th>Custo Última NF</th>
                          <th>Custo Médio</th>
                          <th>Variação (%)</th>
                        </tr>
                      </thead>
                      <tbody>
                        {alertPreview.items.map((item) => (
                          <tr key={item.product_code}>
                            <td>{item.product_code}</td>
                            <td>{item.product_name}</td>
                            <td>{Number(item.last_invoice_cost || 0).toFixed(2)}</td>
                            <td>{Number(item.average_cost || 0).toFixed(2)}</td>
                            <td>{Number(item.variance_pct || 0).toFixed(2)}%</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="empty-hint">Não há itens acima do limite configurado.</p>
                )}
              </div>
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
