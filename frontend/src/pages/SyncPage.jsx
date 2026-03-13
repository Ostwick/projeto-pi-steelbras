import React, { useState, useEffect } from 'react';
import { RefreshCw, AlertCircle, CheckCircle, Loader } from 'lucide-react';
import { syncService } from '../services/api';
import './SyncPage.css';

export const SyncPage = () => {
  const [syncRuns, setSyncRuns] = useState([]);
  const [productsStatus, setProductsStatus] = useState([]);
  const [loading, setLoading] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [activeTab, setActiveTab] = useState('status'); // 'status' or 'history'
  const [productCodes, setProductCodes] = useState(
    '00020011\n00020854\n00069001\n00056737\n00089501\n00020852'
  );

  useEffect(() => {
    loadSyncData();
  }, []);

  const loadSyncData = async () => {
    setLoading(true);
    setError(null);

    try {
      const [runsRes, statusRes] = await Promise.all([
        syncService.getSyncRuns(20),
        syncService.getProductsStatus(100),
      ]);

      setSyncRuns(runsRes.data.runs || []);
      setProductsStatus(statusRes.data.products || []);
    } catch (err) {
      setError(err.message || 'Erro ao carregar dados de sincronização');
    } finally {
      setLoading(false);
    }
  };

  const handleManualSync = async () => {
    await runSyncRequest();
  };

  const runSyncRequest = async (dataset) => {
    setSyncing(true);
    setError(null);
    setSuccess(null);

    try {
      const today = new Date().toISOString().split('T')[0];
      const lastMonth = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)
        .toISOString()
        .split('T')[0];

      const normalizedCodes = productCodes
        .split(/[,\n]/)
        .map((code) => code.trim())
        .filter(Boolean)
        .join(',');
      const response = await syncService.runSync(
        today,
        lastMonth,
        normalizedCodes || undefined,
        dataset || undefined
      );
      
      // Exibir mensagem de sucesso
      const recordsLoaded = response.data.records_loaded;
      const message = response.data.message || 'Sincronização concluída com sucesso!';
      setSuccess(`${message} (${recordsLoaded} registros)`);
      
      // Recarregar dados após sincronização
      setTimeout(loadSyncData, 1000);
    } catch (err) {
      // Extrair mensagem de erro mais útil
      const errorMessage = err.response?.data?.detail || err.message || 'Erro ao executar sincronização manual';
      setError(errorMessage);
    } finally {
      setSyncing(false);
    }
  };

  return (
    <div className="sync-page">
      <div className="sync-header">
        <h1>🔄 Sincronização de Dados</h1>
        <p>Controle e monitore a sincronização entre SQL Server e PostgreSQL</p>
      </div>

      {error && (
        <div className="error-banner">
          <AlertCircle size={20} />
          <span>{error}</span>
        </div>
      )}

      {success && (
        <div className="success-banner">
          <CheckCircle size={20} />
          <span>{success}</span>
        </div>
      )}

      <div className="sync-controls">
        <button
          className="btn-sync-manual"
          onClick={handleManualSync}
          disabled={syncing || loading}
        >
          {syncing ? (
            <>
              <Loader size={18} className="animate-spin" />
              Sincronizando...
            </>
          ) : (
            <>
              <RefreshCw size={18} />
              Sincronizar Agora
            </>
          )}
        </button>

        <div className="sync-debug">
          <div className="sync-debug-title">Debug por dataset</div>
          <div className="sync-debug-buttons">
            <button
              type="button"
              className="btn-sync-debug"
              onClick={() => runSyncRequest('custo_contabil')}
              disabled={syncing || loading}
            >
              Custo Contabil
            </button>
            <button
              type="button"
              className="btn-sync-debug"
              onClick={() => runSyncRequest('custo_nf')}
              disabled={syncing || loading}
            >
              Custo NF
            </button>
            <button
              type="button"
              className="btn-sync-debug"
              onClick={() => runSyncRequest('cost_map')}
              disabled={syncing || loading}
            >
              Cost Map
            </button>
            <button
              type="button"
              className="btn-sync-debug"
              onClick={() => runSyncRequest('bom_rollup')}
              disabled={syncing || loading}
            >
              BOM Rollup
            </button>
          </div>
        </div>

        <div className="sync-products">
          <label className="sync-products-label" htmlFor="sync-product-codes">
            Produtos para sincronizar (um por linha ou separados por virgula)
          </label>
          <textarea
            id="sync-product-codes"
            className="sync-products-input"
            value={productCodes}
            onChange={(event) => setProductCodes(event.target.value)}
            rows={4}
            placeholder="00020011\n00020854\n00069001"
          />
        </div>

        <div className="sync-info">
          <div className="info-item">
            <span className="label">Produtos sincronizados:</span>
            <span className="value">{productsStatus.length}</span>
          </div>
          <div className="info-item">
            <span className="label">Última sincronização:</span>
            <span className="value">
              {syncRuns.length > 0
                ? new Date(syncRuns[0].finished_at).toLocaleString('pt-BR')
                : 'Nunca'}
            </span>
          </div>
        </div>
      </div>

      <div className="sync-tabs">
        <button
          className={`tab ${activeTab === 'status' ? 'active' : ''}`}
          onClick={() => setActiveTab('status')}
        >
          Status dos Produtos
        </button>
        <button
          className={`tab ${activeTab === 'history' ? 'active' : ''}`}
          onClick={() => setActiveTab('history')}
        >
          Histórico de Sincronizações
        </button>
      </div>

      {loading ? (
        <div className="loading-container">
          <Loader size={40} className="animate-spin" />
          <p>Carregando dados...</p>
        </div>
      ) : activeTab === 'status' ? (
        <div className="products-status">
          {productsStatus.length === 0 ? (
            <div className="empty-state">
              <p>Nenhum produto sincronizado</p>
            </div>
          ) : (
            <div className="status-table">
              <table>
                <thead>
                  <tr>
                    <th>Código do Produto</th>
                    <th>Última Sincronização</th>
                    <th>Data Fechamento</th>
                    <th>Origem</th>
                  </tr>
                </thead>
                <tbody>
                  {productsStatus.map((item, idx) => (
                    <tr key={idx}>
                      <td className="code">{item.product_code}</td>
                      <td>
                        {new Date(item.last_sync_at).toLocaleString('pt-BR')}
                      </td>
                      <td>{item.last_data_fechamento || '-'}</td>
                      <td>
                        <span className={`badge ${item.source}`}>
                          {item.source}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      ) : (
        <div className="sync-history">
          {syncRuns.length === 0 ? (
            <div className="empty-state">
              <p>Nenhuma sincronização registrada</p>
            </div>
          ) : (
            <div className="history-cards">
              {syncRuns.map((run, idx) => (
                <div key={idx} className={`history-card ${run.status}`}>
                  <div className="card-header">
                    {run.status === 'success' ? (
                      <CheckCircle size={20} className="status-success" />
                    ) : (
                      <AlertCircle size={20} className="status-error" />
                    )}
                    <span className="status-label">{run.status.toUpperCase()}</span>
                    <time className="timestamp">
                      {new Date(run.started_at).toLocaleString('pt-BR')}
                    </time>
                  </div>

                  <div className="card-details">
                    <div className="detail">
                      <span className="label">Registros carregados:</span>
                      <span className="value">{run.records_loaded}</span>
                    </div>
                    <div className="detail">
                      <span className="label">Duração:</span>
                      <span className="value">
                        {new Date(run.finished_at) -
                          new Date(run.started_at) === 0
                          ? 'Processando...'
                          : `${Math.round(
                              (new Date(run.finished_at) -
                                new Date(run.started_at)) /
                                1000
                            )}s`}
                      </span>
                    </div>
                  </div>

                  {run.error_message && (
                    <div className="card-error">{run.error_message}</div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default SyncPage;
