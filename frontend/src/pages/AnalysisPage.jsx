import React, { useState } from 'react';
import ProductSearch from '../components/ProductSearch';
import ActivitiesList from '../components/ActivitiesList';
import { productService, settingsService } from '../services/api';
import './AnalysisPage.css';

const AnalysisPage = () => {
  const [selectedProduct, setSelectedProduct] = useState(null);
  const [activities, setActivities] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [alertsData, setAlertsData] = useState({ alerts: [], pending_count: 0, data_referencia: null });
  const [alertsLoading, setAlertsLoading] = useState(false);
  const [alertsError, setAlertsError] = useState(null);
  const [alertsMessage, setAlertsMessage] = useState(null);
  const [sendingAlerts, setSendingAlerts] = useState(false);

  const handleSearch = async (query) => {
    setIsLoading(true);
    setError(null);
    try {
      // Buscar produtos
      const searchResponse = await productService.searchProducts(query, 5);
      const products = searchResponse.data.products;

      if (products.length === 0) {
        setError('Nenhum produto encontrado');
        setIsLoading(false);
        return;
      }

      // Se houver apenas um produto, selecionar automaticamente
      // Senão, mostrar lista para escolher
      if (products.length === 1) {
        await loadProductData(products[0].product_id);
      } else {
        // Aqui você poderia mostrar um modal para escolher o produto
        // Por enquanto, vamos com o primeiro
        await loadProductData(products[0].product_id);
      }
    } catch (err) {
      setError('Erro ao buscar produtos: ' + err.message);
    } finally {
      setIsLoading(false);
    }
  };

  const loadProductData = async (productId) => {
    try {
      setIsLoading(true);
      setAlertsMessage(null);

      // Buscar atividades
      const activitiesResponse = await productService.getActivities(productId);
      setActivities(activitiesResponse.data);

      // Buscar resumo
      const summaryResponse = await productService.getSummary(productId);
      setSelectedProduct(summaryResponse.data);

      await loadProductAlerts(productId);
    } catch (err) {
      setError('Erro ao carregar dados do produto: ' + err.message);
    } finally {
      setIsLoading(false);
    }
  };

  const loadProductAlerts = async (productId) => {
    setAlertsLoading(true);
    setAlertsError(null);
    try {
      const response = await settingsService.getProductAlerts(productId);
      setAlertsData(response.data || { alerts: [], pending_count: 0, data_referencia: null });
    } catch (err) {
      setAlertsError('Erro ao carregar alertas do produto.');
    } finally {
      setAlertsLoading(false);
    }
  };

  const handleDispatchAlerts = async () => {
    if (!selectedProduct) return;
    setSendingAlerts(true);
    setAlertsMessage(null);
    try {
      const response = await settingsService.dispatchProductAlerts(selectedProduct.product_id);
      const data = response.data || {};
      setAlertsMessage(data.message || 'Alertas enviados.');
      await loadProductAlerts(selectedProduct.product_id);
    } catch (err) {
      const errorMessage = err.response?.data?.detail || 'Falha ao enviar alertas.';
      setAlertsMessage(errorMessage);
    } finally {
      setSendingAlerts(false);
    }
  };

  return (
    <div className="analysis-page">
      <div className="page-header">
        <h1>Análise de Custos</h1>
        <p>Consulte produtos, visualize a composição e analise custos</p>
      </div>

      <ProductSearch onSearch={handleSearch} isLoading={isLoading} />

      {error && <div className="error-message">{error}</div>}

      {selectedProduct && (
        <div className="product-info">
          <div className="info-card">
            <div className="info-item">
              <span className="label">Nome do Produto:</span>
              <span className="value">{selectedProduct.product_name}</span>
            </div>
            <div className="info-item">
              <span className="label">Código:</span>
              <span className="value">{selectedProduct.product_code}</span>
            </div>
            <div className="info-item">
              <span className="label">Custo Atual:</span>
              <span className="value highlight">R$ {selectedProduct.current_cost.toFixed(2)}</span>
            </div>
            <div className="info-item">
              <span className="label">Última Atualização:</span>
              <span className="value">
                {new Date(selectedProduct.last_update).toLocaleDateString('pt-BR')}
              </span>
            </div>
          </div>
        </div>
      )}

      {selectedProduct && (
        <div className="alert-panel">
          <div className="alert-panel-header">
            <div>
              <h2>Alertas do Produto</h2>
              <p>Baseado no ultimo processamento de custos e variacoes.</p>
            </div>
            <button
              type="button"
              className="alert-send-button"
              onClick={handleDispatchAlerts}
              disabled={sendingAlerts || alertsLoading || alertsData.pending_count === 0}
            >
              {sendingAlerts ? 'Enviando...' : 'Enviar alertas por e-mail'}
            </button>
          </div>

          {alertsMessage && <div className="alert-message">{alertsMessage}</div>}
          {alertsError && <div className="alert-error">{alertsError}</div>}

          {alertsLoading ? (
            <div className="alert-loading">Carregando alertas...</div>
          ) : alertsData.alerts.length === 0 ? (
            <div className="alert-empty">Nenhum alerta encontrado para este produto.</div>
          ) : (
            <div className="alert-list">
              {alertsData.alerts.map((alert, index) => (
                <div key={`${alert.tipo_alerta}-${index}`} className="alert-item">
                  <div className="alert-item-header">
                    <span className={`alert-badge ${alert.severidade || 'media'}`}>
                      {alert.tipo_alerta}
                    </span>
                    <span className={`alert-status ${alert.sent ? 'sent' : 'pending'}`}>
                      {alert.sent ? 'Enviado' : 'Pendente'}
                    </span>
                  </div>
                  <div className="alert-item-body">
                    <strong>{alert.descricao || selectedProduct.product_name}</strong>
                    <p>{alert.mensagem_alerta}</p>
                    <div className="alert-metrics">
                      {alert.custo_atual !== null && alert.custo_atual !== undefined && (
                        <span>Custo atual: R$ {Number(alert.custo_atual).toFixed(2)}</span>
                      )}
                      {alert.variacao_percentual !== null && alert.variacao_percentual !== undefined && (
                        <span>Variacao: {Number(alert.variacao_percentual).toFixed(2)}%</span>
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {activities && <ActivitiesList activities={activities} isLoading={isLoading} />}
    </div>
  );
};

export default AnalysisPage;
