import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Interceptador para erros globais
api.interceptors.response.use(
  (response) => response,
  (error) => {
    console.error('API Error:', error.response?.status, error.message);
    return Promise.reject(error);
  }
);

/**
 * Serviço de Produtos
 */
export const productService = {
  // Buscar produtos por nome ou código
  searchProducts: (query, limit = 10) =>
    api.get('/api/products/search', { params: { query, limit } }),

  // Obter composição do produto
  getComposition: (productId) =>
    api.get(`/api/products/${productId}/composition`),

  // Obter atividades do produto
  getActivities: (productId) =>
    api.get(`/api/products/${productId}/activities`),

  // Obter resumo do produto
  getSummary: (productId) =>
    api.get(`/api/products/${productId}/summary`),
};

/**
 * Serviço de Queries SQL
 */
export const queryService = {
  // Executar query SQL customizada
  executeQuery: (queryName, params) =>
    api.post('/api/queries/execute', { query_name: queryName, params }),

  // Obter datas de fechamento disponíveis
  getAvailableFechamentos: () =>
    api.get('/api/queries/custo-contabil/fechamentos'),

  // Health check
  healthCheck: () =>
    api.get('/api/queries/health'),
};

/**
 * Serviço de Sincronização
 */
export const syncService = {
  // Executar sincronização manual
  runSync: (dataFechamento, dataInicioNf, productCodes, datasets) =>
    api.post('/api/sync/run', null, {
      params: {
        data_fechamento: dataFechamento,
        data_inicio_nf: dataInicioNf,
        ...(productCodes ? { product_codes: productCodes } : {}),
        ...(datasets ? { datasets } : {}),
      },
    }),

  // Obter histórico de sincronizações
  getSyncRuns: (limit = 20) =>
    api.get('/api/sync/runs', { params: { limit } }),

  // Obter status de produtos
  getProductsStatus: (limit = 200) =>
    api.get('/api/sync/products-status', { params: { limit } }),

  // Health check do scheduler
  getHealth: () =>
    api.get('/api/sync/health'),
};

/**
 * Serviço de Mapa de Custos
 */
export const costMapService = {
  // Gerar mapa de custos completo
  generateCostMap: (produtoCodigo, dataInicio, dataFim, filialCodigo) =>
    api.post('/api/cost-map/generate', {
      produto_codigo: produtoCodigo,
      data_inicio: dataInicio,
      data_fim: dataFim,
      filial_codigo: filialCodigo,
    }),

  // Obter estrutura BOM em formato hierárquico
  getStructure: (produtoCodigo) =>
    api.get(`/api/cost-map/estrutura/${encodeURIComponent(produtoCodigo)}`),

  // Health check
  healthCheck: () =>
    api.get('/api/cost-map/health'),
};

/**
 * Serviço de Health Check Geral
 */
export const healthService = {
  // Verificar saúde geral da API
  checkAPI: () =>
    api.get('/health'),
};

/**
 * Serviço de Configurações de Alertas
 */
export const settingsService = {
  // Buscar configuracao atual de alertas
  getAlertSettings: () =>
    api.get('/api/settings/alerts'),

  // Atualizar configuracao de alertas
  updateAlertSettings: (payload) =>
    api.put('/api/settings/alerts', payload),

  // Preview de itens com variacao de custo para alertas
  getAlertPreview: () =>
    api.get('/api/settings/alerts/preview'),

  // Disparar envio de e-mail de teste (placeholder)
  sendTestEmail: () =>
    api.post('/api/settings/alerts/test-email'),
};

export default api;

