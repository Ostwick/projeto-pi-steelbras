import React, { useState } from 'react';
import ProductSearch from '../components/ProductSearch';
import CompositionTree from '../components/CompositionTree';
import ActivitiesList from '../components/ActivitiesList';
import { productService } from '../services/api';
import './AnalysisPage.css';

const AnalysisPage = () => {
  const [selectedProduct, setSelectedProduct] = useState(null);
  const [composition, setComposition] = useState(null);
  const [activities, setActivities] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

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

      // Buscar composição
      const compositionResponse = await productService.getComposition(productId);
      setComposition(compositionResponse.data);

      // Buscar atividades
      const activitiesResponse = await productService.getActivities(productId);
      setActivities(activitiesResponse.data);

      // Buscar resumo
      const summaryResponse = await productService.getSummary(productId);
      setSelectedProduct(summaryResponse.data);
    } catch (err) {
      setError('Erro ao carregar dados do produto: ' + err.message);
    } finally {
      setIsLoading(false);
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

      {composition && <CompositionTree composition={composition} isLoading={isLoading} />}

      {activities && <ActivitiesList activities={activities} isLoading={isLoading} />}
    </div>
  );
};

export default AnalysisPage;
