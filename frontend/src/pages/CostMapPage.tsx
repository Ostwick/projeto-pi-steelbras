import React, { useState } from 'react';
import { Search, AlertCircle, Loader } from 'lucide-react';
import CostMapTree from '../components/CostMapTree';
import { useCostMap } from '../hooks/useCostMap';
import './CostMapPage.css';

export const CostMapPage: React.FC = () => {
  const [inputValue, setInputValue] = useState('00020011');
  const { tree, loading, error, fetchCostMap } = useCostMap();
  const [isSearching, setIsSearching] = useState(false);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!inputValue.trim()) return;

    setIsSearching(true);
    try {
      await fetchCostMap(inputValue.trim());
    } finally {
      setIsSearching(false);
    }
  };

  const handleNodeClick = (node: any) => {
    // Aqui você pode implementar ações quando um nó é clicado
    // Por exemplo, mostrar detalhes, OPs, NFs, etc.
    console.log('Node clicado:', node);
  };

  return (
    <div className="cost-map-page">
      <div className="cost-map-sidebar">
        <div className="sidebar-header">
          <h1>Mapa de Custos</h1>
          <p>Visualize a estrutura de produtos, OPs e notas fiscais</p>
        </div>

        <form onSubmit={handleSearch} className="search-form">
          <div className="search-input-group">
            <input
              type="text"
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value.toUpperCase())}
              placeholder="Código do produto..."
              className="search-input"
              disabled={loading}
            />
            <button
              type="submit"
              className="search-btn"
              disabled={loading || !inputValue.trim()}
            >
              {loading ? (
                <Loader size={20} className="animate-spin" />
              ) : (
                <Search size={20} />
              )}
              <span>{loading ? 'Carregando...' : 'Buscar'}</span>
            </button>
          </div>
        </form>

        {error && (
          <div className="error-message">
            <AlertCircle size={16} />
            <div>
              <strong>Erro</strong>
              <p>{error}</p>
            </div>
          </div>
        )}

        <div className="info-box">
          <h3>ℹ️ Como usar</h3>
          <ul>
            <li>Digite o código do produto acabado (PA)</li>
            <li>Clique em "Buscar" ou pressione Enter</li>
            <li>Clique nas setas para expandir produtos</li>
            <li>Hover nos cartões para ver mais detalhes</li>
          </ul>
        </div>

        <div className="tips-box">
          <h3>💡 Dica</h3>
          <p>Use códigos de produtos que existem no seu banco de dados. Você pode encontrá-los na página de análise ou em sua consulta SQL.</p>
          <p style={{fontSize: '0.9em', marginTop: '8px', color: '#aaa'}}>
            ℹ️ Certificar-se de que o produto tem uma estrutura BOM configurada
          </p>
        </div>
      </div>

      <div className="cost-map-content">
        {loading && (
          <div className="loading-container">
            <Loader size={40} className="animate-spin" />
            <p>Gerando mapa de custos...</p>
          </div>
        )}

        {!loading && !tree && !error && (
          <div className="empty-state">
            <Search size={48} />
            <h2>Nenhum produto selecionado</h2>
            <p>Use o painel de busca para selecionar um produto</p>
          </div>
        )}

        {tree && !loading && (
          <CostMapTree tree={tree} onNodeClick={handleNodeClick} />
        )}
      </div>
    </div>
  );
};

export default CostMapPage;
