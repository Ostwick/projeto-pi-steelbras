import React from 'react';
import './CompositionTree.css';

const CompositionTree = ({ composition, isLoading }) => {
  if (isLoading) {
    return <div className="composition-tree loading">Carregando composição...</div>;
  }

  if (!composition) {
    return null;
  }

  const renderComponent = (component, level = 0) => {
    return (
      <div key={component.component_id} className={`component level-${level}`}>
        <div className="component-header">
          <div className="component-info">
            <strong className="component-code">{component.component_id}</strong>
            <span className="component-name">{component.component_name}</span>
          </div>
          <div className="component-costs">
            <div className="cost-item">
              <span className="label">Quantidade:</span>
              <span className="value">{component.quantity}</span>
            </div>
            <div className="cost-item">
              <span className="label">Custo Unitário:</span>
              <span className="value">R$ {component.unit_cost.toFixed(2)}</span>
            </div>
            <div className="cost-item">
              <span className="label">Custo Total:</span>
              <span className="value highlight">R$ {component.total_cost.toFixed(2)}</span>
            </div>
            <div className="cost-item">
              <span className="label">Custo Médio:</span>
              <span className="value">R$ {component.average_cost.toFixed(2)}</span>
            </div>
          </div>
        </div>
        
        {component.last_costs && component.last_costs.length > 0 && (
          <div className="cost-history">
            <div className="history-header">Últimos Custos</div>
            <div className="history-list">
              {component.last_costs.slice(0, 5).map((cost, idx) => (
                <div key={idx} className="history-item">
                  <span className="history-date">{cost.date}</span>
                  <span className="history-cost">R$ {cost.value.toFixed(2)}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="composition-tree">
      <div className="composition-header">
        <h3>{composition.product_name}</h3>
        <div className="composition-totals">
          <div className="total-item">
            <span className="label">Custo Total:</span>
            <span className="value highlight">R$ {composition.total_cost.toFixed(2)}</span>
          </div>
          <div className="total-item">
            <span className="label">Custo Médio:</span>
            <span className="value">R$ {composition.average_cost.toFixed(2)}</span>
          </div>
        </div>
      </div>

      <div className="components-list">
        <h4>Componentes</h4>
        {composition.components.map((component) => renderComponent(component))}
      </div>
    </div>
  );
};

export default CompositionTree;
