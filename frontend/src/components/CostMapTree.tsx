import React, { useState, useCallback } from 'react';
import { ChevronRight, Package, Zap } from 'lucide-react';
import './CostMapTree.css';

interface TreeNode {
  cod: string;
  desc: string;
  tipo: string;
  nivel: number;
  qtdBase: number;
  custoUnitario?: number;
  custoTotal?: number;
  origemCusto?: string | null;
  statusAlerta?: string | null;
  ultimaNf?: string | null;
  dataUltimaCompra?: string | null;
  variacaoPreco?: number | null;
  children: TreeNode[];
}

interface CostMapTreeProps {
  tree: TreeNode;
  onNodeClick?: (node: TreeNode) => void;
}

interface TreeNodeComponentProps {
  node: TreeNode;
  onNodeClick?: (node: TreeNode) => void;
}

const TreeNodeComponent: React.FC<TreeNodeComponentProps> = ({ node, onNodeClick }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const hasChildren = node.children && node.children.length > 0;
  const childrenCount = node.children ? node.children.length : 0;

  const formatCurrency = (value?: number) => {
    if (!value) return '-';
    return value.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
  };

  const formatPercent = (value?: number | null) => {
    if (value === null || value === undefined) return '-';
    return `${value.toFixed(1)}%`;
  };

  const tipoBgClass = {
    PA: 'bg-blue-600',
    SA: 'bg-green-600',
    MP: 'bg-orange-600',
    SE: 'bg-orange-600',
  }[node.tipo] || 'bg-gray-600';

  const handleToggle = (e: React.MouseEvent) => {
    e.stopPropagation();
    setIsExpanded(!isExpanded);
  };

  const handleNodeClick = () => {
    onNodeClick?.(node);
  };

  return (
    <div className="tree-node">
      <div
        className={`node-card ${hasChildren ? 'has-children' : ''} ${isExpanded ? 'expanded' : ''}`}
        onClick={handleNodeClick}
      >
        <div className={`node-header ${tipoBgClass}`}>
          {hasChildren && (
            <button className="toggle-btn" onClick={handleToggle}>
              <ChevronRight size={16} />
            </button>
          )}
          {!hasChildren && <div className="toggle-placeholder" />}

          <span className="badge">{node.tipo}</span>
          <span className="code">{node.cod}</span>
          <Package size={14} className="ml-auto opacity-50" />
        </div>

        <div className="node-desc">{node.desc}</div>

        {node.nivel === 0 && (
          <div className="node-meta">
            <span className="meta-item">
              <Zap size={12} />
              Raiz
            </span>
          </div>
        )}

        <div className="node-details">
          <div>
            <span className="detail-label">Nivel:</span>
            <span>{node.nivel}</span>
          </div>
          <div>
            <span className="detail-label">Qtd base:</span>
            <span>{Number(node.qtdBase || 0).toFixed(6)}</span>
          </div>
          <div>
            <span className="detail-label">Filhos:</span>
            <span>{childrenCount}</span>
          </div>
          <div>
            <span className="detail-label">Custo unit:</span>
            <span>{formatCurrency(node.custoUnitario)}</span>
          </div>
          <div>
            <span className="detail-label">Custo total:</span>
            <span>{formatCurrency(node.custoTotal)}</span>
          </div>
          <div>
            <span className="detail-label">Origem:</span>
            <span>{node.origemCusto || '-'}</span>
          </div>
          <div>
            <span className="detail-label">Status:</span>
            <span>{node.statusAlerta || '-'}</span>
          </div>
          <div>
            <span className="detail-label">Ultima NF:</span>
            <span>{node.ultimaNf || '-'}</span>
          </div>
          <div>
            <span className="detail-label">Var. preco:</span>
            <span>{formatPercent(node.variacaoPreco)}</span>
          </div>
        </div>
      </div>

      {isExpanded && hasChildren && (
        <div className="children">
          {node.children.map((child) => (
            <TreeNodeComponent key={child.cod} node={child} onNodeClick={onNodeClick} />
          ))}
        </div>
      )}
    </div>
  );
};

export const CostMapTree: React.FC<CostMapTreeProps> = ({ tree, onNodeClick }) => {
  const getStats = useCallback((node: TreeNode) => {
    const stats = {
      total: 1,
      pa: node.tipo === 'PA' ? 1 : 0,
      sa: node.tipo === 'SA' ? 1 : 0,
      mp: node.tipo === 'MP' ? 1 : 0,
    };

    const traverse = (n: TreeNode) => {
      stats.total++;
      if (n.tipo === 'PA') stats.pa++;
      if (n.tipo === 'SA') stats.sa++;
      if (n.tipo === 'MP') stats.mp++;

      n.children?.forEach(traverse);
    };

    node.children?.forEach(traverse);
    return stats;
  }, []);

  const stats = getStats(tree);

  return (
    <div className="cost-map-container">
      <div className="map-header">
        <h2>🗺️ Mapa de Estrutura de Custos</h2>
        <div className="stats-bar">
          <div className="stat">
            <span className="stat-label">Total</span>
            <span className="stat-value">{stats.total}</span>
          </div>
          <div className="stat">
            <span className="stat-label">PA</span>
            <span className="stat-value stat-pa">{stats.pa}</span>
          </div>
          <div className="stat">
            <span className="stat-label">SA</span>
            <span className="stat-value stat-sa">{stats.sa}</span>
          </div>
          <div className="stat">
            <span className="stat-label">MP</span>
            <span className="stat-value stat-mp">{stats.mp}</span>
          </div>
        </div>
      </div>

      <div className="tree-container">
        <TreeNodeComponent node={tree} onNodeClick={onNodeClick} />
      </div>

      <div className="legend">
        <div className="legend-item">
          <span className="legend-badge pa">PA</span>
          <span>Produto Acabado</span>
        </div>
        <div className="legend-item">
          <span className="legend-badge sa">SA</span>
          <span>Semi-Acabado</span>
        </div>
        <div className="legend-item">
          <span className="legend-badge mp">MP</span>
          <span>Matéria Prima</span>
        </div>
      </div>
    </div>
  );
};

export default CostMapTree;
