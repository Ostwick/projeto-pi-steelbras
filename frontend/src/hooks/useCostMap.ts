import { useState, useCallback } from 'react';
import { costMapService } from '../services/api';

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

interface CostMapResponse {
  success: boolean;
  produto_codigo: string;
  tree: TreeNode;
  total_items: number;
}

interface UseCostMapReturn {
  tree: TreeNode | null;
  loading: boolean;
  error: string | null;
  fetchCostMap: (produtoCodigo: string) => Promise<void>;
  totalItems: number;
}

export const useCostMap = (): UseCostMapReturn => {
  const [tree, setTree] = useState<TreeNode | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [totalItems, setTotalItems] = useState(0);

  const fetchCostMap = useCallback(
    async (produtoCodigo: string) => {
      setLoading(true);
      setError(null);

      try {
        const response = await costMapService.getStructure(produtoCodigo);
        const data: CostMapResponse = response.data;

        if (!data.success) {
          throw new Error('Produto não encontrado ou sem estrutura BOM');
        }

        setTree(data.tree);
        setTotalItems(data.total_items);
      } catch (err) {
        let errorMessage = 'Erro ao carregar estrutura de custos';
        
        if (err instanceof Error) {
          if (err.message.includes('404')) {
            errorMessage = 'Produto não encontrado no banco de dados';
          } else {
            errorMessage = err.message;
          }
        } else if (err && typeof err === 'object' && 'response' in err) {
          const axiosError = err as any;
          if (axiosError.response?.status === 404) {
            errorMessage = 'Produto não encontrado no banco de dados';
          } else if (axiosError.response?.data?.detail) {
            errorMessage = axiosError.response.data.detail;
          }
        }
        
        setError(errorMessage);
        setTree(null);
        setTotalItems(0);
      } finally {
        setLoading(false);
      }
    },
    []
  );

  return {
    tree,
    loading,
    error,
    fetchCostMap,
    totalItems,
  };
};

export default useCostMap;
