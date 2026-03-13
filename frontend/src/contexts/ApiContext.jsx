import React, { createContext, useCallback, useState } from 'react';

/**
 * Context para gerenciar estado da API
 * Fornece métodos para fazer requisições e compartilhar estado de loading/erro
 */
export const ApiContext = createContext();

export const ApiProvider = ({ children }) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const apiCall = useCallback(async (url, options = {}) => {
    setLoading(true);
    setError(null);

    try {
      const defaultOptions = {
        headers: {
          'Content-Type': 'application/json',
        },
      };

      const response = await fetch(url, { ...defaultOptions, ...options });

      if (!response.ok) {
        throw new Error(`API Error: ${response.status} ${response.statusText}`);
      }

      const data = await response.json();
      return data;
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Erro desconhecido';
      setError(errorMessage);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  const clearError = useCallback(() => {
    setError(null);
  }, []);

  return (
    <ApiContext.Provider value={{ loading, error, apiCall, clearError }}>
      {children}
    </ApiContext.Provider>
  );
};

export const useApi = () => {
  const context = React.useContext(ApiContext);
  if (!context) {
    throw new Error('useApi deve ser usado dentro de ApiProvider');
  }
  return context;
};
