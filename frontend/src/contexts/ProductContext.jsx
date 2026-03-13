import React, { createContext, useCallback, useState } from 'react';

/**
 * Context para gerenciar estado de produtos
 * Compartilha produto selecionado, análises, etc.
 */
export const ProductContext = createContext();

export const ProductProvider = ({ children }) => {
  const [selectedProduct, setSelectedProduct] = useState(null);
  const [productData, setProductData] = useState(null);
  const [costMapTree, setCostMapTree] = useState(null);

  const updateSelectedProduct = useCallback((product) => {
    setSelectedProduct(product);
    setProductData(null);
    setCostMapTree(null);
  }, []);

  const updateProductData = useCallback((data) => {
    setProductData(data);
  }, []);

  const updateCostMapTree = useCallback((tree) => {
    setCostMapTree(tree);
  }, []);

  const clearAll = useCallback(() => {
    setSelectedProduct(null);
    setProductData(null);
    setCostMapTree(null);
  }, []);

  return (
    <ProductContext.Provider
      value={{
        selectedProduct,
        productData,
        costMapTree,
        updateSelectedProduct,
        updateProductData,
        updateCostMapTree,
        clearAll,
      }}
    >
      {children}
    </ProductContext.Provider>
  );
};

export const useProduct = () => {
  const context = React.useContext(ProductContext);
  if (!context) {
    throw new Error('useProduct deve ser usado dentro de ProductProvider');
  }
  return context;
};
