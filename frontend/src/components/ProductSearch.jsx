import React from 'react';
import './ProductSearch.css';

const ProductSearch = ({ onSearch, isLoading }) => {
  const [searchQuery, setSearchQuery] = React.useState('');

  const handleSubmit = (e) => {
    e.preventDefault();
    if (searchQuery.trim()) {
      onSearch(searchQuery);
      setSearchQuery('');
    }
  };

  return (
    <div className="product-search">
      <h2>Buscar Produto</h2>
      <form onSubmit={handleSubmit}>
        <input
          type="text"
          placeholder="Digite o nome ou código do produto..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          disabled={isLoading}
          className="search-input"
        />
        <button type="submit" disabled={isLoading} className="search-button">
          {isLoading ? 'Buscando...' : 'Buscar'}
        </button>
      </form>
    </div>
  );
};

export default ProductSearch;
