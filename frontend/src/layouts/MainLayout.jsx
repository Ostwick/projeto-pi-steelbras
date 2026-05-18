import React, { useState, useEffect } from 'react';
import { Link, Outlet } from 'react-router-dom';
import { Sun, Moon } from 'lucide-react';
import './MainLayout.css';

export const MainLayout = () => {
  const [isDarkMode, setIsDarkMode] = useState(true);

  useEffect(() => {
    // Load theme from localStorage
    const savedTheme = localStorage.getItem('theme');
    if (savedTheme) {
      setIsDarkMode(savedTheme === 'dark');
    }
  }, []);

  const toggleTheme = () => {
    const newMode = !isDarkMode;
    setIsDarkMode(newMode);
    localStorage.setItem('theme', newMode ? 'dark' : 'light');
  };

  return (
    <div className={`main-layout ${isDarkMode ? 'dark-mode' : 'light-mode'}`}>
      <nav className="navbar">
        <div className="nav-container">
          <Link to="/" className="nav-logo">
            📊 CostAnalysis
          </Link>

          <ul className="nav-menu">
            <li className="nav-item">
              <Link to="/" className="nav-link">
                Análise
              </Link>
            </li>
            <li className="nav-item">
              <Link to="/cost-map" className="nav-link">
                Mapa de Custos
              </Link>
            </li>
            <li className="nav-item">
              <Link to="/sync" className="nav-link">
                Sincronização
              </Link>
            </li>
            <li className="nav-item">
              <Link to="/settings" className="nav-link">
                ⚙️ Configurações
              </Link>
            </li>
            <li className="nav-item">
              <button 
                className="theme-toggle" 
                onClick={toggleTheme}
                title={isDarkMode ? 'Ativar modo claro' : 'Ativar modo escuro'}
              >
                {isDarkMode ? <Sun size={20} /> : <Moon size={20} />}
              </button>
            </li>
          </ul>
        </div>
      </nav>

      <main className="main-content">
        <Outlet />
      </main>

      <footer className="app-footer">
        <p>© 2026 Grupo G20 | Steelbras</p>
      </footer>
    </div>
  );
};

export default MainLayout;
