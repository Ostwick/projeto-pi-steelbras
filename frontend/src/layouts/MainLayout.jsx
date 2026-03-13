import React from 'react';
import { Link, Outlet } from 'react-router-dom';
import './MainLayout.css';

export const MainLayout = () => {
  return (
    <div className="main-layout">
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
          </ul>
        </div>
      </nav>

      <main className="main-content">
        <Outlet />
      </main>

      <footer className="app-footer">
        <p>© 2026 Cost Analysis System | Data-Driven</p>
      </footer>
    </div>
  );
};

export default MainLayout;
