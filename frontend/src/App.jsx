import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { ApiProvider, ProductProvider } from './contexts';
import MainLayout from './layouts/MainLayout';
import AnalysisPage from './pages/AnalysisPage';
import CostMapPage from './pages/CostMapPage';
import SyncPage from './pages/SyncPage';
import SettingsPage from './pages/SettingsPage';
import './App.css';

function App() {
  return (
    <ApiProvider>
      <ProductProvider>
        <Router>
          <Routes>
            <Route element={<MainLayout />}>
              <Route path="/" element={<AnalysisPage />} />
              <Route path="/cost-map" element={<CostMapPage />} />
              <Route path="/sync" element={<SyncPage />} />
              <Route path="/settings" element={<SettingsPage />} />
            </Route>
          </Routes>
        </Router>
      </ProductProvider>
    </ApiProvider>
  );
}

export default App;
