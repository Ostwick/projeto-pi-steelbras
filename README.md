# Aplicação de Análise de Custos

Uma aplicação web para análise de custos de produtos, permitindo visualizar a composição de produtos, custos de componentes e atividades/processos relacionados.

## 📋 Funcionalidades

- **Busca de Produtos**: Busque produtos por nome ou código
- **Árvore de Composição**: Visualize a composição hierárquica dos produtos
- **Análise de Custos**: Veja custo médio e histórico de custos para cada componente
- **Mapa de Custos**: Visualização hierárquica de custos por componentes e atividades
- **Lista de Atividades**: Consulte atividades/processos e seus custos
- **Sincronização de Dados**: Sincronize dados com fontes externas
- **Configurações**: Gerencie as configurações da aplicação
- **Dashboard**: (Futuro) Análise de dados e insights sobre aumentos de preço e oportunidades de melhoria

## 🏗️ Arquitetura

```
Projeto PI/
├── backend/                  # API Python FastAPI
│   ├── app/
│   │   ├── __init__.py
│   │   ├── config.py         # Configurações da aplicação
│   │   ├── database.py       # Conexão com banco de dados
│   │   ├── schemas.py        # Modelos de dados (Pydantic)
│   │   ├── routes/
│   │   │   ├── products.py   # Endpoints de produtos
│   │   │   ├── cost_map.py   # Endpoints de mapa de custos
│   │   │   ├── queries.py    # Endpoints para consultas SQL
│   │   │   ├── settings.py   # Endpoints de configurações
│   │   │   └── sync.py       # Endpoints de sincronização
│   │   └── services/
│   │       └── sync_service.py   # Serviço de sincronização
│   ├── main.py               # Aplicação FastAPI
│   ├── requirements.txt
│   └── .env.example
│
├── frontend/                 # Interface React + Vite
│   ├── src/
│   │   ├── components/       # Componentes React
│   │   │   ├── ProductSearch.jsx/css
│   │   │   ├── CompositionTree.jsx/css
│   │   │   ├── CostMapTree.tsx/css
│   │   │   └── ActivitiesList.jsx/css
│   │   ├── contexts/         # Context API
│   │   │   ├── ApiContext.jsx
│   │   │   ├── ProductContext.jsx
│   │   │   └── index.js
│   │   ├── hooks/            # Custom Hooks
│   │   │   └── useCostMap.ts
│   │   ├── layouts/          # Layouts da aplicação
│   │   │   ├── MainLayout.jsx/css
│   │   │   └── index.js
│   │   ├── pages/            # Páginas
│   │   │   ├── AnalysisPage.jsx/css
│   │   │   ├── CostMapPage.tsx/css
│   │   │   ├── SettingsPage.jsx/css
│   │   │   └── SyncPage.jsx/css
│   │   ├── services/         # Serviços (API)
│   │   │   └── api.js
│   │   ├── App.jsx
│   │   ├── App.css
│   │   └── main.jsx
│   ├── index.html
│   ├── vite.config.js
│   ├── package.json
│   └── .env.example
│
├── queries/                  # Queries SQL
│   └── BOM.sql
│
├── ARQUITETURA.md           # Documentação técnica detalhada
├── README.md                # Este arquivo
└── .gitignore
```

## 🚀 Quick Start

### Backend Setup

1. **Navegue para a pasta backend:**
   ```bash
   cd backend
   ```

2. **Crie um ambiente virtual Python:**
   ```bash
   python -m venv venv
   source venv\Scripts\activate  # Windows
   # ou no Linux/Mac: source venv/bin/activate
   ```

3. **Configure as variáveis de ambiente:**
   ```bash
   cp .env.example .env
   # Edite .env com suas credenciais do SQL Server
   ```

4. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

5. **Execute a API:**
   ```bash
   python main.py
   ```

   A API estará disponível em: `http://localhost:8000`
   - Documentação Swagger: `http://localhost:8000/docs`
   - ReDoc: `http://localhost:8000/redoc`

### Frontend Setup

1. **Em outro terminal, navegue para a pasta frontend:**
   ```bash
   cd frontend
   ```

2. **Instale as dependências:**
   ```bash
   npm install
   ```

3. **Configure as variáveis de ambiente:**
   ```bash
   cp .env.example .env
   ```

4. **Execute o servidor de desenvolvimento:**
   ```bash
   npm run dev
   ```

   A aplicação estará disponível em: `http://localhost:5173`

## 🔧 Integração com suas Queries SQL

### Produtos

No arquivo `backend/app/routes/products.py`, implemente endpoints para:

1. **Search Products**: Busca de produtos por nome/código
   ```sql
   SELECT id as product_id,
          name as product_name,
          code as product_code,
          current_cost
   FROM products
   WHERE name LIKE ? OR code LIKE ?
   ```

2. **Get Composition**: Árvore de composição com custos
3. **Get Activities**: Lista de atividades/processos
4. **Get Summary**: Resumo do produto

### Mapa de Custos

No arquivo `backend/app/routes/cost_map.py`, implemente:

1. **GET /api/cost-map/tree** - Retorna árvore hierárquica de custos
2. **GET /api/cost-map/export** - Exporta mapa em JSON/CSV

### Sincronização

No arquivo `backend/app/routes/sync.py`, implemente:

1. **POST /api/sync/execute** - Executa sincronização de dados
2. **GET /api/sync/status** - Retorna status da última sincronização

Use `backend/app/services/sync_service.py` para implementar a lógica.

### Queries Customizadas

No arquivo `backend/app/routes/queries.py`:

```python
@router.post("/api/queries/execute")
async def execute_query(query: QueryRequest, db: Session = Depends(get_db)):
    # Validar e executar query customizada
    results = db.execute(text(query.sql)).fetchall()
    return {"results": results}
```

### Configurações

No arquivo `backend/app/routes/settings.py`:

```python
@router.get("/api/settings")
async def get_settings(db: Session = Depends(get_db)):
    # Retornar configurações da aplicação
    pass

@router.post("/api/settings")
async def update_settings(settings: SettingsSchema, db: Session = Depends(get_db)):
    # Atualizar configurações
    pass
```

## 🎨 Páginas e Componentes

### Páginas Principais

**AnalysisPage** (`frontend/src/pages/AnalysisPage.jsx`)
- Página inicial com busca de produtos
- Exibe composição e custos
- Mostra atividades/processos

**CostMapPage** (`frontend/src/pages/CostMapPage.tsx`)
- Visualização hierárquica completa do mapa de custos
- Análise detalhada de componentes e atividades

**SettingsPage** (`frontend/src/pages/SettingsPage.jsx`)
- Gerenciamento de configurações da aplicação
- Integração com `backend/app/routes/settings.py`

**SyncPage** (`frontend/src/pages/SyncPage.jsx`)
- Controle de sincronização de dados
- Monitoramento de status
- Integração com `backend/app/services/sync_service.py`

### Componentes Reutilizáveis

- **ProductSearch**: Campo de busca com autocomplete
- **CompositionTree**: Árvore visual de componentes
- **CostMapTree**: Visualização hierárquica de custos (TypeScript)
- **ActivitiesList**: Tabela de atividades/processos
- **MainLayout**: Layout compartilhado entre páginas

### Gerenciamento de Estado

**Contexts** (`frontend/src/contexts/`)
- **ApiContext**: Gerencia conexão e chamadas à API
- **ProductContext**: Armazena dados de produtos em cache

**Hooks** (`frontend/src/hooks/`)
- **useCostMap**: Hook para gerenciar dados de mapa de custos

### Personalização

As cores principais estão definidas nos arquivos CSS:
- Primária: `#007bff` (Azul)
- Sucesso: `#28a745` (Verde)
- Fundo: `#f5f5f5` (Cinza claro)

Para adicionar novos componentes:
1. Crie em `frontend/src/components/NovoComponente.jsx`
2. Importe em `frontend/src/contexts/` ou `frontend/src/pages/`
3. Integre com os contextos existentes

## 📂 Estrutura de Pastas Detalhada

**Backend:**
```
backend/
├── app/
│   ├── routes/
│   │   ├── products.py    # ⭐ Busca e composição
│   │   ├── cost_map.py    # ⭐ Mapa de custos
│   │   ├── settings.py    # ⭐ Configurações
│   │   ├── sync.py        # ⭐ Sincronização
│   │   └── queries.py     # Queries customizadas
│   ├── services/
│   │   └── sync_service.py # Lógica de sincronização
│   ├── config.py          # Variáveis de ambiente
│   ├── database.py        # Conexão BD
│   └── schemas.py         # Modelos Pydantic
├── main.py                # FastAPI app
└── requirements.txt       # Dependências
```

**Frontend:**
```
frontend/src/
├── pages/
│   ├── AnalysisPage.jsx    # ⭐ Principal
│   ├── CostMapPage.tsx     # ⭐ Mapa de custos
│   ├── SettingsPage.jsx    # ⭐ Configurações
│   └── SyncPage.jsx        # ⭐ Sincronização
├── components/
│   ├── ProductSearch.jsx
│   ├── CompositionTree.jsx
│   ├── CostMapTree.tsx
│   └── ActivitiesList.jsx
├── contexts/
│   ├── ApiContext.jsx
│   └── ProductContext.jsx
├── hooks/
│   └── useCostMap.ts
├── layouts/
│   └── MainLayout.jsx
└── services/
    └── api.js
```

## 📚 Próximos Passos

1. **Implemente as queries** em `backend/app/routes/products.py`
2. **Configure** `.env` com credenciais do SQL Server
3. **Teste endpoints** em `http://localhost:8000/docs`
4. **Customize** as páginas conforme necessário
5. **Implemente sincronização** em `backend/app/services/sync_service.py`
6. **Adicione alertas** e lógica de decisão

## 📝 Exemplo de Estrutura SQL

**Tabelas esperadas:**
- `Produtos` (id, name, code, current_cost)
- `Componentes` (id, name, product_id, quantity, unit_cost)
- `HistoricoCustos` (id, component_id, valor, data)
- `Atividades` (id, name, product_id, unit_cost, total_cost)