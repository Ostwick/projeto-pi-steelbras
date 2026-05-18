# 📊 Estrutura do Projeto de Análise de Custos

## Visão Geral

```
Projeto PI/
│
├── 📁 backend/                          # API Python FastAPI
│   ├── 📁 app/                         # Módulo principal da aplicação
│   │   ├── __init__.py                 # Inicialização do módulo
│   │   ├── config.py                   # ⚙️ Configurações e credenciais
│   │   ├── database.py                 # 🗄️ Conexão com SQL Server
│   │   ├── schemas.py                  # 📋 Modelos de dados (Pydantic)
│   │   └── 📁 routes/
│   │       ├── __init__.py
│   │       ├── products.py             # 🛣️ Endpoints de produtos
│   │       │   └── GET /api/products/search
│   │       │   └── GET /api/products/{id}/composition
│   │       │   └── GET /api/products/{id}/activities
│   │       │   └── GET /api/products/{id}/summary
│   │       ├── cost_map.py             # 🛣️ Endpoints de mapa de custos
│   │       │   └── GET /api/cost-map/tree
│   │       │   └── GET /api/cost-map/export
│   │       ├── queries.py              # 🛣️ Endpoints para queries SQL
│   │       │   └── POST /api/queries/execute
│   │       ├── settings.py             # ⚙️ Endpoints de configurações
│   │       │   └── GET/POST /api/settings
│   │       └── sync.py                 # 🔄 Endpoints de sincronização
│   │           └── POST /api/sync/execute
│   ├── 📁 services/
│   │   └── sync_service.py             # 🔧 Lógica de sincronização
│   ├── main.py                         # 🚀 Aplicação FastAPI principal
│   ├── requirements.txt                # 📦 Dependências Python
│   ├── .env.example                    # 📝 Exemplo de variáveis
│   └── .env                            # ⚠️ Adicionar suas credenciais aqui!
│
├── 📁 frontend/                        # Interface React + Vite
│   ├── 📁 src/
│   │   ├── 📁 components/
│   │   │   ├── ProductSearch.jsx       # ✍️ Buscar produtos
│   │   │   ├── ProductSearch.css
│   │   │   ├── CompositionTree.jsx     # 🌳 Mostrar composição
│   │   │   ├── CompositionTree.css
│   │   │   ├── CostMapTree.tsx         # 💰 Visualizar mapa de custos
│   │   │   ├── CostMapTree.css
│   │   │   ├── ActivitiesList.jsx      # 📋 Listar atividades
│   │   │   └── ActivitiesList.css
│   │   ├── 📁 contexts/
│   │   │   ├── ApiContext.jsx          # 🔌 Contexto da API
│   │   │   ├── ProductContext.jsx      # 📦 Contexto de produtos
│   │   │   └── index.js                # Exports dos contextos
│   │   ├── 📁 hooks/
│   │   │   └── useCostMap.ts           # 🎣 Hook customizado para custos
│   │   ├── 📁 layouts/
│   │   │   ├── MainLayout.jsx          # 🎨 Layout principal
│   │   │   ├── MainLayout.css
│   │   │   └── index.js
│   │   ├── 📁 pages/
│   │   │   ├── AnalysisPage.jsx        # 📄 Página principal
│   │   │   ├── AnalysisPage.css
│   │   │   ├── CostMapPage.tsx         # 📊 Página de mapa de custos
│   │   │   ├── CostMapPage.css
│   │   │   ├── SettingsPage.jsx        # ⚙️ Página de configurações
│   │   │   ├── SettingsPage.css
│   │   │   ├── SyncPage.jsx            # 🔄 Página de sincronização
│   │   │   └── SyncPage.css
│   │   ├── 📁 services/
│   │   │   └── api.js                  # 🔌 Cliente HTTP (axios)
│   │   ├── App.jsx                     # 🎨 Componente raiz
│   │   ├── App.css
│   │   └── main.jsx                    # 🔧 Entry point
│   ├── index.html                      # 📄 HTML principal
│   ├── vite.config.js                  # ⚙️ Config Vite
│   ├── package.json                    # 📦 Dependências Node
│   ├── .env.example                    # 📝 Variáveis de exemplo
│   └── .env                            # ⚠️ Configurar se necessário
│
├── 📁 queries/                         # 📚 Queries SQL reutilizáveis
│   └── BOM.sql                         # 📋 Bill of Materials
│
├── README.md                           # 📖 Documentação principal
├── ARQUITETURA.md                      # 📐 Visão técnica (este arquivo)
└── .gitignore                          # 🔒 Ignorar no Git
```

## 🎯 Fluxo de Dados

```
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│  Frontend (React + Vite)                                               │
│  ├── ProductSearch ──────────────────┐                                 │
│  ├── CompositionTree                 │                                 │
│  ├── CostMapTree                     │  Context API                    │
│  ├── ActivitiesList                  │  (ApiContext,                   │
│  ├── SettingsPage                    │   ProductContext)               │
│  └── SyncPage ──────────────────────┘                                  │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
                              │
                              │ HTTP REST API (JSON)
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│  Backend (FastAPI)                                                     │
│  ├── /api/products/*          ◄── ProductSearch                        │
│  ├── /api/cost-map/*          ◄── CostMapTree + CostMapPage           │
│  ├── /api/queries/*           ◄── Advanced Queries                     │
│  ├── /api/settings/*          ◄── SettingsPage                        │
│  └── /api/sync/*              ◄── SyncPage + sync_service             │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
                              │
                              │ SQL Queries via pyodbc
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│  SQL Server Database                                                   │
│  ├── Produtos                                                          │
│  ├── Componentes                                                       │
│  ├── Custos (histórico)                                                │
│  ├── Atividades/Processos                                              │
│  └── Sincronização de dados                                            │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

## 🔑 Componentes Principais

### Backend (FastAPI)

**Responsabilidades:**
- Receber requisições HTTP do frontend
- Executar queries SQL contra SQL Server
- Retornar dados em formato JSON
- Validar e transformar dados usando Pydantic
- Gerenciar sincronização de dados
- Fornecer configurações da aplicação

**Tecnologias:**
- `FastAPI` - Framework web rápido e moderno
- `SQLAlchemy` - ORM para SQL
- `pyodbc` - Driver para SQL Server
- `Pydantic` - Validação de dados

**Routes principais:**
1. **products.py** - Busca, composição e resumo de produtos
2. **cost_map.py** - Visualização hierárquica de custos
3. **queries.py** - Execução de queries SQL customizadas
4. **settings.py** - Gerenciamento de configurações
5. **sync.py** - Sincronização de dados com fontes externas

**Services:**
- **sync_service.py** - Lógica de sincronização de dados

### Frontend (React + TypeScript/JavaScript)

**Responsabilidades:**
- Interface visual para usuário
- Entrada de dados de busca e filtros
- Exibir composição, custos e atividades
- Gerenciar estado da aplicação via Context API
- Sincronizar com backend via API REST

**Tecnologias:**
- `React` - Biblioteca UI
- `Vite` - Build tool rápido
- `Axios` - Cliente HTTP
- `TypeScript/JavaScript` - Linguagens
- `CSS Vanilla` - Estilos

**Componentes principais:**
1. **ProductSearch** - Busca de produtos
2. **CompositionTree** - Árvore de composição com custos
3. **CostMapTree** - Visualização hierárquica de mapa de custos (TypeScript)
4. **ActivitiesList** - Lista de atividades/processos
5. **MainLayout** - Layout principal compartilhado

**Pages (Páginas):**
1. **AnalysisPage** - Página principal de análise
2. **CostMapPage** - Visualização completa do mapa de custos
3. **SettingsPage** - Configurações da aplicação
4. **SyncPage** - Página de sincronização

**Contexts:**
- **ApiContext** - Gerencia conexão com API
- **ProductContext** - Gerencia estado de produtos

**Hooks:**
- **useCostMap** - Hook customizado para dados de mapa de custos

## 📋 Schemas (Modelos de Dados)

```python
# Produto
{
  product_id: int,
  product_name: str,
  product_code: str,
  current_cost: float,
  last_update: datetime
}

# Composição
{
  product_id: int,
  product_name: str,
  total_cost: float,
  average_cost: float,
  components: [
    {
      component_id: int,
      component_name: str,
      quantity: float,
      unit_cost: float,
      total_cost: float,
      average_cost: float,
      last_costs: [
        { value: float, date: str }
      ]
    }
  ]
}

# Mapa de Custos
{
  product_id: int,
  structure: {
    id: int,
    name: str,
    type: 'product' | 'component' | 'activity',
    cost: float,
    quantity: float,
    children: [...]
  }
}

# Atividade
{
  activity_id: int,
  activity_name: str,
  description: str,
  unit_cost: float,
  quantity: float,
  total_cost: float,
  last_update: datetime
}
```

## 🛣️ Endpoints da API

```
┌─────────────────────────────────────────────────────────┐
│                   API REST                              │
├─────────────┬──────────────┬──────────────────────────┤
│ Método      │ Endpoint     │ Resultado                │
├─────────────┼──────────────┼──────────────────────────┤
│ GET         │ /            │ Info da API              │
│ GET         │ /health      │ Status da API            │
│                                                         │
│ PRODUCTS ENDPOINTS                                      │
│ GET         │ /api/        │ Buscar produtos          │
│             │ products/    │                          │
│             │ search       │                          │
│             │ ?query=...   │                          │
│             │                                          │
│ GET         │ /api/        │ Composição do produto    │
│             │ products/{id}│                          │
│             │ /composition │                          │
│             │                                          │
│ GET         │ /api/        │ Atividades do produto    │
│             │ products/{id}│                          │
│             │ /activities  │                          │
│             │                                          │
│ GET         │ /api/        │ Resumo do produto        │
│             │ products/{id}│                          │
│             │ /summary     │                          │
│                                                         │
│ COST MAP ENDPOINTS                                      │
│ GET         │ /api/        │ Árvore hierárquica de    │
│             │ cost-map/tree│ custos                   │
│             │                                          │
│ GET         │ /api/        │ Exportar mapa de custos  │
│             │ cost-map/    │ em JSON ou CSV           │
│             │ export       │                          │
│                                                         │
│ SETTINGS ENDPOINTS                                      │
│ GET         │ /api/settings│ Obter configurações      │
│ POST        │ /api/settings│ Atualizar configurações  │
│                                                         │
│ SYNC ENDPOINTS                                          │
│ POST        │ /api/sync/   │ Executar sincronização   │
│             │ execute      │ de dados                 │
│             │                                          │
│ GET         │ /api/sync/   │ Status da sincronização  │
│             │ status       │                          │
│                                                         │
│ QUERIES ENDPOINTS                                       │
│ POST        │ /api/        │ Executar query SQL       │
│             │ queries/     │ customizada              │
│             │ execute      │                          │
│                                                         │
│ GET         │ /api/        │ Health check             │
│             │ queries/     │                          │
│             │ health       │                          │
└─────────────┴──────────────┴──────────────────────────┘

DOCUMENTAÇÃO INTERATIVA:
GET /docs        → Swagger UI
GET /redoc       → ReDoc
```

## 🗄️ Estrutura SQL Server Esperada

```sql
┌──────────────────┐
│    Produtos      │
├──────────────────┤
│ ID (PK)          │
│ Nome             │
│ Codigo           │
│ CustoAtual       │
│ DataAtualizacao  │
└──────────────────┘
        │
        │ 1:N
        ▼
┌──────────────────────┐
│   Componentes        │
├──────────────────────┤
│ ID (PK)              │
│ Nome                 │
│ ProdutoID (FK)       │
│ Quantidade           │
│ CustoUnitario        │
│ TipoCusto            │
│ Fornecedor           │
└──────────────────────┘
        │
        │ 1:N
        ▼
┌──────────────────────┐
│  HistoricoCustos     │
├──────────────────────┤
│ ID (PK)              │
│ ComponenteID (FK)    │
│ Valor                │
│ DataCusto            │
└──────────────────────┘

        ┌──────────────────────┐
        │    Atividades        │
        ├──────────────────────┤
        │ ID (PK)              │
        │ Nome                 │
        │ ProdutoID (FK)       │
        │ TipoAtividade        │
        │ CustoMaoDeObra       │
        │ CustoMaquina         │
        │ CustoTotal           │
        │ DataAtualizacao      │
        └──────────────────────┘
```

## 🔄 Fluxo de Integração com SQL

1. **Adicione suas queries** em `backend/app/routes/products.py`
2. **Execute via SQLAlchemy** usando `text()` e `db.execute()`
3. **Transforme em Pydantic schemas** para validação
4. **Retorne como JSON** via FastAPI

Exemplo:
```python
@router.get("/search")
async def search_products(query: str, db: Session = Depends(get_db)):
    sql = text("SELECT * FROM Produtos WHERE Nome LIKE :q")
    results = db.execute(sql, {"q": f"%{query}%"}).fetchall()
    return [ProductSummary(...) for r in results]
```

## 🚀 Como Rodar

**Terminal 1 - Backend:**
```bash
cd backend
venv\Scripts\activate
python main.py
# API em http://localhost:8000
```

**Terminal 2 - Frontend:**
```bash
cd frontend
npm run dev
# App em http://localhost:5173
```

## 📚 Onde Cada Coisa Está

| O que preciso?              | Arquivo                          |
|-----------------------------|----------------------------------|
| Adicionar queries SQL       | `backend/app/routes/products.py` |
| Implementar mapa de custos  | `backend/app/routes/cost_map.py` |
| Gerenciar sincronização     | `backend/app/services/sync_service.py` |
| Configurar BD               | `backend/app/config.py` + `backend/.env` |
| Entender componentes UI     | `frontend/src/components/`       |
| Adicionar novas páginas     | `frontend/src/pages/`            |
| Gerenciar estado global     | `frontend/src/contexts/`         |
| Custom hooks                | `frontend/src/hooks/`            |
| Queries SQL reutilizáveis   | `queries/BOM.sql`                |
| Layout compartilhado        | `frontend/src/layouts/`          |
