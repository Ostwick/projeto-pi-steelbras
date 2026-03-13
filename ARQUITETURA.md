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
│   │       └── queries.py              # 🛣️ Endpoints para queries SQL
│   │           └── POST /api/queries/execute
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
│   │   │   ├── ActivitiesList.jsx      # 📋 Listar atividades
│   │   │   └── ActivitiesList.css
│   │   ├── 📁 pages/
│   │   │   ├── AnalysisPage.jsx        # 📄 Página principal
│   │   │   └── AnalysisPage.css
│   │   ├── 📁 services/
│   │   │   └── api.js                  # 🔌 Cliente HTTP (axios)
│   │   ├── App.jsx                     # 🎨 Componente raiz
│   │   ├── App.css
│   │   ├── main.jsx                    # 🔧 Entry point
│   │
│   ├── index.html                      # 📄 HTML principal
│   ├── vite.config.js                  # ⚙️ Config Vite
│   ├── package.json                    # 📦 Dependências Node
│   ├── .env.example                    # 📝 Variáveis de exemplo
│   └── .env                            # ⚠️ Configurar se necessário
│
├── 📁 docs/                            # 📚 Documentação
│   ├── SETUP.md                        # 🔧 Guia completo de instalação
│   ├── INTEGRACAO_SQL.md              # 🔌 Como integrar suas queries
│   └── EXEMPLO_SQL_SERVER.md          # 💾 Scripts SQL de exemplo
│
├── 📁 .github/
│   └── copilot-instructions.md        # 🤖 Instruções para IA
│
├── COMECE_AQUI.md                      # 🚀 Guia rápido
├── README.md                           # 📖 Documentação principal
├── .gitignore                          # 🔒 Ignorar no Git
│
└── 📝 ARQUITETURA.md (este arquivo)   # 📐 Visão técnica
```

## 🎯 Fluxo de Dados

```
┌─────────────────┐       ┌──────────────────┐       ┌──────────────┐
│  Frontend React │──────▶│ Backend FastAPI  │──────▶│  SQL Server  │
│  (localhost:5173) API   │ (localhost:8000) │ Queries│ Database     │
│                 │◀──────│                  │◀──────│              │
│ - Busca         │ JSON  │ - Rotas          │ Data  │ - Produtos   │
│ - Composição    │       │ - Schemas        │       │ - Componentes│
│ - Atividades    │       │ - Config BD      │       │ - Custos     │
│                 │       │ - Queries SQL    │       │ - Atividades │
└─────────────────┘       └──────────────────┘       └──────────────┘
```

## 🔑 Componentes Principais

### Backend (FastAPI)

**Responsabilidades:**
- Receber requisições HTTP do frontend
- Executar queries SQL contra SQL Server
- Retornar dados em formato JSON
- Validar e transformar dados usando Pydantic

**Tecnologias:**
- `Fastapi` - Framework web rápido e moderno
- `SQLAlchemy` - ORM para SQL
- `pyodbc` - Driver para SQL Server
- `Pydantic` - Validação de dados

**Como funciona:**
1. Frontend envia requisição GET/POST
2. Route em `routes/products.py` recebe a requisição
3. Schema valida os dados
4. Database executa query SQL
5. Resultado é transformado e retornado como JSON

### Frontend (React)

**Responsabilidades:**
- Interface visual para usuário
- Eingeber dados de busca
- Exibir composição e custos
- Listar atividades

**Tecnologias:**
- `React` - Biblioteca UI
- `Vite` - Build tool rápido
- `Axios` - Cliente HTTP
- `CSS Vanilla` - Estilos

**Componentes:**
1. **ProductSearch** - Permite buscar produtos
2. **CompositionTree** - Mostra árvore de componentes com custos
3. **ActivitiesList** - Lista atividades/processos

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
│ QUERIES ENDPOINTS                                       │
│ POST        │ /api/        │ Executar query           │
│             │ queries/     │ customizada              │
│             │ execute      │                          │
│             │                                          │
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

| O que preciso?         | Arquivo                        |
|------------------------|--------------------------------|
| Adicionar queries SQL  | `backend/app/routes/products.py` |
| Configurar BD          | `backend/app/config.py` + `backend/.env` |
| Customizar UI          | `frontend/src/components/` |
| Entender integração    | `docs/INTEGRACAO_SQL.md` |
| Scripts SQL exemplo    | `docs/EXEMPLO_SQL_SERVER.md` |
| Instalar tudo          | `docs/SETUP.md` |
| Começar rápido         | `COMECE_AQUI.md` |

## ✅ Próximos Passos

1. **Completar `backend/.env`** com suas credenciais SQL Server
2. **Adaptar `/routes/products.py`** com suas queries
3. **Rodar e testar** em `http://localhost:8000/docs`
4. **Customizar frontend** conforme necessário
5. **Adicionar dashboard** e análises no futuro

---

**Status:** ✅ Pronto para desenvolvimento!
