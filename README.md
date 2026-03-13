# Aplicação de Análise de Custos

Uma aplicação web para análise de custos de produtos, permitindo visualizar a composição de produtos, custos de componentes e atividades/processos relacionados.

## 📋 Funcionalidades

- **Busca de Produtos**: Busque produtos por nome ou código
- **Árvore de Composição**: Visualize a composição hierárquica dos produtos
- **Análise de Custos**: Veja custo médio e histórico de custos para cada componente
- **Lista de Atividades**: Consulte atividades/processos e seus custos
- **Dashboard**: (Futuro) Análise de dados e insights sobre aumentos de preço e oportunidades de melhoria

## 🏗️ Arquitetura

```
Projeto PI/
├── backend/              # API Python FastAPI
│   ├── app/
│   │   ├── __init__.py
│   │   ├── config.py          # Configurações da aplicação
│   │   ├── database.py        # Conexão com banco de dados
│   │   ├── schemas.py         # Modelos de dados (Pydantic)
│   │   ├── routes/
│   │   │   ├── products.py    # Endpoints de produtos
│   │   │   └── queries.py     # Endpoints para consultas SQL
│   ├── main.py               # Aplicação FastAPI
│   ├── requirements.txt
│   └── .env.example
│
├── frontend/            # Interface React
│   ├── src/
│   │   ├── components/       # Componentes React
│   │   │   ├── ProductSearch.jsx
│   │   │   ├── CompositionTree.jsx
│   │   │   └── ActivitiesList.jsx
│   │   ├── pages/           # Páginas
│   │   │   └── AnalysisPage.jsx
│   │   ├── services/        # Serviços (API)
│   │   │   └── api.js
│   │   ├── App.jsx
│   │   └── main.jsx
│   ├── index.html
│   ├── vite.config.js
│   ├── package.json
│   └── .env.example
│
└── docs/               # Documentação
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

### Passo 1: Adicione suas Queries

No arquivo `backend/app/routes/queries.py`, adicione suas queries SQL:

```python
queries = {
    "sua_query": "SELECT * FROM sua_tabela WHERE...",
    "products": "SELECT * FROM seus_produtos",
    "components": "SELECT * FROM seus_componentes WHERE product_id = :product_id",
    # Adicione todas as suas queries aqui
}
```

### Passo 2: Implemente os Endpoints

No arquivo `backend/app/routes/products.py`, implemente a lógica para:

1. **Search Products**: Use sua query para buscar produtos
2. **Get Composition**: Retorne a árvore de composição
3. **Get Activities**: Liste as atividades/processos
4. **Get Summary**: Retorne o resumo do produto

Exemplo:

```python
@router.get("/search", response_model=ProductSearchResponse)
async def search_products(
    query: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    # Use sua query SQL aqui
    sql = text("SELECT * FROM products WHERE name LIKE :q")
    results = db.execute(sql, {"q": f"%{query}%"}).fetchall()
    
    products = [ProductSummary(
        product_id=r.id,
        product_name=r.name,
        product_code=r.code,
        current_cost=r.cost,
        last_update=r.updated_at
    ) for r in results]
    
    return {"products": products, "total": len(products)}
```

## 📊 Estrutura de Dados Esperada

### Produtos
```sql
SELECT 
    id as product_id,
    name as product_name,
    code as product_code,
    current_cost
FROM products
```

### Componentes
```sql
SELECT 
    id as component_id,
    name as component_name,
    quantity,
    unit_cost,
    product_id
FROM components
WHERE product_id = ?
```

### Custos Históricos
```sql
SELECT 
    component_id,
    cost_value as cost,
    cost_date as date
FROM cost_history
ORDER BY cost_date DESC
LIMIT 5
```

### Atividades
```sql
SELECT 
    id as activity_id,
    name as activity_name,
    description,
    unit_cost,
    quantity,
    total_cost,
    updated_at as last_update,
    product_id
FROM activities
WHERE product_id = ?
```

## 🎨 Personalização

### Temas e Cores

As cores principais estão definidas nos arquivos CSS:
- Primária: `#007bff` (Azul)
- Sucesso: `#28a745` (Verde)
- Fundo: `#f5f5f5` (Cinza claro)

Edite os arquivos `.css` para personalizar a aparência.

### Adicionar Novos Componentes

1. Crie um novo arquivo em `frontend/src/components/NovoComponente.jsx`
2. Importe-o em `frontend/src/pages/AnalysisPage.jsx`
3. Adicione-o ao JSX da página

## 📈 Próximos Passos - Dashboard e Análises

Para adicionar o dashboard com análises de dados:

1. **Crie uma nova página** em `frontend/src/pages/DashboardPage.jsx`
2. **Implemente componentes de gráficos** usando bibliotecas como:
   - [Chart.js](https://www.chartjs.org/) com `react-chartjs-2`
   - [Apache ECharts](https://echarts.apache.org/)
   - [Plotly.js](https://plotly.com/javascript/)

3. **Crie endpoints de análise** no backend para:
   - Análise de tendências de preço
   - Identificação de oportunidades de melhoria
   - Comparação de custo entre períodos
   - Simulações de impacto

Exemplo de endpoint para análise:
```python
@router.get("/analytics/price-trends/{product_id}")
async def get_price_trends(product_id: int, db: Session = Depends(get_db)):
    """Retorna tendência de preços do produto ao longo do tempo"""
    # Implementar lógica de análise
    pass
```

## 🤝 Contribuição

1. Crie uma branch para sua feature: `git checkout -b feature/nova-feature`
2. Commit suas mudanças: `git commit -am 'Add nova feature'`
3. Push para a branch: `git push origin feature/nova-feature`
4. Abra um Pull Request

## 📝 Licença

Projeto interno - Todos os direitos reservados

## 💬 Suporte

Para dúvidas ou problemas, consulte a documentação da API em `http://localhost:8000/docs`
