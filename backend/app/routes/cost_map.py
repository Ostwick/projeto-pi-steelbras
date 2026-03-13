from typing import Any, Dict, List, Optional
from datetime import datetime, date
from fastapi import APIRouter, Depends, HTTPException
import logging
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import settings
from app.services.sync_service import BOM_COST_ROLLUP_SQL
from app.database import get_source_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/cost-map", tags=["cost-map"])


class CostMapRequest(BaseModel):
    produto_codigo: str
    data_inicio: date = Field(default_factory=lambda: date(2025, 1, 1))
    data_fim: date = Field(default_factory=lambda: date(2025, 12, 31))
    filial_codigo: int = 8637511000120


class BomItem(BaseModel):
    produto_codigo: str
    produto_descricao: str
    tipo_produto: str
    tem_estrutura: str
    numero_hierarquia: str
    nivel: int
    pai_codigo: Optional[str] = None
    quantidade_base: float


class OpData(BaseModel):
    produto_codigo: str
    op_numero: int
    centro_custo: str
    quantidade: float
    tempo_horas: float
    qtd_por_hora: float
    custo_mo: float
    mo_por_hora: float
    mo_por_peca: float
    custo_mp: float
    mp_por_peca: float
    data_op: date
    tipo_filtro: str


class NfData(BaseModel):
    produto_codigo: str
    nf_numero: str
    data_nf: date
    quantidade: float
    valor_total: float
    icms: float
    valor_compra: float
    qtd_utilizada: float
    mp_por_peca: float


class CostMapResponse(BaseModel):
    bom_structure: List[BomItem]
    ops_data: List[OpData]
    nfs_data: List[NfData]
    summary: Dict[str, Any]


@router.post("/generate")
async def generate_cost_map(
    request: CostMapRequest,
    db: Session = Depends(get_source_db),
) -> CostMapResponse:
    """
    Gera o mapa de custos recursivo com estrutura BOM, OPs e NFs.
    
    Parâmetros:
    - produto_codigo: Código do produto acabado para análise
    - data_inicio: Data inicial para filtro de OPs (padrão: 01/01/2025)
    - data_fim: Data final para filtro de OPs (padrão: 31/12/2025)
    - filial_codigo: Código da filial (padrão: 8637511000120)
    """
    
    try:
        # Executar query que retorna 3 result sets
        query = text("""
DECLARE @ProdutoCodigo CHAR(60) = :produto_codigo;
DECLARE @DataInicio DATE = :data_inicio;
DECLARE @DataFim DATE = :data_fim;
DECLARE @FilialCodigo DECIMAL(14,0) = :filial_codigo;

IF OBJECT_ID('tempdb..#BomExplodida') IS NOT NULL DROP TABLE #BomExplodida;

CREATE TABLE #BomExplodida (
    Id INT IDENTITY(1,1),
    Nivel INT,
    ProdutoPai CHAR(60),
    ProdutoFilho CHAR(60),
    DescricaoFilho VARCHAR(120),
    QuantidadeBase DECIMAL(18,6),
    TipoProduto VARCHAR(20),
    TemEstrutura CHAR(1),
    NumeroHierarquia VARCHAR(100)
);

;WITH BomRecursiva AS (
    SELECT 
        0 AS Nivel, CAST(NULL AS CHAR(60)) AS ProdutoPai,
        p.PRO_Codigo AS ProdutoFilho, p.PRO_Descricao AS DescricaoFilho,
        CAST(1.000000 AS DECIMAL(18,6)) AS QuantidadeBase,
        CASE WHEN p.PRO_Codigo LIKE 'SA%' THEN 'SA' WHEN p.PRO_Codigo LIKE 'SE%' OR p.PRO_Codigo LIKE 'MP%' THEN 'MP' ELSE 'PA' END AS TipoProduto,
        CASE WHEN EXISTS (SELECT 1 FROM ENG_ESTRUTURA e WHERE e.ENG_EstruturaProduto = p.PRO_Codigo AND e.ENG_EstruturaAtiva = 'S') THEN 'S' ELSE 'N' END AS TemEstrutura,
        CAST('000' AS VARCHAR(100)) AS NumeroHierarquia
    FROM PRO_PRODUTO p WHERE RTRIM(p.PRO_Codigo) = RTRIM(@ProdutoCodigo)
    UNION ALL
    SELECT 
        r.Nivel + 1, r.ProdutoFilho, ei.ENG_EstruturaItemProduto, p.PRO_Descricao,
        CAST(r.QuantidadeBase * ei.ENG_EstruturaItemQuantidade AS DECIMAL(18,6)),
        CASE WHEN ei.ENG_EstruturaItemProduto LIKE 'SA%' THEN 'SA' WHEN ei.ENG_EstruturaItemProduto LIKE 'SE%' OR ei.ENG_EstruturaItemProduto LIKE 'MP%' THEN 'MP' ELSE 'PA' END,
        CASE WHEN EXISTS (SELECT 1 FROM ENG_ESTRUTURA e2 WHERE e2.ENG_EstruturaProduto = ei.ENG_EstruturaItemProduto AND e2.ENG_EstruturaAtiva = 'S') THEN 'S' ELSE 'N' END,
        CAST(r.NumeroHierarquia + '.' + RIGHT('000' + CAST(ei.ENG_EstruturaItemSeq AS VARCHAR), 3) AS VARCHAR(100))
    FROM BomRecursiva r
    INNER JOIN ENG_ESTRUTURA e ON RTRIM(e.ENG_EstruturaProduto) = RTRIM(r.ProdutoFilho) AND e.ENG_EstruturaAtiva = 'S' AND e.ENG_EstruturaTipo = 'P'
    INNER JOIN ENG_ESTRUTURAITEM ei ON ei.ENG_EstruturaProduto = e.ENG_EstruturaProduto AND ei.ENG_EstruturaNumero = e.ENG_EstruturaNumero
    INNER JOIN PRO_PRODUTO p ON p.PRO_Codigo = ei.ENG_EstruturaItemProduto
    WHERE r.Nivel < 6
)
INSERT INTO #BomExplodida SELECT Nivel, ProdutoPai, ProdutoFilho, DescricaoFilho, QuantidadeBase, TipoProduto, TemEstrutura, NumeroHierarquia FROM BomRecursiva ORDER BY NumeroHierarquia;

SELECT 
    RTRIM(ProdutoFilho) AS produto_codigo,
    DescricaoFilho AS produto_descricao,
    TipoProduto AS tipo_produto,
    TemEstrutura AS tem_estrutura,
    NumeroHierarquia AS numero_hierarquia,
    Nivel AS nivel,
    CASE WHEN ProdutoPai IS NULL THEN NULL ELSE RTRIM(ProdutoPai) END AS pai_codigo,
    QuantidadeBase AS quantidade_base
FROM #BomExplodida
ORDER BY NumeroHierarquia;

DROP TABLE #BomExplodida;
        """)
        
        result = db.execute(
            query,
            {
                "produto_codigo": request.produto_codigo,
                "data_inicio": request.data_inicio,
                "data_fim": request.data_fim,
                "filial_codigo": request.filial_codigo,
            }
        )
        
        # Parse BOM structure
        bom_rows = result.mappings().all()
        bom_structure = [BomItem(**dict(row)) for row in bom_rows]
        
        if not bom_structure:
            raise HTTPException(
                status_code=404,
                detail=f"Produto {request.produto_codigo} não encontrado ou sem estrutura"
            )
        
        # Executar queries para OPs e NFs (simplificado - retorna dados mockados para demo)
        # Em produção, isso seria parte da query completa acima
        ops_data: List[OpData] = []
        nfs_data: List[NfData] = []
        
        return CostMapResponse(
            bom_structure=bom_structure,
            ops_data=ops_data,
            nfs_data=nfs_data,
            summary={
                "produto_codigo": request.produto_codigo,
                "data_inicio": request.data_inicio.isoformat(),
                "data_fim": request.data_fim.isoformat(),
                "total_produtos": len(bom_structure),
                "produtos_pa": len([b for b in bom_structure if b.tipo_produto == "PA"]),
                "produtos_sa": len([b for b in bom_structure if b.tipo_produto == "SA"]),
                "produtos_mp": len([b for b in bom_structure if b.tipo_produto == "MP"]),
                "data_geracao": datetime.now().isoformat(),
            },
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar mapa de custos: {str(e)}")


@router.get("/estrutura/{produto_codigo}")
async def get_bom_structure(
    produto_codigo: str,
    db: Session = Depends(get_source_db),
) -> Dict[str, Any]:
    """Retorna apenas a estrutura BOM em formato hierárquico pronto para frontend."""
    
    try:
        query = text("""
WITH RecursiveBom AS (
    SELECT 
        0 AS Nivel, CAST(NULL AS CHAR(60)) AS ProdutoPai,
        p.PRO_Codigo AS ProdutoFilho, p.PRO_Descricao AS DescricaoFilho,
        CAST(1.000000 AS DECIMAL(18,6)) AS QuantidadeBase,
        CASE WHEN p.PRO_Codigo LIKE 'SA%' THEN 'SA' WHEN p.PRO_Codigo LIKE 'SE%' OR p.PRO_Codigo LIKE 'MP%' THEN 'MP' ELSE 'PA' END AS TipoProduto,
        CAST('000' AS VARCHAR(100)) AS NumeroHierarquia
    FROM PRO_PRODUTO p WHERE RTRIM(p.PRO_Codigo) = RTRIM(:produto_codigo)
    UNION ALL
    SELECT 
        r.Nivel + 1, r.ProdutoFilho, ei.ENG_EstruturaItemProduto, p.PRO_Descricao,
        CAST(r.QuantidadeBase * ei.ENG_EstruturaItemQuantidade AS DECIMAL(18,6)),
        CASE WHEN ei.ENG_EstruturaItemProduto LIKE 'SA%' THEN 'SA' WHEN ei.ENG_EstruturaItemProduto LIKE 'SE%' OR ei.ENG_EstruturaItemProduto LIKE 'MP%' THEN 'MP' ELSE 'PA' END,
        CAST(r.NumeroHierarquia + '.' + RIGHT('000' + CAST(ei.ENG_EstruturaItemSeq AS VARCHAR), 3) AS VARCHAR(100))
    FROM RecursiveBom r
    INNER JOIN ENG_ESTRUTURA e ON RTRIM(e.ENG_EstruturaProduto) = RTRIM(r.ProdutoFilho) AND e.ENG_EstruturaAtiva = 'S' AND e.ENG_EstruturaTipo = 'P'
    INNER JOIN ENG_ESTRUTURAITEM ei ON ei.ENG_EstruturaProduto = e.ENG_EstruturaProduto AND ei.ENG_EstruturaNumero = e.ENG_EstruturaNumero
    INNER JOIN PRO_PRODUTO p ON p.PRO_Codigo = ei.ENG_EstruturaItemProduto
    WHERE r.Nivel < 6
)
SELECT 
    RTRIM(ProdutoFilho) AS cod,
    DescricaoFilho AS descricao,
    TipoProduto AS tipo,
    NumeroHierarquia AS hierarquia,
    Nivel AS nivel,
    CASE WHEN ProdutoPai IS NULL THEN NULL ELSE RTRIM(ProdutoPai) END AS pai,
    QuantidadeBase AS qtdBase
FROM RecursiveBom
ORDER BY NumeroHierarquia
        """)
        
        result = db.execute(query, {"produto_codigo": produto_codigo})
        rows = result.mappings().all()
        
        if not rows:
            raise HTTPException(status_code=404, detail=f"Produto {produto_codigo} não encontrado")
        
        rollup_map: Dict[str, Dict[str, Any]] = {}
        try:
            connection = db.connection().connection
            cursor = connection.cursor()
            cursor.execute(
                BOM_COST_ROLLUP_SQL,
                (
                    produto_codigo,
                    settings.bom_tipo_custo,
                    settings.bom_qtd_notas_media,
                    settings.bom_limite_variacao_preco,
                ),
            )

            rollup_rows: List[Dict[str, Any]] = []
            while True:
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    rollup_rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                if not cursor.nextset():
                    break
            cursor.close()

            for row in rollup_rows:
                hierarquia = str(row.get("numero_hierarquia") or "").strip()
                if hierarquia:
                    rollup_map[hierarquia] = row
        except Exception:
            logger.exception("Erro ao buscar custos do BOM para %s", produto_codigo)

        # Converter para JSON estruturado
        items_dict = {row["cod"].strip(): dict(row) for row in rows if row.get("cod")}
        
        # Encontrar raiz
        root = next(
            (item for item in items_dict.values() if not (item.get("pai") or "").strip()),
            None,
        )
        
        if not root:
            raise HTTPException(status_code=500, detail="Estrutura inválida: nenhuma raiz encontrada")
        
        def build_tree(cod: str) -> Dict[str, Any]:
            """Constrói árvore hierárquica recursivamente."""
            item = items_dict.get(cod.strip())
            if not item:
                return {}
            
            hierarquia = str(item.get("hierarquia") or "").strip()
            rollup = rollup_map.get(hierarquia, {})

            children = [
                build_tree(child_cod)
                for child_cod, child_item in items_dict.items()
                if child_item.get("pai") and child_item["pai"].strip() == cod.strip()
            ]
            
            return {
                "cod": cod.strip(),
                "desc": item.get("descricao", ""),
                "tipo": item.get("tipo", ""),
                "nivel": item.get("nivel", 0),
                "qtdBase": float(item.get("qtdBase", 0)),
                "custoUnitario": float(rollup.get("custo_unitario") or 0),
                "custoTotal": float(rollup.get("custo_total") or 0),
                "origemCusto": rollup.get("origem_custo"),
                "statusAlerta": rollup.get("status_alerta"),
                "ultimaNf": rollup.get("ultima_nf"),
                "dataUltimaCompra": (
                    rollup.get("data_ultima_compra").isoformat()
                    if rollup.get("data_ultima_compra")
                    else None
                ),
                "variacaoPreco": (
                    float(rollup.get("variacao_preco"))
                    if rollup.get("variacao_preco") is not None
                    else None
                ),
                "children": children,
            }
        
        tree = build_tree(root["cod"])
        
        return {
            "success": True,
            "produto_codigo": produto_codigo,
            "tree": tree,
            "total_items": len(items_dict),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro ao buscar estrutura BOM para %s", produto_codigo)
        raise HTTPException(status_code=500, detail=f"Erro ao buscar estrutura: {str(e)}")


@router.get("/health")
async def health_check():
    """Verifica se o serviço de mapa de custos está disponível."""
    return {"status": "ok", "message": "Serviço de Mapa de Custos está ativo"}
