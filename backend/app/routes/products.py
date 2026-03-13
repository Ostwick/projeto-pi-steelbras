from datetime import datetime, time
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List
from app.database import get_app_db, get_source_db
from app.services.sync_service import BOM_COST_ROLLUP_SQL
from app.config import settings
from app.schemas import (
    ProductComposition,
    ProductSummary,
    ProductSearchResponse,
    ActivityCost
)

router = APIRouter(prefix="/api/products", tags=["products"])


@router.get("/search", response_model=ProductSearchResponse)
async def search_products(
    query: str = Query(..., min_length=1, description="Nome ou código do produto"),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_source_db)
):
    """
    Busca produtos por nome ou código.
    Aqui você adiciona a sua query SQL existente.
    """
    like_query = f"%{query}%"
    rows = db.execute(
        text(
            """
            SELECT TOP (:limit)
                RTRIM(p.PRO_Codigo) AS product_id,
                p.PRO_Descricao AS product_name,
                RTRIM(p.PRO_Codigo) AS product_code
            FROM PRO_PRODUTO p
            WHERE RTRIM(p.PRO_Codigo) LIKE :query
               OR p.PRO_Descricao LIKE :query
            ORDER BY p.PRO_Codigo;
            """
        ),
        {"limit": limit, "query": like_query},
    ).mappings().all()

    now = datetime.utcnow()
    products = [
        ProductSummary(
            product_id=row["product_id"],
            product_name=row["product_name"],
            product_code=row["product_code"],
            current_cost=0.0,
            last_update=now,
        )
        for row in rows
    ]
    return {"products": products, "total": len(products)}


@router.get("/{product_id}/composition", response_model=ProductComposition)
async def get_product_composition(
    product_id: str,
    source_db: Session = Depends(get_source_db),
    app_db: Session = Depends(get_app_db),
):
    """
    Retorna a árvore de composição do produto com custos.
    Inclui custo médio e últimos custos de cada componente.
    """
    product_row = source_db.execute(
        text(
            """
            SELECT TOP 1 RTRIM(PRO_Codigo) AS product_code, PRO_Descricao AS product_name
            FROM PRO_PRODUTO
            WHERE RTRIM(PRO_Codigo) = RTRIM(:code);
            """
        ),
        {"code": product_id},
    ).mappings().first()
    if not product_row:
        raise HTTPException(status_code=404, detail="Produto não encontrado")

    connection = source_db.connection().connection
    cursor = connection.cursor()
    cursor.execute(
        BOM_COST_ROLLUP_SQL,
        (
            product_id,
            settings.bom_tipo_custo,
            settings.bom_qtd_notas_media,
            settings.bom_limite_variacao_preco,
        ),
    )

    rows = []
    while True:
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        if not cursor.nextset():
            break
    cursor.close()

    if not rows:
        raise HTTPException(status_code=404, detail="Sem dados de composicao para o produto")

    total_cost = sum((row.get("custo_total") or 0) for row in rows)

    components = []
    for row in rows:
        if row.get("numero_hierarquia") == "000":
            continue
        components.append(
            {
                "component_id": str(row.get("produto_codigo") or ""),
                "component_name": row.get("produto_descricao") or "",
                "quantity": float(row.get("quantidade_necessaria") or 0),
                "unit_cost": float(row.get("custo_unitario") or 0),
                "total_cost": float(row.get("custo_total") or 0),
                "average_cost": float(row.get("custo_unitario") or 0),
                "last_costs": [],
            }
        )

    return ProductComposition(
        product_id=product_row["product_code"],
        product_name=product_row["product_name"],
        product_code=product_row["product_code"],
        total_cost=float(total_cost or 0),
        average_cost=float(total_cost or 0),
        components=components,
    )


@router.get("/{product_id}/activities", response_model=List[ActivityCost])
async def get_product_activities(
    product_id: str,
    db: Session = Depends(get_app_db)
):
    """
    Retorna lista de atividades/processos relacionados ao produto
    com seus respectivos custos.
    """
    rows = db.execute(
        text(
            """
            SELECT op_numero, centro_custo, centro_codigo, quantidade,
                   mo_por_peca, custo_mo, data_op
            FROM cost_map_ops
            WHERE produto_codigo = :code
            ORDER BY data_op DESC, op_numero DESC
            LIMIT 200;
            """
        ),
        {"code": product_id},
    ).mappings().all()

    activities: List[ActivityCost] = []
    for row in rows:
        activities.append(
            ActivityCost(
                activity_id=int(row.get("op_numero") or 0),
                activity_name=row.get("centro_custo") or f"OP {row.get('op_numero')}",
                description=row.get("centro_codigo"),
                unit_cost=float(row.get("mo_por_peca") or 0),
                total_cost=float(row.get("custo_mo") or 0),
                quantity=float(row.get("quantidade") or 0),
                last_update=row.get("data_op") or datetime.utcnow(),
            )
        )

    return activities


@router.get("/{product_id}/summary", response_model=ProductSummary)
async def get_product_summary(
    product_id: str,
    source_db: Session = Depends(get_source_db),
    app_db: Session = Depends(get_app_db),
):
    """
    Retorna resumo do produto com custo atual.
    """
    product_row = source_db.execute(
        text(
            """
            SELECT TOP 1 RTRIM(PRO_Codigo) AS product_code, PRO_Descricao AS product_name
            FROM PRO_PRODUTO
            WHERE RTRIM(PRO_Codigo) = RTRIM(:code);
            """
        ),
        {"code": product_id},
    ).mappings().first()
    if not product_row:
        raise HTTPException(status_code=404, detail="Produto não encontrado")

    cost_row = app_db.execute(
        text(
            """
            SELECT custo_medio_real, data_fechamento
            FROM product_cost_snapshot
            WHERE product_code = :code
            ORDER BY data_fechamento DESC
            LIMIT 1;
            """
        ),
        {"code": product_id},
    ).mappings().first()

    current_cost = float(cost_row["custo_medio_real"]) if cost_row else 0.0
    last_update = (
        datetime.combine(cost_row["data_fechamento"], time.min)
        if cost_row and cost_row.get("data_fechamento")
        else datetime.utcnow()
    )

    return ProductSummary(
        product_id=product_row["product_code"],
        product_name=product_row["product_name"],
        product_code=product_row["product_code"],
        current_cost=current_cost,
        last_update=last_update,
    )
