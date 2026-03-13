from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging

from app.config import settings
from app.database import get_app_db, get_source_db
from app.services.sync_service import SyncService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sync", tags=["sync"])


@router.post("/run")
async def run_sync(
    data_fechamento: str = Query(default=settings.default_data_fechamento),
    data_inicio_nf: str = Query(default=settings.default_data_inicio_nf),
    product_codes: str | None = Query(default=None),
    datasets: str | None = Query(default=None),
    source_db: Session = Depends(get_source_db),
    app_db: Session = Depends(get_app_db),
):
    """Executa uma sincronizacao manual SQL Server -> PostgreSQL."""
    service = SyncService(source_db=source_db, app_db=app_db)
    codes: list[str] | None = None
    if product_codes:
        codes = [code.strip() for code in product_codes.split(",") if code.strip()]
    dataset_list: list[str] | None = None
    if datasets:
        dataset_list = [value.strip() for value in datasets.split(",") if value.strip()]

    try:
        # Tenta sincronizar
        result = service.run_sync(
            data_fechamento=data_fechamento,
            data_inicio_nf=data_inicio_nf,
            product_codes=codes,
            datasets=dataset_list,
        )
        return result
    except Exception as exc:
        error_msg = str(exc)
        logger.error(f"Erro na sincronização: {error_msg}")
        
        # Tentar retornar uma resposta mais informativa
        if "pyodbc" in error_msg.lower() or "connection" in error_msg.lower():
            raise HTTPException(
                status_code=500,
                detail="Não foi possível conectar ao SQL Server. Verifique as credenciais e disponibilidade do servidor."
            )
        elif "table" in error_msg.lower():
            raise HTTPException(
                status_code=500,
                detail=f"Tabela não encontrada no SQL Server. Detalhes: {error_msg}"
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Erro ao executar sincronização: {error_msg}"
            )


@router.get("/runs")
async def get_sync_runs(
    limit: int = Query(default=20, ge=1, le=200),
    source_db: Session = Depends(get_source_db),
    app_db: Session = Depends(get_app_db),
):
    """Lista historico de execucoes de sincronizacao."""
    service = SyncService(source_db=source_db, app_db=app_db)

    try:
        return {"runs": service.get_last_runs(limit=limit)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Falha ao consultar historico: {exc}")


@router.get("/products-status")
async def get_products_status(
    limit: int = Query(default=200, ge=1, le=2000),
    source_db: Session = Depends(get_source_db),
    app_db: Session = Depends(get_app_db),
):
    """Retorna ultima atualizacao por produto no PostgreSQL."""
    service = SyncService(source_db=source_db, app_db=app_db)

    try:
        return {"products": service.get_product_status(limit=limit)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Falha ao consultar status por produto: {exc}")


@router.get("/health")
async def sync_health():
    return {
        "status": "ok",
        "sync_enabled": settings.sync_enabled,
        "sync_interval_minutes": settings.sync_interval_minutes,
    }
