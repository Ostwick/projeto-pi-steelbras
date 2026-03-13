from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import settings
from app.database import PostgresSessionLocal, SqlServerSessionLocal
from app.routes import products, queries, sync, cost_map
from app.services.sync_service import run_sync_job

scheduler = BackgroundScheduler(timezone="UTC")


def _scheduled_sync_runner() -> None:
    """Wrapper do job agendado para abrir/fechar sessoes com seguranca."""
    source_db = SqlServerSessionLocal()
    app_db = PostgresSessionLocal()

    try:
        run_sync_job(source_db=source_db, app_db=app_db)
    finally:
        source_db.close()
        app_db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gerencia o ciclo de vida da aplicação FastAPI.
    
    Startup: Inicia o scheduler de sincronização
    Shutdown: Para o scheduler de forma segura
    """
    # STARTUP
    if settings.sync_enabled:
        scheduler.add_job(
            _scheduled_sync_runner,
            trigger=IntervalTrigger(minutes=settings.sync_interval_minutes),
            id="sqlserver_to_postgres_sync",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        scheduler.start()
        print(f"✓ Scheduler iniciado: sincroniza a cada {settings.sync_interval_minutes} minutos")
    else:
        print("⊘ Scheduler desabilitado (SYNC_ENABLED=false)")
    
    yield
    
    # SHUTDOWN
    if scheduler.running:
        scheduler.shutdown(wait=False)
        print("✓ Scheduler parado")


# Inicializar aplicação FastAPI com lifespan
app = FastAPI(
    title="API de Análise de Custos",
    description="API para análise de custos de produtos, componentes e atividades",
    version="1.0.0",
    lifespan=lifespan
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(products.router)
app.include_router(queries.router)
app.include_router(sync.router)
app.include_router(cost_map.router)


@app.get("/")
async def root():
    """Endpoint raiz da API"""
    return {
        "message": "Bem-vindo à API de Análise de Custos",
        "docs": "/docs",
        "redoc": "/redoc"
    }


@app.get("/health")
async def health_check():
    """Verificar saúde da API"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.environment == "development"
    )
