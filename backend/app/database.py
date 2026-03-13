from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from app.config import settings

# SQL Server: fonte de dados (ERP)
sqlserver_engine = create_engine(
    settings.sqlserver_url,
    echo=settings.environment == "development",
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

# PostgreSQL: cache/aplicacao
postgres_engine = create_engine(
    settings.postgres_url,
    echo=settings.environment == "development",
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

SqlServerSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sqlserver_engine)
PostgresSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=postgres_engine)


def get_source_db() -> Session:
    """Dependency para obter sessao do SQL Server (origem ERP)."""
    db = SqlServerSessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_app_db() -> Session:
    """Dependency para obter sessao do PostgreSQL (cache da app)."""
    db = PostgresSessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db() -> Session:
    """Compatibilidade retroativa: retorna o banco da aplicacao (PostgreSQL)."""
    yield from get_app_db()
