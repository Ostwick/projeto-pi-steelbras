from pathlib import Path
from urllib.parse import quote_plus
from datetime import date, timedelta
from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def _last_day_previous_month() -> str:
    today = date.today()
    first_day_this_month = date(today.year, today.month, 1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    return last_day_prev_month.isoformat()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(ENV_PATH), case_sensitive=False)
    # SQL Server (fonte de dados do ERP)
    sqlserver_host: Optional[str] = None
    sqlserver_database: Optional[str] = None
    sqlserver_user: Optional[str] = None
    sqlserver_password: Optional[str] = None
    sqlserver_port: int = 1433
    sqlserver_encrypt: str = "no"
    sqlserver_trust_server_certificate: str = "yes"

    # PostgreSQL (cache de dados tratados da aplicacao)
    postgres_host: str = "localhost"
    postgres_database: Optional[str] = None
    postgres_user: Optional[str] = None
    postgres_password: Optional[str] = None
    postgres_port: int = 5432
    postgres_sslmode: str = "prefer"

    # Compatibilidade com configuracao antiga (single DB)
    database_server: Optional[str] = None
    database_name: Optional[str] = None
    database_user: Optional[str] = None
    database_password: Optional[str] = None
    database_port: int = 1433
    
    # Application Configuration
    environment: str = "development"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    
    # CORS
    frontend_url: str = "http://localhost:5173"

    # Sync/ETL
    sync_enabled: bool = True
    sync_interval_minutes: int = 60
    default_data_fechamento: str = Field(default_factory=_last_day_previous_month)
    default_data_inicio_nf: str = "2026-01-01"
    cost_map_filial_codigo: int = 8637511000120
    bom_tipo_custo: str = "1"
    bom_qtd_notas_media: int = 5
    bom_limite_variacao_preco: float = 5.0
    sync_product_codes: str = "00020011,00020854,00069001,00056737,00089501,00020852"

    @property
    def sync_product_codes_list(self) -> List[str]:
        if not self.sync_product_codes:
            return []
        raw = self.sync_product_codes.replace("\n", ",")
        return [code.strip() for code in raw.split(",") if code.strip()]
    
    @property
    def sqlserver_url(self) -> str:
        """Gera a URL de conexao para SQL Server (origem ERP)."""
        host = self.sqlserver_host or self.database_server
        database = self.sqlserver_database or self.database_name
        user = self.sqlserver_user or self.database_user
        password = self.sqlserver_password or self.database_password
        port = self.sqlserver_port or self.database_port

        if not host or not database or not user or not password:
            raise ValueError(
                "Configuracao SQL Server incompleta. Defina sqlserver_* ou database_* no .env"
            )

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"Encrypt={self.sqlserver_encrypt};"
            f"TrustServerCertificate={self.sqlserver_trust_server_certificate}"
        )
        return f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"

    @property
    def postgres_url(self) -> str:
        """Gera a URL de conexao para PostgreSQL (cache da aplicacao)."""
        if not self.postgres_database or not self.postgres_user or not self.postgres_password:
            raise ValueError(
                "Configuracao PostgreSQL incompleta. Defina postgres_database, postgres_user e postgres_password no .env"
            )

        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
            f"?sslmode={self.postgres_sslmode}"
        )


settings = Settings()
