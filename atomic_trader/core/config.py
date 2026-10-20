from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # API Keys
    binance_api_key: str = ""
    binance_api_secret: str = ""

    # TimescaleDB Config
    timescale_dsn: str = "postgresql://postgres:postgres@localhost:5433/trading"

    # ClickHouse Config
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8124
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_db: str = "trading"

    # Redis Config
    redis_url: str = "redis://localhost:6380/0"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()
