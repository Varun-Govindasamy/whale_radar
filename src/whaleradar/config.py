"""Centralized configuration loaded from environment variables."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings — loaded from .env or environment."""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    # ── OpenAI ──────────────────────────────────────────────
    openai_api_key: str

    # ── Telegram ────────────────────────────────────────────
    telegram_bot_token: str
    telegram_chat_id: str

    # ── Blockchair ──────────────────────────────────────────
    blockchair_api_key: str

    # ── Kafka ───────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9094"

    # ── HDFS ────────────────────────────────────────────────
    hdfs_url: str = "http://localhost:9870"
    hdfs_namenode: str = "hdfs://localhost:9000"

    # ── Spark ───────────────────────────────────────────────
    spark_master: str = "spark://localhost:7077"

    # ── Binance ─────────────────────────────────────────────
    binance_symbols: str = "btcusdt,ethusdt"

    # ── Thresholds ──────────────────────────────────────────
    whale_threshold_usd: float = 500_000.0
    medium_threshold_usd: float = 10_000.0
    onchain_min_btc: float = 100.0
    onchain_min_eth: float = 1_000.0

    @property
    def symbols_list(self) -> list[str]:
        return [s.strip().lower() for s in self.binance_symbols.split(",")]


settings = Settings()  # type: ignore[call-arg]
