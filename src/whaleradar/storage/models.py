"""Pydantic models for all domain entities persisted to HDFS."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class TradeClassification(str, Enum):
    RETAIL = "retail"
    MEDIUM = "medium"
    WHALE = "whale"


class WhaleIntent(str, Enum):
    PUMP = "pump"
    DUMP = "dump"
    NEUTRAL = "neutral"


class MarketCondition(str, Enum):
    BULL = "bull"
    BEAR = "bear"
    SIDEWAYS = "sideways"


# ── Raw trade from Binance ──────────────────────────────────
class TradeRecord(BaseModel):
    symbol: str
    price: float
    quantity: float
    quote_volume: float  # price * quantity (USD value)
    trade_time: datetime
    is_buyer_maker: bool
    classification: TradeClassification = TradeClassification.RETAIL


# ── On-chain whale movement ─────────────────────────────────
class WhaleEvent(BaseModel):
    tx_hash: str
    blockchain: str  # "bitcoin" | "ethereum"
    from_address: str
    to_address: str
    amount: float  # in native coin
    amount_usd: float
    is_exchange_deposit: bool = False
    exchange_name: str | None = None
    detected_at: datetime = Field(default_factory=datetime.utcnow)


# ── Wallet profile built by agents ──────────────────────────
class WalletProfile(BaseModel):
    address: str
    blockchain: str
    total_appearances: int = 0
    dump_count: int = 0
    pump_count: int = 0
    neutral_count: int = 0
    avg_amount: float = 0.0
    last_seen: datetime | None = None
    is_known: bool = False
    notes: str = ""

    @property
    def dump_ratio(self) -> float:
        if self.total_appearances == 0:
            return 0.0
        return self.dump_count / self.total_appearances


# ── ML prediction output ────────────────────────────────────
class MLPrediction(BaseModel):
    intent: WhaleIntent
    confidence: float  # 0.0 – 1.0
    features_used: dict = Field(default_factory=dict)
    model_version: str = "v1"


# ── Agent decision record ───────────────────────────────────
class AgentDecision(BaseModel):
    whale_event: WhaleEvent
    wallet_profile: WalletProfile
    ml_prediction: MLPrediction
    market_condition: MarketCondition
    reasoning_steps: list[str] = Field(default_factory=list)
    final_assessment: str = ""
    risk_level: str = "unknown"  # low / medium / high / critical
    decided_at: datetime = Field(default_factory=datetime.utcnow)


# ── Final signal sent to user ────────────────────────────────
class Signal(BaseModel):
    signal_id: str
    whale_event: WhaleEvent
    intent: WhaleIntent
    confidence: float
    risk_level: str
    summary: str  # human-readable alert text
    suggested_action: str
    timeframe_minutes: int = 120
    created_at: datetime = Field(default_factory=datetime.utcnow)
    outcome: str | None = None  # filled later by backtest
