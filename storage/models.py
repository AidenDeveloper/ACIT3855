from datetime import datetime, UTC

from sqlalchemy import BigInteger, DateTime, Integer, String
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.orm import DeclarativeBase, mapped_column

# MySQL's plain DATETIME only keeps whole seconds. We need microseconds (fsp=6).
DT6 = DateTime().with_variant(DATETIME(fsp=6), "mysql")


def utcnow():
    """Naive UTC datetime. Everything in the database is UTC."""
    return datetime.now(UTC).replace(tzinfo=None)


def parse_ts(value):
    """'2026-09-07T20:12:33.001Z' -> naive UTC datetime."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def to_iso(dt):
    """Always 6 decimals + Z, so the strings also sort correctly as text."""
    return dt.isoformat(timespec="microseconds") + "Z"


class Base(DeclarativeBase):
    pass


class CombatEncounter(Base):
    __tablename__ = "combat_encounter"

    id = mapped_column(Integer, primary_key=True)
    player_id = mapped_column(String(36), nullable=False)
    character_name = mapped_column(String(50), nullable=False)
    monster_type = mapped_column(String(50), nullable=False)
    damage_dealt = mapped_column(Integer, nullable=False)
    timestamp = mapped_column(DT6, nullable=False)
    recorded_timestamp = mapped_column(DT6, default=utcnow, index=True)
    # unique: Kafka delivers "at least once", so the same event can arrive twice.
    # The unique key lets the worker detect and ignore duplicates.
    trace_id = mapped_column(BigInteger, nullable=False, unique=True)

    def to_json(self):
        return {
            "player_id": self.player_id,
            "character_name": self.character_name,
            "monster_type": self.monster_type,
            "damage_dealt": self.damage_dealt,
            "timestamp": to_iso(self.timestamp),
            "trace_id": self.trace_id,
            "recorded_timestamp": to_iso(self.recorded_timestamp),
        }


class GoldTransaction(Base):
    __tablename__ = "gold_transaction"

    id = mapped_column(Integer, primary_key=True)
    player_id = mapped_column(String(36), nullable=False)
    character_name = mapped_column(String(50), nullable=False)
    transaction_type = mapped_column(String(20), nullable=False)
    gold_amount = mapped_column(Integer, nullable=False)
    timestamp = mapped_column(DT6, nullable=False)
    recorded_timestamp = mapped_column(DT6, default=utcnow, index=True)
    trace_id = mapped_column(BigInteger, nullable=False, unique=True)

    def to_json(self):
        return {
            "player_id": self.player_id,
            "character_name": self.character_name,
            "transaction_type": self.transaction_type,
            "gold_amount": self.gold_amount,
            "timestamp": to_iso(self.timestamp),
            "trace_id": self.trace_id,
            "recorded_timestamp": to_iso(self.recorded_timestamp),
        }
