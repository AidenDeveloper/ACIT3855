"""Storage worker: Kafka -> MySQL. Fully synchronous, no HTTP."""
import json
import logging
import logging.config
import os
import signal
import sys
import time
from pathlib import Path

import yaml
from confluent_kafka import Consumer, KafkaError, KafkaException
from sqlalchemy.exc import IntegrityError, InterfaceError, OperationalError, ProgrammingError

with open("log_conf.yml", "r") as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))

from db import init_db, make_session  # noqa: E402  (after logging is configured)
from models import CombatEncounter, GoldTransaction, parse_ts  # noqa: E402

logger = logging.getLogger("worker")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "storage_worker_group")
HEARTBEAT_FILE = Path(os.getenv("HEARTBEAT_FILE", "/tmp/heartbeat"))


def heartbeat():
    """The Docker healthcheck looks at this file's modification time."""
    try:
        HEARTBEAT_FILE.touch()
    except OSError:
        pass


def handle_sigterm(signum, frame):
    # "docker compose stop" sends SIGTERM. Turn it into the Ctrl-C exception.
    raise KeyboardInterrupt


signal.signal(signal.SIGTERM, handle_sigterm)


# ---------------------------------------------------------------- database logic
def build_row(value):
    event_type = value["type"]
    common = dict(
        player_id=value["player_id"],
        character_name=value["character_name"],
        timestamp=parse_ts(value["timestamp"]),
        trace_id=value["trace_id"],
    )
    if event_type == "combat_encounter":
        return CombatEncounter(
            monster_type=value["monster_type"], damage_dealt=value["damage_dealt"], **common
        )
    if event_type == "gold_transaction":
        return GoldTransaction(
            transaction_type=value["transaction_type"], gold_amount=value["gold_amount"], **common
        )
    raise ValueError(f"unknown event type {event_type!r}")


def _errno(exc):
    args = getattr(getattr(exc, "orig", None), "args", ())
    return args[0] if args and isinstance(args[0], int) else None


def is_duplicate(exc):
    return _errno(exc) == 1062 or "UNIQUE constraint failed" in str(exc.orig)


def is_transient(exc):
    """Problems that go away on their own: database down, or table briefly missing (/reset)."""
    if isinstance(exc, (OperationalError, InterfaceError)):
        return True
    if isinstance(exc, ProgrammingError):
        return _errno(exc) == 1146 or "no such table" in str(exc.orig)
    return False


def save_event(value):
    """Returns 'stored', 'duplicate' or 'invalid'. Raises on transient database errors."""
    try:
        row = build_row(value)
    except (KeyError, ValueError, TypeError) as exc:
        logger.error("Invalid event, skipping: %r", exc)
        return "invalid"

    session = make_session()
    try:
        session.add(row)
        session.commit()
        return "stored"
    except IntegrityError as exc:
        session.rollback()
        if is_duplicate(exc):
            logger.info("Duplicate trace_id %s, already stored", value.get("trace_id"))
            return "duplicate"
        logger.error("Integrity error, skipping message: %s", exc.orig)
        return "invalid"
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def save_with_retry(value):
    """Never gives up on a transient error: the Kafka offset is NOT committed until this returns."""
    delay = 1
    while True:
        try:
            return save_event(value)
        except Exception as exc:  # KeyboardInterrupt is not an Exception, so shutdown still works
            if not is_transient(exc):
                logger.error("Permanent failure, skipping message: %r", exc)
                return "invalid"
            logger.warning(
                "Database unavailable (%s), retrying in %ss (Kafka offset not committed)",
                type(exc).__name__, delay,
            )
            heartbeat()
            time.sleep(delay)
            delay = min(delay * 2, 30)


# ---------------------------------------------------------------- Kafka loop
def commit(consumer, msg):
    try:
        consumer.commit(msg, asynchronous=False)
    except KafkaException as exc:
        logger.warning("Offset commit failed (message may be re-delivered, duplicates are ignored): %s", exc)


def main():
    init_db()  # waits for MySQL

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # we commit ourselves, only after the row is saved
        "allow.auto.create.topics": True,
    })
    consumer.subscribe([KAFKA_TOPIC])
    logger.info("Worker started: topic '%s' on %s, group '%s'", KAFKA_TOPIC, KAFKA_BROKER, KAFKA_GROUP_ID)

    try:
        while True:
            heartbeat()
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                err = msg.error()
                if err.fatal():
                    raise KafkaException(err)
                if err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.debug("Topic '%s' does not exist yet", KAFKA_TOPIC)
                elif err.code() != KafkaError._PARTITION_EOF:
                    logger.warning("Kafka error: %s", err)
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                logger.error("Malformed message at offset %d, skipping", msg.offset())
                commit(consumer, msg)
                continue

            result = save_with_retry(value)
            if result == "stored":
                logger.debug(
                    "Stored event '%s' with a trace id of %s", value.get("type"), value.get("trace_id")
                )
            commit(consumer, msg)
    except KafkaException as exc:
        logger.error("Fatal Kafka error: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        logger.info("Closing Kafka connection...")
        consumer.close()


if __name__ == "__main__":
    main()
