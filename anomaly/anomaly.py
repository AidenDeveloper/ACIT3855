"""Anomaly service: Kafka (events) -> thresholds -> Kafka (dead letter queue). No other storage."""
import json
import logging
import logging.config
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager

import connexion
import yaml
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

with open("log_conf.yml", "r") as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))

logger = logging.getLogger("anomaly")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
EVENTS_TOPIC = os.getenv("KAFKA_TOPIC", "events")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "anomalies")
GROUP_ID = os.getenv("ANOMALY_GROUP_ID", "anomaly_group")  # NOT the storage worker's group
SCAN_TIMEOUT = float(os.getenv("SCAN_TIMEOUT_SECONDS", "15"))


def _num(name):
    value = os.getenv(name)
    return float(value) if value not in (None, "") else None


# event type -> (numeric field, below threshold, above threshold). Configure via environment.
RULES = {
    "combat_encounter": ("damage_dealt", _num("COMBAT_DAMAGE_BELOW"), _num("COMBAT_DAMAGE_ABOVE")),
    "gold_transaction": ("gold_amount", _num("GOLD_BELOW"), _num("GOLD_ABOVE")),
}


def check(event, field, below, above):
    """Returns a human-readable reason if the event is anomalous, else None."""
    value = event.get(field)
    if not isinstance(value, (int, float)):
        return None
    if below is not None and value < below:
        return f"{field}={value} is below {below}"
    if above is not None and value > above:
        return f"{field}={value} is above {above}"
    return None


def detect(event):
    rule = RULES.get(event.get("type"))
    return check(event, *rule) if rule else None


# ---------------------------------------------------------------- background worker
def detect_loop(stop):
    """Consumes the events topic with its own consumer group and publishes anomalies to the DLQ."""
    delay = 1
    while not stop.is_set():
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": GROUP_ID,  # own group: gets every event, independent from the storage worker
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "allow.auto.create.topics": True,
        })
        producer = Producer({"bootstrap.servers": KAFKA_BROKER, "enable.idempotence": True})
        try:
            consumer.subscribe([EVENTS_TOPIC])
            logger.info("Anomaly detector started (group '%s')", GROUP_ID)
            delay = 1
            while not stop.is_set():
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().fatal():
                        raise KafkaException(msg.error())
                    logger.debug("Kafka error: %s", msg.error())
                    continue
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    consumer.commit(msg, asynchronous=False)
                    continue

                reason = detect(event)
                if reason:
                    out = {**event, "anomaly_reason": reason, "source_offset": msg.offset()}
                    producer.produce(DLQ_TOPIC, json.dumps(out).encode("utf-8"))
                    if producer.flush(10) > 0:  # not delivered: do NOT commit, reconnect and retry
                        raise RuntimeError("could not deliver anomaly to the dead letter queue")
                    logger.info("Anomaly published to '%s': %s | trace_id: %s", DLQ_TOPIC, reason, event.get("trace_id"))
                consumer.commit(msg, asynchronous=False)
        except (KafkaException, RuntimeError) as exc:
            logger.error("Kafka problem (%s), retrying in %ss", exc, delay)
            stop.wait(delay)
            delay = min(delay * 2, 30)
        finally:
            consumer.close()
    logger.info("Anomaly detector stopped")


@asynccontextmanager
async def lifespan(app):
    stop = threading.Event()
    thread = threading.Thread(target=detect_loop, args=(stop,), daemon=True, name="anomaly-detector")
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=10)


# ---------------------------------------------------------------- reading a topic
def read_topic(topic):
    """Yields every message currently in `topic`, from the beginning.

    Uses a throw-away consumer group and never commits, so it does not disturb any
    other consumer's offsets."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": f"anomaly-scan-{uuid.uuid4()}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    try:
        metadata = consumer.list_topics(topic, timeout=5)
        info = metadata.topics.get(topic)
        if info is None or info.error is not None:
            return
        last = {}
        assignments = []
        for partition in info.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=5)
            if high > low:
                last[partition] = high - 1
                assignments.append(TopicPartition(topic, partition, low))
        if not assignments:
            return
        consumer.assign(assignments)
        pending = set(last)
        deadline = time.monotonic() + SCAN_TIMEOUT
        while pending and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            if msg.offset() >= last[msg.partition()]:
                pending.discard(msg.partition())
            try:
                yield json.loads(msg.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue
    finally:
        consumer.close()


def kafka_down(exc):
    logger.error("Kafka unavailable (%s), returning 503", exc)
    return {"error": "kafka unavailable"}, 503


# ---------------------------------------------------------------- API
def get_status():
    return {"status": "ok"}, 200


def get_anomalies():
    try:
        seen = {}
        for event in read_topic(DLQ_TOPIC):
            seen[event.get("trace_id")] = event  # de-duplicate: delivery is at-least-once
    except KafkaException as exc:
        return kafka_down(exc)
    anomalies = list(seen.values())
    return {"count": len(anomalies), "anomalies": anomalies}, 200


def scan(type, below=None, above=None, limit=1000):
    field = RULES[type][0]
    matches = []
    try:
        for event in read_topic(EVENTS_TOPIC):
            if event.get("type") != type:
                continue
            if check(event, field, below, above):
                matches.append(event)
                if len(matches) >= limit:
                    break
    except KafkaException as exc:
        return kafka_down(exc)
    return {"type": type, "field": field, "count": len(matches), "events": matches}, 200


app = connexion.AsyncApp(__name__, specification_dir="", lifespan=lifespan)
app.add_api("anomaly.yaml", strict_validation=True, validate_responses=True)
