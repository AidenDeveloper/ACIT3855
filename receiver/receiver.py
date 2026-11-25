import asyncio
import json
import logging
import logging.config
import os
import time
from contextlib import asynccontextmanager

import connexion
import yaml
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from connexion import NoContent, request

with open("log_conf.yml", "r") as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))

logger = logging.getLogger("receiver")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")  # host:port, no "http://"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")  # one topic for both event types


async def start_producer():
    """Startup policy for ALL services: wait (with backoff) until the dependency is there."""
    delay = 1
    while True:
        producer = AIOKafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            enable_idempotence=True,  # producer id + sequence numbers prevent duplicates
            request_timeout_ms=10_000,  # fail after 10 s
            retry_backoff_ms=250,
            linger_ms=5,
        )
        try:
            await producer.start()
            logger.info("Connected to Kafka at %s", KAFKA_BROKER)
            return producer
        except (KafkaError, OSError) as exc:
            logger.warning("Kafka not reachable (%s), retrying in %ss", type(exc).__name__, delay)
            try:
                await producer.stop()
            except Exception:
                pass
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


@asynccontextmanager
async def lifespan(app):
    producer = await start_producer()
    try:
        # available in the endpoints as request.state.kafka / request.state.health
        yield {"kafka": producer, "health": {"kafka": "ok"}}
    finally:
        await producer.stop()
        logger.info("Kafka producer stopped")


async def publish(event_type, body):
    trace_id = time.time_ns()
    data = {**body, "trace_id": trace_id, "type": event_type}
    payload = json.dumps(data).encode("utf-8")
    logger.info("Received '%s' event | trace_id: %s", event_type, trace_id)

    producer = request.state.kafka
    health = request.state.health
    try:
        # send_and_wait = delivered to Kafka (not just queued); wait_for = upper bound of 5 s
        await asyncio.wait_for(producer.send_and_wait(KAFKA_TOPIC, payload), timeout=5)
    except (KafkaError, asyncio.TimeoutError) as exc:
        if health["kafka"] != "degraded":
            logger.error("Kafka unavailable (%s): receiver is now in degraded mode", type(exc).__name__)
        health["kafka"] = "degraded"
        logger.error("Rejected '%s' event | trace_id: %s", event_type, trace_id)
        return {"error": "temporarily unavailable"}, 503

    if health["kafka"] != "ok":
        logger.info("Kafka is back: receiver is operating normally again")
    health["kafka"] = "ok"
    logger.debug("Event '%s' sent to Kafka topic '%s' | trace_id: %s", event_type, KAFKA_TOPIC, trace_id)
    return NoContent, 201


async def report_combat_encounter(body):
    return await publish("combat_encounter", body)


async def report_gold_transaction(body):
    return await publish("gold_transaction", body)


async def get_status():
    return {"status": "ok", "kafka": request.state.health["kafka"]}, 200


app = connexion.AsyncApp(__name__, specification_dir="", lifespan=lifespan)
app.add_api("realm.yaml", strict_validation=True, validate_responses=True)
