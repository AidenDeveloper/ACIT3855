import logging
import logging.config
import os
import time

import connexion
import requests
import yaml
from connexion import NoContent

with open("log_conf.yml", "r") as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))

logger = logging.getLogger("receiver")

STORAGE_URL = os.getenv("STORAGE_URL", "http://localhost:8090")


def get_status():
    return {"status": "ok"}, 200


def report_combat_encounter(body):
    trace_id = time.time_ns()
    body["trace_id"] = trace_id
    logger.info(
        f"Received 'combat_encounter' event - damage: {body['damage_dealt']} | trace_id: {trace_id}"
    )

    r = requests.post(f"{STORAGE_URL}/combat/encounter", json=body, timeout=30)

    logger.debug(
        f"Response for event combat_encounter (id: {trace_id}) has status {r.status_code}"
    )
    return NoContent, r.status_code


def report_gold_transaction(body):
    trace_id = time.time_ns()
    body["trace_id"] = trace_id
    logger.info(
        f"Received 'gold_transaction' event - gold: {body['gold_amount']} | trace_id: {trace_id}"
    )

    r = requests.post(f"{STORAGE_URL}/economy/transaction", json=body, timeout=30)

    logger.debug(
        f"Response for event gold_transaction (id: {trace_id}) has status {r.status_code}"
    )
    return NoContent, r.status_code


app = connexion.AsyncApp(__name__, specification_dir="")
app.add_api("realm.yaml", strict_validation=True, validate_responses=True)
