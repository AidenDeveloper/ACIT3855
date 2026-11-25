"""Storage API: read-only access to the events (the worker does the writing)."""
import logging
import logging.config
import os
from datetime import timedelta

import connexion
import yaml
from connexion import NoContent
from sqlalchemy import func, select
from sqlalchemy.exc import InterfaceError, OperationalError

with open("log_conf.yml", "r") as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))

from db import ENGINE, init_db, make_session  # noqa: E402
from models import Base, CombatEncounter, GoldTransaction, parse_ts, utcnow  # noqa: E402

logger = logging.getLogger("storage")

# Rows younger than this are not returned yet (see Lab 7: avoids skipping rows that are
# inserted but not committed when the processing service moves its "after" marker).
SETTLE_SECONDS = float(os.getenv("SETTLE_SECONDS", "1"))

DB_ERRORS = (OperationalError, InterfaceError)


def db_unavailable(exc):
    logger.error("Database unavailable (%s), returning 503", type(exc).__name__)
    return {"error": "database unavailable"}, 503


def _get_events(model, name, after):
    cutoff = utcnow() - timedelta(seconds=SETTLE_SECONDS)
    query = select(model).where(model.recorded_timestamp <= cutoff)
    if after:
        query = query.where(model.recorded_timestamp > parse_ts(after))
    query = query.order_by(model.recorded_timestamp)

    session = make_session()
    try:
        results = [row.to_json() for row in session.scalars(query).all()]
    except DB_ERRORS as exc:
        return db_unavailable(exc)
    finally:
        session.close()

    logger.debug("Found %d %s events after %s", len(results), name, after)
    return results, 200


def get_combat_encounters(after=None):
    return _get_events(CombatEncounter, "combat_encounter", after)


def get_gold_transactions(after=None):
    return _get_events(GoldTransaction, "gold_transaction", after)


def get_status():
    session = make_session()
    try:
        combat = session.scalar(select(func.count()).select_from(CombatEncounter))
        economy = session.scalar(select(func.count()).select_from(GoldTransaction))
    except DB_ERRORS as exc:
        return db_unavailable(exc)
    finally:
        session.close()
    return {"status": "ok", "combat_count": combat, "economy_count": economy}, 200


def reset_db():
    try:
        Base.metadata.drop_all(ENGINE)
        Base.metadata.create_all(ENGINE)
    except DB_ERRORS as exc:
        return db_unavailable(exc)
    logger.info("Database tables dropped and recreated")
    return NoContent, 200


init_db()  # waits for MySQL (same startup policy as every other service)

app = connexion.AsyncApp(__name__, specification_dir="", )
app.add_api("storage.yaml", strict_validation=True, validate_responses=True)
