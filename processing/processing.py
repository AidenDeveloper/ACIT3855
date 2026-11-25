import asyncio
import json
import logging
import logging.config
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, UTC
from pathlib import Path

import connexion
import httpx
import yaml
from connexion import request

with open("log_conf.yml", "r") as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))

logger = logging.getLogger("processing")

# --- configuration (set in docker-compose.yml) ---------------------------------
STORAGE_URL = os.getenv("STORAGE_URL", "http://localhost:8090")
STATS_PATH = Path(os.getenv("STATS_PATH", "statistics.json"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL_SECONDS", "5"))

# Used as "after" the very first time, so storage returns everything.
EPOCH = "1970-01-01T00:00:00.000000Z"


# --- statistics state ----------------------------------------------------------
def default_state():
    return {
        "num_combat_events": 0,
        "damage_sum": 0,  # kept so the average can be updated without re-reading old events
        "max_damage_dealt": None,
        "last_combat_recorded": EPOCH,
        "num_economy_events": 0,
        "gold_sum": 0,
        "max_gold_amount": None,
        "min_gold_amount": None,
        "last_economy_recorded": EPOCH,
        "last_updated": None,
    }


def load_state():
    state = default_state()
    if STATS_PATH.exists():
        try:
            state.update(json.loads(STATS_PATH.read_text()))
            logger.info("Loaded existing statistics from %s", STATS_PATH)
        except (OSError, ValueError):
            logger.exception("Could not read %s, starting from scratch", STATS_PATH)
    else:
        logger.info("No statistics file found, starting from scratch")
    return state


def save_state(state):
    """Atomic write: write a temp file, then swap it in. Readers never see a half-written file."""
    STATS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATS_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(state, f)
    tmp.replace(STATS_PATH)


def _bigger(current, values):
    return max(values) if current is None else max([current, *values])


def _smaller(current, values):
    return min(values) if current is None else min([current, *values])


def apply_combat(state, events):
    if not events:
        return
    damages = [e["damage_dealt"] for e in events]
    state["num_combat_events"] += len(events)
    state["damage_sum"] += sum(damages)
    state["max_damage_dealt"] = _bigger(state["max_damage_dealt"], damages)
    # storage always formats timestamps with 6 decimals + Z, so max() on strings is safe
    state["last_combat_recorded"] = max(e["recorded_timestamp"] for e in events)


def apply_economy(state, events):
    if not events:
        return
    golds = [e["gold_amount"] for e in events]
    state["num_economy_events"] += len(events)
    state["gold_sum"] += sum(golds)
    state["max_gold_amount"] = _bigger(state["max_gold_amount"], golds)
    state["min_gold_amount"] = _smaller(state["min_gold_amount"], golds)
    state["last_economy_recorded"] = max(e["recorded_timestamp"] for e in events)


def public_view(state):
    n_combat = state["num_combat_events"]
    n_econ = state["num_economy_events"]
    return {
        "num_combat_events": n_combat,
        "max_damage_dealt": state["max_damage_dealt"],
        "avg_damage_dealt": round(state["damage_sum"] / n_combat, 2) if n_combat else None,
        "num_economy_events": n_econ,
        "max_gold_amount": state["max_gold_amount"],
        "min_gold_amount": state["min_gold_amount"],
        "avg_gold_amount": round(state["gold_sum"] / n_econ, 2) if n_econ else None,
        "last_updated": state["last_updated"],
    }


# --- background processing -----------------------------------------------------
async def fetch_events(client, path, after):
    response = await client.get(f"{STORAGE_URL}{path}", params={"after": after})
    response.raise_for_status()
    return response.json()


async def run_cycle(client, state):
    started = time.monotonic()
    logger.info("Periodic processing started")

    try:
        combat = await fetch_events(client, "/combat/encounter", state["last_combat_recorded"])
        economy = await fetch_events(client, "/economy/transaction", state["last_economy_recorded"])
    except httpx.HTTPError as exc:
        logger.error("Storage service not reachable or request failed: %r", exc)
        return state

    logger.info(
        "Received %d combat and %d economy events from storage", len(combat), len(economy)
    )

    new_state = dict(state)
    apply_combat(new_state, combat)
    apply_economy(new_state, economy)

    if combat or economy:
        new_state["last_updated"] = datetime.now(UTC).isoformat(timespec="seconds")
        await asyncio.to_thread(save_state, new_state)  # file I/O is blocking, keep it off the event loop
        logger.debug("Updated statistics: %s", public_view(new_state))
        logger.info("Statistics updated")
    else:
        logger.info("No new events, statistics unchanged")

    logger.info("Processing finished in %.1f ms", (time.monotonic() - started) * 1000)
    return new_state


async def update_loop(holder):
    # One shared client: reuses the TCP connection to storage instead of reconnecting every time.
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            try:
                holder["stats"] = await run_cycle(client, holder["stats"])
            except Exception:
                logger.exception("Unexpected error during processing, will retry")
            await asyncio.sleep(POLL_INTERVAL)


@asynccontextmanager
async def lifespan(app):
    holder = {"stats": await asyncio.to_thread(load_state)}
    task = asyncio.create_task(update_loop(holder))
    logger.info("Background processing started (every %.1f s)", POLL_INTERVAL)
    try:
        # whatever is yielded here is available in the endpoints as request.state
        yield {"holder": holder}
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        logger.info("Background processing stopped")


# --- API endpoints ---------------------------------------------------------------
async def get_stats():
    return public_view(request.state.holder["stats"]), 200


async def get_status():
    return {"status": "ok"}, 200


app = connexion.AsyncApp(__name__, specification_dir="", lifespan=lifespan)
app.add_api("processing.yaml", strict_validation=True, validate_responses=True)
