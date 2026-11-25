import logging
import os
import time

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from models import Base

logger = logging.getLogger("db")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "mysql+pymysql://realm_user:change_me_please@127.0.0.1:3307/realm_events",
)

# pool_pre_ping: test a pooled connection before using it, so a restarted MySQL
# does not leave us with dead connections.
ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True)

_Session = sessionmaker(bind=ENGINE)


def make_session():
    return _Session()


def init_db():
    """Startup policy for ALL services: wait (with backoff) until the dependency is there."""
    delay = 1
    while True:
        try:
            Base.metadata.create_all(ENGINE)
            logger.info("Database is ready")
            return
        except SQLAlchemyError as exc:
            logger.warning("Database not ready (%s), retrying in %ss", type(exc).__name__, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)
