from __future__ import annotations

from contextlib import contextmanager

from sqlmodel import Session, create_engine

from app.core.config import settings

_connect_args = {}
_kwargs = {"echo": False}

if settings.database_url.startswith("sqlite"):
    _connect_args["check_same_thread"] = False
else:
    _kwargs["pool_pre_ping"] = True

engine = create_engine(settings.database_url, connect_args=_connect_args, **_kwargs)


@contextmanager
def get_session() -> Session:
    with Session(engine) as session:
        yield session
