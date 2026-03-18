from __future__ import annotations

from datetime import datetime
from typing import List

import structlog
from sqlmodel import Session

from app.db.session import engine
from app.models import SyncRun, SyncStatus
from app.services.quickbooks_client import QuickBooksClient
from app.workers.gmail_ingest import GmailIngestor
from app.workers.quickbooks_sync import (
    sync_items,
    sync_purchase_orders,
    sync_repairs,
    sync_vendors,
)

logger = structlog.get_logger(__name__)


def start_run(job_name: str) -> int:
    logger.info("sync_runner.run.start", job=job_name)
    with Session(engine) as session:
        run = SyncRun(job_name=job_name, status=SyncStatus.PENDING, started_at=datetime.utcnow())
        session.add(run)
        session.commit()
        session.refresh(run)
        return run.id


def finish_run(run_id: int, status: SyncStatus, records: int | None = None, detail: str | None = None) -> None:
    logger.info("sync_runner.run.finish", run_id=run_id, status=status.value, records=records)
    with Session(engine) as session:
        run = session.get(SyncRun, run_id)
        if not run:
            logger.warning("sync_runner.run.missing", run_id=run_id)
            return
        run.status = status
        run.finished_at = datetime.utcnow()
        run.records_processed = records
        run.detail = detail
        session.add(run)
        session.commit()


def run_quickbooks_sync(changed_since: str | None = None) -> None:
    run_id = start_run("quickbooks")
    processed = 0
    client = QuickBooksClient()
    try:
        with Session(engine) as session:
            processed += sync_vendors(session, client)
            processed += sync_items(session, client)
            processed += sync_repairs(session, client, changed_since=changed_since)
            processed += sync_purchase_orders(session, client, changed_since=changed_since)
        finish_run(run_id, SyncStatus.SUCCESS, processed)
    except Exception as exc:  # pragma: no cover - logging side effect
        logger.exception("sync_runner.quickbooks.failed", run_id=run_id)
        finish_run(run_id, SyncStatus.FAILED, processed, detail=str(exc))
        raise


def run_gmail_sync(accounts: List[str], query: str, pages: int) -> None:
    if not accounts:
        raise ValueError("At least one Gmail account is required for ingestion")
    unique_accounts = list(dict.fromkeys(account.strip() for account in accounts if account.strip()))
    run_id = start_run("gmail")
    processed = 0
    ingestor = GmailIngestor()
    try:
        with Session(engine) as session:
            for account in unique_accounts:
                processed += ingestor.ingest_account(session, account, query=query, max_pages=pages)
        finish_run(run_id, SyncStatus.SUCCESS, processed)
    except Exception as exc:  # pragma: no cover - logging side effect
        logger.exception("sync_runner.gmail.failed", run_id=run_id)
        finish_run(run_id, SyncStatus.FAILED, processed, detail=str(exc))
        raise


__all__ = [
    "start_run",
    "finish_run",
    "run_quickbooks_sync",
    "run_gmail_sync",
]
