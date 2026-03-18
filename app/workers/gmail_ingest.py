from __future__ import annotations


import base64
from email import message_from_bytes, policy
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import structlog
from sqlalchemy import delete
from sqlmodel import Session, select

from app.models import EmailReceiptLine, GmailMessage
from app.parsers.receipt_parser import parse_receipt_text
from app.services.gmail_client import GmailClient

logger = structlog.get_logger(__name__)


class GmailIngestor:
    def __init__(self, storage_dir: str = "../data/gmail") -> None:
        self.gmail = GmailClient()
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def _persist_raw(self, account: str, message_id: str, raw_data: Optional[str]) -> tuple[Optional[str], Optional[bytes]]:
        if not raw_data:
            return None, None
        data = base64.urlsafe_b64decode(raw_data.encode("utf-8"))
        account_dir = self.storage_dir / account
        account_dir.mkdir(parents=True, exist_ok=True)
        file_path = account_dir / f"{message_id}.eml"
        file_path.write_bytes(data)
        return str(file_path), data

    def _upsert_metadata(self, session: Session, account: str, metadata: dict, raw_path: Optional[str]) -> GmailMessage:
        msg_id = metadata["id"]
        stmt = select(GmailMessage).where(GmailMessage.message_id == msg_id)
        record = session.exec(stmt).first()
        if not record:
            record = GmailMessage(
                account_email=account,
                message_id=msg_id,
                thread_id=metadata.get("threadId", ""),
                received_at=_ts(metadata.get("internalDate")),
            )
        headers = {h["name"].lower(): h["value"] for h in metadata.get("payload", {}).get("headers", [])}
        record.subject = headers.get("subject")
        record.sender = headers.get("from")
        record.received_at = record.received_at or _ts(metadata.get("internalDate"))
        record.raw_payload_path = raw_path
        session.add(record)
        session.flush()
        return record

    def _store_receipt_lines(self, session: Session, message_record: GmailMessage, parsed_lines):
        session.exec(delete(EmailReceiptLine).where(EmailReceiptLine.gmail_message_id == message_record.id))
        for line in parsed_lines:
            entry = EmailReceiptLine(
                gmail_message_id=message_record.id,
                vendor_name=line.vendor_name or message_record.sender,
                part_hint=line.part_hint,
                quantity=line.quantity,
                unit_cost=line.unit_cost,
            )
            session.add(entry)

    def ingest_account(self, session: Session, account: str, query: Optional[str] = None, max_pages: int = 5) -> int:
        logger.info("gmail.ingest.start", account=account)
        page_token: Optional[str] = None
        pages = 0
        total = 0
        while True:
            resp = self.gmail.list_messages(account, query=query, page_token=page_token)
            messages = resp.get("messages", [])
            for msg in messages:
                full = self.gmail.get_message(account, msg["id"], fmt="full")
                raw = self.gmail.get_message(account, msg["id"], fmt="raw")
                raw_path, raw_bytes = self._persist_raw(account, msg["id"], raw.get("raw"))
                record = self._upsert_metadata(session, account, full, raw_path)
                if raw_bytes:
                    text_body = _extract_body_text(raw_bytes)
                    parsed_lines = parse_receipt_text(text_body, vendor=record.sender)
                    if parsed_lines:
                        self._store_receipt_lines(session, record, parsed_lines)
            session.commit()
            total += len(messages)
            page_token = resp.get("nextPageToken")
            pages += 1
            if not page_token or pages >= max_pages:
                break
        logger.info("gmail.ingest.complete", account=account, count=total)
        return total


def _ts(value) -> datetime:
    """Convert Gmail internalDate (ms since epoch) to datetime."""
    if value is None:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return datetime.now(timezone.utc)


def _extract_body_text(raw_bytes: bytes) -> str:
    message = message_from_bytes(raw_bytes, policy=policy.default)
    if message.is_multipart():
        for part in message.walk():
            if part.get_content_type() == "text/plain":
                return part.get_content()
        for part in message.walk():
            if part.get_content_type() == "text/html":
                return part.get_content()
    return message.get_content()
