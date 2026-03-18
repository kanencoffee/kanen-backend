"""Customer sentiment analysis from email data."""
from __future__ import annotations

import json
import os
import sqlite3
import email as emaillib
from email import policy
from collections import Counter
from typing import Any, Dict, List

from fastapi import APIRouter

router = APIRouter(prefix="/v1/analytics", tags=["analytics"])

KANEN_DB = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "kanen.db")

VENDOR_INDICATORS = [
    "clivecoffee", "chriscoffee", "espressoparts", "lamarzocco", "lamacchina",
    "jura-parts", "mouser", "fedex", "ereplacementparts", "hydrangea", "shiprush",
    "shipstation", "dialpad", "judge.me", "rocket-espresso", "ordini@",
    "kanencoffee", "shopify", "squarespace", "acuity", "quickbooks", "intuit",
    "google", "noreply", "no-reply", "notification",
]

POSITIVE_SIGNALS = [
    "thank", "thanks", "appreciate", "great", "love", "amazing", "awesome",
    "excellent", "wonderful", "happy", "perfect", "can't wait", "excited",
]

NEGATIVE_SIGNALS = [
    "problem", "issue", "broken", "leak", "not working", "recurring", "wrong",
    "complaint", "disappointed", "frustrated", "cancel", "refund", "damage",
    "same problem", "still broken",
]

ORDER_SUBJECT_SKIP = [
    "order confirmed", "has shipped", "on its way", "delivered",
    "out for delivery", "payment completed", "subscription",
]


def _extract_body(raw_path: str) -> str | None:
    """Read an .eml file and extract the customer's reply text."""
    if not os.path.exists(raw_path):
        raw_path = raw_path.replace("../", "")
    if not os.path.exists(raw_path):
        return None
    try:
        with open(raw_path, "r", errors="replace") as f:
            raw = f.read()
        msg = emaillib.message_from_string(raw, policy=policy.default)
        body_part = msg.get_body(preferencelist=("plain", "html"))
        if not body_part:
            return None
        text = body_part.get_content()
        lines = text.split("\n")
        clean = []
        for line in lines:
            s = line.strip()
            if s.startswith(">"):
                break
            if "On " in s and "wrote:" in s:
                break
            if "From: Kanen" in s or "service@kanencoffee" in s:
                break
            if "Sent from my" in s:
                continue
            clean.append(line)
        return "\n".join(clean).strip() or None
    except Exception:
        return None


def _classify(body: str) -> str:
    lower = body.lower()
    pos = sum(1 for s in POSITIVE_SIGNALS if s in lower)
    neg = sum(1 for s in NEGATIVE_SIGNALS if s in lower)
    if neg > pos:
        return "negative"
    if pos > neg:
        return "positive"
    return "neutral"


def _topics(body: str, subject: str) -> List[str]:
    lower = body.lower()
    slower = subject.lower()
    topics: list[str] = []
    if any(x in lower or x in slower for x in ["pick up", "pickup", "pick-up", "schedule", "appointment", "drop off"]):
        topics.append("scheduling")
    if any(x in lower for x in ["leak", "broken", "problem", "issue", "recurring", "same problem", "not working"]):
        topics.append("repair_issue")
    if any(x in lower or x in slower for x in ["order", "stock", "purchase", "buy", "price", "batch", "serial"]):
        topics.append("product_inquiry")
    if any(x in lower for x in ["thank", "appreciate", "great work", "love"]):
        topics.append("gratitude")
    if any(x in lower or x in slower for x in ["invoice", "receipt", "payment", "po#", "deposit"]):
        topics.append("billing")
    if any(x in lower for x in ["warranty", "guarantee"]):
        topics.append("warranty")
    if any(x in lower for x in ["cancel", "refund", "return"]):
        topics.append("cancellation")
    if not topics:
        topics.append("general")
    return topics


@router.get("/customer-sentiment")
async def customer_sentiment(days: int = 60) -> Dict[str, Any]:
    """Analyse customer email sentiment over the last N days."""
    db_path = os.path.normpath(KANEN_DB)
    if not os.path.exists(db_path):
        return {"error": "Database not found", "path": db_path}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT id, subject, sender, received_at, raw_payload_path
        FROM gmail_messages
        WHERE received_at >= date('now', ? || ' days')
        ORDER BY received_at DESC
        """,
        (f"-{days}",),
    ).fetchall()
    conn.close()

    messages: list[dict] = []
    for r in rows:
        sender = r["sender"].lower()
        if any(v in sender for v in VENDOR_INDICATORS):
            continue
        subj = r["subject"].lower()
        if any(x in subj for x in ORDER_SUBJECT_SKIP):
            continue

        body = _extract_body(r["raw_payload_path"] or "")
        if not body or len(body) < 15:
            continue

        sentiment = _classify(body)
        topics = _topics(body, r["subject"])
        sender_name = r["sender"].split("<")[0].strip().strip('"')

        messages.append({
            "sender": sender_name,
            "date": r["received_at"][:10],
            "subject": r["subject"],
            "body": body[:400],
            "sentiment": sentiment,
            "topics": topics,
        })

    pos = [m for m in messages if m["sentiment"] == "positive"]
    neg = [m for m in messages if m["sentiment"] == "negative"]
    neu = [m for m in messages if m["sentiment"] == "neutral"]

    def _topic_summary(msgs):
        tc = Counter()
        for m in msgs:
            for t in m["topics"]:
                tc[t] += 1
        return dict(tc.most_common(5))

    def _examples(msgs, n=5):
        return [
            {"sender": m["sender"], "date": m["date"], "excerpt": m["body"][:150]}
            for m in msgs[:n]
        ]

    total = len(messages) or 1
    return {
        "period": f"last_{days}_days",
        "total": len(messages),
        "positive": {
            "count": len(pos),
            "pct": round(len(pos) / total * 100, 1),
            "topics": _topic_summary(pos),
            "examples": _examples(pos),
        },
        "neutral": {
            "count": len(neu),
            "pct": round(len(neu) / total * 100, 1),
            "topics": _topic_summary(neu),
            "examples": _examples(neu),
        },
        "negative": {
            "count": len(neg),
            "pct": round(len(neg) / total * 100, 1),
            "topics": _topic_summary(neg),
            "examples": _examples(neg),
        },
    }
