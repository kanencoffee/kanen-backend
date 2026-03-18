from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog
from sqlalchemy import delete
from sqlmodel import Session, select

from app.models import Part, PurchaseOrder, PurchaseOrderLine, RepairOrder, RepairPartUsage, RepairStatus, Vendor
from app.services.quickbooks_client import QuickBooksClient

logger = structlog.get_logger(__name__)


def _upsert_vendor(session: Session, payload: Dict[str, Any]) -> Vendor:
    external_id = payload.get("Id")
    stmt = select(Vendor).where(Vendor.external_id == external_id)
    vendor = session.exec(stmt).first()
    if not vendor:
        vendor = Vendor(external_id=external_id, source_system="quickbooks", name=payload.get("DisplayName", ""))
    vendor.name = payload.get("DisplayName", vendor.name)
    vendor.contact_email = payload.get("PrimaryEmailAddr", {}).get("Address")
    vendor.contact_phone = payload.get("PrimaryPhone", {}).get("FreeFormNumber")
    vendor.notes = payload.get("Notes")
    session.add(vendor)
    return vendor


def _upsert_part(session: Session, payload: Dict[str, Any]) -> Part:
    external_id = payload.get("Id")
    stmt = select(Part).where(Part.external_id == external_id)
    part = session.exec(stmt).first()
    if not part:
        part = Part(external_id=external_id, source_system="quickbooks", name=payload.get("Name", ""))
    part.name = payload.get("Name", part.name)
    part.sku = payload.get("Sku") or payload.get("Name")
    part.description = payload.get("Description")
    part.lead_time_days = payload.get("TrackQtyOnHand", False) and payload.get("ReorderPoint")

    vendor_ref = payload.get("VendorRef", {}).get("value")
    if vendor_ref:
        vendor_stmt = select(Vendor).where(Vendor.external_id == vendor_ref)
        vendor = session.exec(vendor_stmt).first()
        if vendor:
            part.preferred_vendor_id = vendor.id
    session.add(part)
    return part


def _paginate(response: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    return response.get("QueryResponse", {}).get(key, [])


def sync_vendors(session: Session, client: QuickBooksClient) -> int:
    logger.info("quickbooks.sync_vendors.start")
    start = 1
    total = 0
    while True:
        payload = client.fetch_vendors(start_position=start)
        vendors = _paginate(payload, "Vendor")
        if not vendors:
            break
        for raw in vendors:
            _upsert_vendor(session, raw)
        session.commit()
        total += len(vendors)
        start += len(vendors)
    logger.info("quickbooks.sync_vendors.complete", count=total)
    return total


def sync_items(session: Session, client: QuickBooksClient) -> int:
    logger.info("quickbooks.sync_items.start")
    start = 1
    total = 0
    while True:
        payload = client.fetch_items(start_position=start)
        items = _paginate(payload, "Item")
        if not items:
            break
        for raw in items:
            _upsert_part(session, raw)
        session.commit()
        total += len(items)
        start += len(items)
    logger.info("quickbooks.sync_items.complete", count=total)
    return total

def _parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None

def _upsert_repair(session: Session, payload: Dict[str, Any]) -> RepairOrder:
    external_id = payload.get("Id")
    stmt = select(RepairOrder).where(RepairOrder.external_id == external_id)
    repair = session.exec(stmt).first()
    if not repair:
        repair = RepairOrder(
            external_id=external_id,
            source_system="quickbooks",
            opened_at=_parse_date(payload.get("TxnDate")) or datetime.utcnow(),
        )
    repair.opened_at = _parse_date(payload.get("TxnDate")) or repair.opened_at
    repair.closed_at = _parse_date(payload.get("DueDate"))
    repair.status = RepairStatus.CLOSED if payload.get("Balance") in (0, 0.0) else RepairStatus.OPEN
    customer = payload.get("CustomerRef", {})
    repair.customer_name = customer.get("name")
    repair.failure_mode = payload.get("PrivateNote") or payload.get("CustomerMemo", {}).get("value")
    session.add(repair)
    session.flush()
    return repair

def _sync_repair_lines(session: Session, repair: RepairOrder, payload: Dict[str, Any]) -> None:
    session.exec(delete(RepairPartUsage).where(RepairPartUsage.repair_id == repair.id))
    for line in payload.get("Line", []):
        detail = line.get("SalesItemLineDetail")
        if not detail:
            continue
        item_ref = detail.get("ItemRef", {}).get("value")
        if not item_ref:
            continue
        part_stmt = select(Part).where(Part.external_id == item_ref)
        part = session.exec(part_stmt).first()
        if not part:
            logger.warning("quickbooks.sync_parts.missing_part", repair=repair.external_id, item_ref=item_ref)
            continue
        usage = RepairPartUsage(
            repair_id=repair.id,
            part_id=part.id,
            quantity=detail.get("Qty", 1),
            unit_sale_price=detail.get("UnitPrice"),
            source_line_id=line.get("Id"),
        )
        session.add(usage)

def sync_repairs(session: Session, client: QuickBooksClient, changed_since: Optional[str] = None) -> int:
    logger.info("quickbooks.sync_repairs.start", changed_since=changed_since)
    start = 1
    total = 0
    while True:
        payload = client.fetch_invoices(changed_since=changed_since, start_position=start)
        invoices = _paginate(payload, "Invoice")
        if not invoices:
            break
        for raw in invoices:
            repair = _upsert_repair(session, raw)
            _sync_repair_lines(session, repair, raw)
        session.commit()
        total += len(invoices)
        start += len(invoices)
    logger.info("quickbooks.sync_repairs.complete", count=total)
    return total

def _upsert_purchase_order(session: Session, payload: Dict[str, Any]) -> PurchaseOrder:
    external_id = payload.get("Id")
    stmt = select(PurchaseOrder).where(PurchaseOrder.external_id == external_id)
    po = session.exec(stmt).first()
    if not po:
        po = PurchaseOrder(
            external_id=external_id,
            vendor_id=None,
            source_system="quickbooks",
            ordered_at=_parse_date(payload.get("TxnDate")) or datetime.utcnow(),
        )
    po.ordered_at = _parse_date(payload.get("TxnDate")) or po.ordered_at
    po.expected_receipt = _parse_date(payload.get("DueDate"))
    po.status = payload.get("POStatus") or payload.get("PrivateNote")
    vendor_ref = payload.get("VendorRef", {}).get("value")
    if vendor_ref:
        vendor_stmt = select(Vendor).where(Vendor.external_id == vendor_ref)
        vendor = session.exec(vendor_stmt).first()
        if vendor:
            po.vendor_id = vendor.id
    session.add(po)
    session.flush()
    return po

def _sync_purchase_lines(session: Session, po: PurchaseOrder, payload: Dict[str, Any]) -> None:
    session.exec(delete(PurchaseOrderLine).where(PurchaseOrderLine.purchase_order_id == po.id))
    for line in payload.get("Line", []):
        detail = line.get("ItemBasedExpenseLineDetail") or line.get("ItemLineDetail")
        if not detail:
            continue
        item_ref = detail.get("ItemRef", {}).get("value")
        part_id = None
        if item_ref:
            part_stmt = select(Part).where(Part.external_id == item_ref)
            part = session.exec(part_stmt).first()
            if part:
                part_id = part.id
        pol = PurchaseOrderLine(
            purchase_order_id=po.id,
            part_id=part_id,
            description=line.get("Description"),
            quantity=detail.get("Qty", 1),
            unit_cost=(detail.get("UnitPrice") or detail.get("Cost")),
            source_line_id=line.get("Id"),
        )
        session.add(pol)

def sync_purchase_orders(session: Session, client: QuickBooksClient, changed_since: Optional[str] = None) -> int:
    logger.info("quickbooks.sync_purchase_orders.start", changed_since=changed_since)
    start = 1
    total = 0
    while True:
        payload = client.fetch_purchase_orders(changed_since=changed_since, start_position=start)
        purchase_orders = _paginate(payload, "PurchaseOrder")
        if not purchase_orders:
            break
        for raw in purchase_orders:
            po = _upsert_purchase_order(session, raw)
            _sync_purchase_lines(session, po, raw)
        session.commit()
        total += len(purchase_orders)
        start += len(purchase_orders)
    logger.info("quickbooks.sync_purchase_orders.complete", count=total)
    return total
