from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from sqlmodel import Field, SQLModel


class Timestamped(SQLModel):
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    updated_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)


class Vendor(SQLModel, table=True):
    __tablename__ = "vendors"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    source_system: str = Field(default="quickbooks")
    external_id: Optional[str] = Field(default=None, index=True)
    contact_email: Optional[str] = Field(default=None)
    contact_phone: Optional[str] = Field(default=None)
    notes: Optional[str] = Field(default=None)


class Part(SQLModel, table=True):
    __tablename__ = "parts"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    source_system: str = Field(default="quickbooks")
    external_id: Optional[str] = Field(default=None, index=True)
    sku: Optional[str] = Field(default=None, index=True)
    description: Optional[str] = None
    preferred_vendor_id: Optional[int] = Field(default=None, foreign_key="vendors.id")
    safety_stock: int = Field(default=0)
    reorder_threshold_days: int = Field(default=30)
    lead_time_days: Optional[int] = None


class PartAlias(SQLModel, table=True):
    __tablename__ = "part_aliases"

    id: Optional[int] = Field(default=None, primary_key=True)
    part_id: int = Field(foreign_key="parts.id")
    source_system: str
    alias_value: str


class MachineModel(SQLModel, table=True):
    __tablename__ = "machine_models"

    id: Optional[int] = Field(default=None, primary_key=True)
    manufacturer: str
    model_name: str
    aka: Optional[str] = None


class RepairStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    CANCELLED = "cancelled"


class RepairOrder(SQLModel, table=True):
    __tablename__ = "repair_orders"

    id: Optional[int] = Field(default=None, primary_key=True)
    source_system: str
    external_id: str = Field(index=True)
    machine_model_id: Optional[int] = Field(default=None, foreign_key="machine_models.id")
    status: RepairStatus = Field(default=RepairStatus.OPEN)
    failure_mode: Optional[str] = Field(default=None, index=True)
    opened_at: datetime = Field(default_factory=datetime.utcnow)
    closed_at: Optional[datetime] = None
    customer_name: Optional[str] = None
    notes: Optional[str] = None


class RepairPartUsage(SQLModel, table=True):
    __tablename__ = "repair_part_usage"

    id: Optional[int] = Field(default=None, primary_key=True)
    repair_id: int = Field(foreign_key="repair_orders.id")
    part_id: int = Field(foreign_key="parts.id")
    quantity: float = Field(default=1)
    unit_sale_price: Optional[float] = Field(default=None, nullable=True)
    source_line_id: Optional[str] = None


class InventoryTransactionType(str, Enum):
    ADJUSTMENT = "adjustment"
    RECEIPT = "receipt"
    CONSUMPTION = "consumption"


class InventoryTransaction(SQLModel, table=True):
    __tablename__ = "inventory_transactions"

    id: Optional[int] = Field(default=None, primary_key=True)
    part_id: int = Field(foreign_key="parts.id")
    transaction_type: InventoryTransactionType
    quantity: float
    occurred_at: datetime = Field(default_factory=datetime.utcnow)
    source: Optional[str] = None
    source_id: Optional[str] = None


class VendorPriceSnapshot(SQLModel, table=True):
    __tablename__ = "vendor_price_snapshots"

    id: Optional[int] = Field(default=None, primary_key=True)
    vendor_id: int = Field(foreign_key="vendors.id")
    part_id: int = Field(foreign_key="parts.id")
    unit_cost: float
    currency: str = Field(default="USD")
    captured_at: datetime = Field(default_factory=datetime.utcnow, index=True)


class PurchaseOrder(SQLModel, table=True):
    __tablename__ = "purchase_orders"

    id: Optional[int] = Field(default=None, primary_key=True)
    vendor_id: int = Field(foreign_key="vendors.id")
    source_system: str
    external_id: str
    ordered_at: datetime = Field(default_factory=datetime.utcnow)
    expected_receipt: Optional[datetime] = None
    status: Optional[str] = None


class PurchaseOrderLine(SQLModel, table=True):
    __tablename__ = "purchase_order_lines"

    id: Optional[int] = Field(default=None, primary_key=True)
    purchase_order_id: int = Field(foreign_key="purchase_orders.id")
    part_id: Optional[int] = Field(default=None, foreign_key="parts.id")
    description: Optional[str] = None
    quantity: float = Field(default=1)
    unit_cost: Optional[float] = None
    source_line_id: Optional[str] = None


class GmailMessage(SQLModel, table=True):
    __tablename__ = "gmail_messages"

    id: Optional[int] = Field(default=None, primary_key=True)
    account_email: str
    message_id: str = Field(index=True)
    thread_id: str
    subject: Optional[str] = None
    sender: Optional[str] = None
    received_at: datetime
    raw_payload_path: Optional[str] = None
    processed_at: Optional[datetime] = None


class EmailReceiptLine(SQLModel, table=True):
    __tablename__ = "email_receipt_lines"

    id: Optional[int] = Field(default=None, primary_key=True)
    gmail_message_id: int = Field(foreign_key="gmail_messages.id")
    vendor_name: Optional[str] = None
    part_hint: Optional[str] = None
    quantity: Optional[float] = None
    unit_cost: Optional[float] = None
    currency: str = Field(default="USD")


class SyncStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"


class SyncRun(SQLModel, table=True):
    __tablename__ = "sync_runs"

    id: Optional[int] = Field(default=None, primary_key=True)
    job_name: str
    status: SyncStatus = Field(default=SyncStatus.PENDING)
    started_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    finished_at: Optional[datetime] = None
    records_processed: Optional[int] = None
    detail: Optional[str] = None
