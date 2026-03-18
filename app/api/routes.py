from __future__ import annotations

from typing import List, Optional

from sqlalchemy import func
from sqlmodel import Session, select
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.api.deps import require_api_key
from app.db.session import engine
from app.models import Part, RepairOrder, RepairStatus, Vendor, SyncRun
from app.workers.sync_runner import run_gmail_sync, run_quickbooks_sync

router = APIRouter(prefix="/v1")

DEFAULT_GMAIL_ACCOUNTS = ["service@kanencoffee.com", "samuel@kanencoffee.com"]


class SyncRequest(BaseModel):
    quickbooks: bool = False
    gmail: bool = False
    changed_since: Optional[str] = None
    gmail_accounts: Optional[List[str]] = None
    gmail_query: str = "label:receipts"
    gmail_pages: int = Field(default=5, ge=1, le=50)
    simulate_only: bool = False


@router.get("/status", tags=["system"])
async def service_status() -> dict[str, str]:
    return {"message": "Need input!", "detail": "Backend skeleton is up."}


@router.get("/dashboard/summary", tags=["dashboard"])
def dashboard_summary() -> dict:
    """Full analytics dashboard: stockout alerts, burn rates, velocity, brand profiles, customers."""
    from app.services.analytics import generate_dashboard_summary
    try:
        return generate_dashboard_summary()
    except Exception as e:
        # Fallback to basic metrics if analytics engine fails
        with Session(engine) as session:
            active_repairs = session.exec(
                select(func.count()).select_from(RepairOrder).where(RepairOrder.status == RepairStatus.OPEN)
            ).one()
            low_stock_parts = session.exec(
                select(func.count()).select_from(Part).where(Part.safety_stock > 0)
            ).one()
            avg_lead_time = session.exec(select(func.avg(Part.lead_time_days))).one()
        return {
            "headline": {
                "active_repairs": int(active_repairs or 0),
                "low_stock_parts": int(low_stock_parts or 0),
                "avg_lead_time_days": float(avg_lead_time or 0.0),
            },
            "error": str(e),
        }


@router.get("/analytics/stockout-alerts", tags=["analytics"])
def stockout_alerts() -> dict:
    """Parts at risk of stockout based on burn rate vs. supply."""
    from app.services.analytics import get_stockout_alerts
    alerts = get_stockout_alerts()
    return {
        "count": len(alerts),
        "items": [
            {"part": a.part_name, "severity": a.severity, "monthly_rate": a.monthly_rate,
             "net_supply": a.net_supply, "message": a.message}
            for a in alerts
        ],
    }


@router.get("/analytics/burn-rates", tags=["analytics"])
def burn_rates(months: int = 6) -> dict:
    """Monthly part consumption rates."""
    from app.services.analytics import get_part_burn_rates
    rates = get_part_burn_rates(months_back=months)
    return {
        "items": [
            {"part": br.part_name, "monthly_rate": br.monthly_rate, "total_used": br.total_used,
             "total_ordered": br.total_ordered, "net_supply": br.net_supply,
             "months_of_stock": br.months_of_stock}
            for br in rates
        ],
    }


@router.get("/analytics/repair-velocity", tags=["analytics"])
def repair_velocity(period: str = "week", months: int = 3) -> dict:
    """Repair volume trends by week or month."""
    from app.services.analytics import get_repair_velocity
    trends = get_repair_velocity(period=period, months_back=months)
    return {"items": [{"period": t.period, "count": t.count} for t in trends]}


@router.get("/analytics/brand-profiles", tags=["analytics"])
def brand_profiles(months: int = 6) -> dict:
    """Which machine brands generate the most repairs and which parts they consume."""
    from app.services.analytics import get_brand_failure_profile
    return get_brand_failure_profile(months_back=months)


@router.get("/analytics/parts-profit", tags=["analytics"])
def parts_profit(months: int = 6, top_n: int = 20) -> dict:
    """Parts profit analytics: revenue (from QuickBooks invoice unit prices) vs.
    cost (from vendor receipt emails).

    Returns top parts by profit, top by margin %, and parts sold below cost.
    """
    from app.services.analytics import get_parts_profit
    return get_parts_profit(months_back=months, top_n=top_n)


@router.get("/analytics/repeat-customers", tags=["analytics"])
def repeat_customers(min_repairs: int = 3) -> dict:
    """Repeat customers sorted by repair count."""
    from app.services.analytics import get_top_customers
    customers = get_top_customers(min_repairs=min_repairs)
    return {
        "items": [
            {"name": c.name, "repairs": c.repair_count, "first": c.first_repair,
             "last": c.last_repair, "commercial": c.is_commercial}
            for c in customers
        ],
    }


@router.get("/repairs/recent", tags=["repairs"] )
def recent_repairs(limit: int = 5) -> dict:
    with Session(engine) as session:
        stmt = (
            select(RepairOrder)
            .order_by(RepairOrder.opened_at.desc())
            .limit(limit)
        )
        repairs = session.exec(stmt).all()
    data = [
        {
            "external_id": r.external_id,
            "opened_at": r.opened_at,
            "closed_at": r.closed_at,
            "status": r.status.value if r.status else None,
            "failure_mode": r.failure_mode,
            "customer": r.customer_name,
        }
        for r in repairs
    ]
    return {"items": data}


@router.get("/inventory/low-stock", tags=["inventory"] )
def low_stock_parts(limit: int = 5) -> dict:
    with Session(engine) as session:
        stmt = (
            select(Part)
            .where(Part.safety_stock > 0)
            .order_by(Part.safety_stock.desc())
            .limit(limit)
        )
        parts = session.exec(stmt).all()
    payload = [
        {"name": p.name, "sku": p.sku, "safety_stock": p.safety_stock, "lead_time_days": p.lead_time_days}
        for p in parts
    ]
    return {"items": payload}


@router.get("/vendors/scorecard", tags=["vendors"] )
def vendor_scorecard(limit: int = 5) -> dict:
    with Session(engine) as session:
        vendor_rows = session.exec(select(Vendor).limit(limit)).all()
        results = []
        for vendor in vendor_rows:
            avg_lead = session.exec(
                select(func.avg(Part.lead_time_days)).where(Part.preferred_vendor_id == vendor.id)
            ).one()
            results.append({
                "name": vendor.name,
                "lead_time_days": float(avg_lead or 0.0),
                "on_time_rate": 0.9,
            })
    if not results:
        return {"items": [
            {"name": "Northwest Espresso Supply", "lead_time_days": 12, "on_time_rate": 0.96},
            {"name": "EuroMatic Parts", "lead_time_days": 18, "on_time_rate": 0.91},
        ]}
    return {"items": results}


@router.get("/analytics/failure-modes", tags=["analytics"])
def failure_modes(limit: int = 5) -> dict:
    with Session(engine) as session:
        stmt = (
            select(RepairOrder.failure_mode, func.count().label("count"))
            .where(RepairOrder.failure_mode.is_not(None))
            .group_by(RepairOrder.failure_mode)
            .order_by(func.count().desc())
            .limit(limit)
        )
        rows = session.exec(stmt).all()
    if not rows:
        return {"items": [{"failure_mode": "Grouphead leak", "count": 4}, {"failure_mode": "Pump pressure low", "count": 3}]}
    return {"items": [{"failure_mode": fm, "count": count} for fm, count in rows]}

@router.get("/inventory/forecast", tags=["inventory"])
def inventory_forecast(limit: int = 5) -> dict:
    with Session(engine) as session:
        stmt = (
            select(Part)
            .where(Part.safety_stock > 0)
            .order_by(Part.reorder_threshold_days.desc())
            .limit(limit)
        )
        parts = session.exec(stmt).all()
    if not parts:
        return {"items": [{"name": "Gasket Kit", "recommended_order": 12, "daily_usage": 0.8}]}
    payload = []
    for part in parts:
        usage = max(0.5, (part.safety_stock or 1) / 14)
        recommended = int((part.safety_stock or 1) + usage * (part.lead_time_days or 14))
        payload.append(
            {
                "name": part.name,
                "recommended_order": recommended,
                "daily_usage": round(usage, 2),
            }
        )
    return {"items": payload}


@router.post(
    "/system/run-sync",
    tags=["system"],
    dependencies=[Depends(require_api_key)],
    status_code=status.HTTP_202_ACCEPTED,
)
def trigger_sync_jobs(payload: SyncRequest, background_tasks: BackgroundTasks) -> dict:
    selected: list[str] = []
    if payload.quickbooks:
        selected.append("quickbooks")
    if payload.gmail:
        selected.append("gmail")
    if not selected:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Select at least one job to run.")

    accounts = payload.gmail_accounts or DEFAULT_GMAIL_ACCOUNTS
    accounts = [acct.strip() for acct in accounts if acct and acct.strip()]
    if payload.gmail and not accounts:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provide at least one Gmail account.")

    if payload.simulate_only:
        return {"detail": "Simulation only — no syncs enqueued", "jobs": selected}

    if payload.quickbooks:
        background_tasks.add_task(run_quickbooks_sync, payload.changed_since)
    if payload.gmail:
        background_tasks.add_task(run_gmail_sync, accounts, payload.gmail_query, payload.gmail_pages)

    return {"detail": "Sync jobs enqueued", "jobs": selected}


@router.get("/system/sync-runs", tags=["system"], dependencies=[Depends(require_api_key)])
def sync_runs(limit: int = 10) -> dict:
    with Session(engine) as session:
        stmt = select(SyncRun).order_by(SyncRun.started_at.desc()).limit(limit)
        runs = session.exec(stmt).all()
    payload = [
        {
            "job_name": run.job_name,
            "status": run.status.value,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "records_processed": run.records_processed,
            "detail": run.detail,
        }
        for run in runs
    ]
    return {"items": payload}


@router.get("/v1/analytics/technicians")
async def technician_performance(months: int = 12):
    """Technician performance metrics from Pipedrive + QuickBooks cross-reference."""
    from app.services.analytics import get_technician_performance
    return get_technician_performance(months_back=months)


@router.get("/v1/analytics/revenue-breakdown")
async def revenue_breakdown(months: int = 12):
    """Revenue breakdown by category: Labor, Parts, Equipment, Non-Revenue."""
    from app.services.analytics import get_revenue_breakdown
    return get_revenue_breakdown(months_back=months)
