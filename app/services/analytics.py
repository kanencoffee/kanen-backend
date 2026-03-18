"""Repair Intelligence & Inventory Analytics Service.

Provides stockout risk alerts, part burn rate analysis, supplier gap detection,
customer loyalty metrics, repair velocity trends, and financial breakdowns.

Line item classification:
- LABOR: Service Hours, Tune-ups, Travel, Diagnostic, Commercial Service, etc.
- PARTS: Physical components (pumps, valves, gaskets, o-rings, etc.)
- EQUIPMENT: Espresso machines, grinders sold
- NON-REVENUE: Deposits, Tips, Shipping, Discounts, Rentals, Warranties
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlmodel import Session, text

from app.db.session import engine


# ── Classification patterns ──────────────────────────────────────────
LABOR_PATTERNS = [
    '%Service Hour%', '%Tune-up', '%Tune-up %', '%Travel%', '%Diagnostic%',
    '%Commercial Service%', '%Commercial Domestic%', '%Premium Service%',
    '%Consultation%', '%Jura Service%', '%Grouphead Overhaul',
    '%Rancilio Silvia Tune-up', '%La Pavoni Tune-up',
    '%Delonghi Tune-up', '%Superauto Tune-up', '%Dual Boiler Tune-up',
    '%Semi Auto Tune-up', '%E-61 Grouphead Overhaul',
]

NON_REVENUE_PATTERNS = [
    '%Deposit%', '%Tips%', '%Tip%', '%Square Tips%', '%Shipping%',
    '%Discount%', '%Rental%', '%Warranty%', '%Gift Card%',
    '%Drop-Off%', '%Other - Services%', '%Coffee',
    '%Latte Art%', '%Training%',
]

EQUIPMENT_PATTERNS = [
    '%Espresso Machine%', '%Lelit Bianca%', '%Lelit Elizabeth%',
    '%Lelit Mara%', '%Lelit Anna%', '%Lelit Kate%', '%Lelit Victoria%',
    '%Breville Barista%', '%Breville Dual Boiler%', '%Breville Oracle%',
    '%Breville Bambino%', '%La Marzocco%', '%Rancilio Silvia Pro%',
    '%Eureka%Grinder%', '%LUCCA Atom%', '%Baratza%',
    '%Schaerer%', '%Refurbished Espresso%', '%Flair Royal%',
    '%Bellwether%', '%Lelit Fred%', '%Lelit William%',
]

TUNE_UP_PARTS_PATTERNS = [
    '%Tune-up Parts%', '%overhaul kit%', '%Gasket Set%',
]


def _not_like_clauses(column: str, patterns: list, prefix: str = "ex") -> tuple:
    """Build NOT LIKE clauses and params for exclusion."""
    clauses = []
    params = {}
    for i, pat in enumerate(patterns):
        key = f"{prefix}_{i}"
        clauses.append(f"{column} NOT LIKE :{key}")
        params[key] = pat
    return " AND ".join(clauses), params


def _like_clauses(column: str, patterns: list, prefix: str = "inc") -> tuple:
    """Build OR LIKE clauses for inclusion."""
    clauses = []
    params = {}
    for i, pat in enumerate(patterns):
        key = f"{prefix}_{i}"
        clauses.append(f"{column} LIKE :{key}")
        params[key] = pat
    return " OR ".join(clauses), params


@dataclass
class PartBurnRate:
    part_name: str
    active_months: int
    total_used: float
    monthly_rate: float
    total_ordered: float
    net_supply: float
    months_of_stock: Optional[float]


@dataclass
class StockoutAlert:
    part_name: str
    monthly_rate: float
    net_supply: float
    severity: str
    message: str


@dataclass
class RepairTrend:
    period: str
    count: int


@dataclass
class CustomerInsight:
    name: str
    repair_count: int
    first_repair: str
    last_repair: str
    is_commercial: bool


def _execute(query: str, params: Optional[Dict] = None) -> List[Any]:
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]


# ── PARTS-ONLY burn rates (excludes labor, equipment, non-revenue) ──

def get_part_burn_rates(months_back: int = 6, min_usage: float = 3.0) -> List[PartBurnRate]:
    cutoff = (datetime.utcnow() - timedelta(days=months_back * 30)).strftime("%Y-%m-%d")

    # Build exclusion clauses for labor + non-revenue + equipment
    all_exclude = LABOR_PATTERNS + NON_REVENUE_PATTERNS + EQUIPMENT_PATTERNS
    excl_clause, excl_params = _not_like_clauses("p.name", all_exclude, "ex")
    # Also exclude generic "Parts" catch-all
    excl_clause += " AND p.name != 'Parts' AND p.name != 'Commercial Parts' AND p.name != 'Accessories'"

    rows = _execute(f"""
        SELECT p.name,
            count(DISTINCT strftime('%Y-%m', ro.opened_at)) as active_months,
            sum(rpu.quantity) as total_used,
            sum(rpu.quantity) * 1.0 / count(DISTINCT strftime('%Y-%m', ro.opened_at)) as monthly_rate,
            COALESCE(ordered.total_ordered, 0) as total_ordered,
            COALESCE(ordered.total_ordered, 0) - sum(rpu.quantity) as net_supply
        FROM repair_part_usage rpu
        JOIN parts p ON p.id = rpu.part_id
        JOIN repair_orders ro ON ro.id = rpu.repair_id
        LEFT JOIN (
            SELECT pol.part_id, sum(pol.quantity) as total_ordered
            FROM purchase_order_lines pol
            JOIN purchase_orders po ON po.id = pol.purchase_order_id
            WHERE po.ordered_at >= :cutoff
            GROUP BY pol.part_id
        ) ordered ON ordered.part_id = p.id
        WHERE ro.opened_at >= :cutoff
        AND {excl_clause}
        GROUP BY p.name
        HAVING total_used >= :min_usage
        ORDER BY monthly_rate DESC
    """, {"cutoff": cutoff, "min_usage": min_usage, **excl_params})

    results = []
    for r in rows:
        rate = r["monthly_rate"]
        net = r["net_supply"]
        months_stock = (net / rate) if rate > 0 and net > 0 else 0 if net <= 0 else None
        results.append(PartBurnRate(
            part_name=r["name"],
            active_months=r["active_months"],
            total_used=r["total_used"],
            monthly_rate=round(rate, 1),
            total_ordered=r["total_ordered"],
            net_supply=round(net, 1),
            months_of_stock=round(months_stock, 1) if months_stock is not None else None,
        ))
    return results


def get_stockout_alerts(months_back: int = 6) -> List[StockoutAlert]:
    burn_rates = get_part_burn_rates(months_back=months_back, min_usage=2.0)
    alerts = []
    for br in burn_rates:
        if br.net_supply >= 0 and br.months_of_stock and br.months_of_stock > 2:
            continue
        if br.monthly_rate >= 5 and br.total_ordered == 0:
            severity = "critical"
            msg = f"🔴 {br.part_name}: burning {br.monthly_rate}/month with ZERO on order"
        elif br.monthly_rate >= 2 and br.net_supply <= 0:
            severity = "warning"
            msg = f"🟡 {br.part_name}: burning {br.monthly_rate}/month, net supply {br.net_supply}"
        elif br.net_supply < 0:
            severity = "watch"
            msg = f"👀 {br.part_name}: consumption exceeding orders by {abs(br.net_supply)}"
        else:
            continue
        alerts.append(StockoutAlert(
            part_name=br.part_name, monthly_rate=br.monthly_rate,
            net_supply=br.net_supply, severity=severity, message=msg,
        ))
    return sorted(alerts, key=lambda a: {"critical": 0, "warning": 1, "watch": 2}[a.severity])


# ── Financial breakdown: Labor vs Parts vs Equipment ──

def get_revenue_breakdown(months_back: int = 12) -> Dict[str, Any]:
    """Break down total revenue into Labor, Parts, Equipment, and Non-Revenue categories."""
    cutoff = (datetime.utcnow() - timedelta(days=months_back * 30)).strftime("%Y-%m-%d")

    # Get all revenue lines
    rows = _execute("""
        SELECT p.name AS item_name,
               SUM(rpu.quantity * rpu.unit_sale_price) AS revenue,
               SUM(rpu.quantity) AS qty
        FROM repair_part_usage rpu
        JOIN parts p ON p.id = rpu.part_id
        JOIN repair_orders ro ON ro.id = rpu.repair_id
        WHERE rpu.unit_sale_price IS NOT NULL AND rpu.unit_sale_price > 0
          AND ro.opened_at >= :cutoff
        GROUP BY p.name
        ORDER BY revenue DESC
    """, {"cutoff": cutoff})

    def _matches(name: str, patterns: list) -> bool:
        name_lower = name.lower()
        for pat in patterns:
            p = pat.strip('%').lower()
            if p in name_lower:
                return True
        return False

    labor_items = []
    parts_items = []
    equipment_items = []
    non_revenue_items = []

    for r in rows:
        name = r["item_name"]
        rev = float(r["revenue"])
        qty = float(r["qty"])
        entry = {"name": name, "revenue": round(rev, 2), "qty": qty}

        if _matches(name, LABOR_PATTERNS):
            labor_items.append(entry)
        elif _matches(name, NON_REVENUE_PATTERNS):
            non_revenue_items.append(entry)
        elif _matches(name, EQUIPMENT_PATTERNS):
            equipment_items.append(entry)
        else:
            parts_items.append(entry)

    labor_total = sum(i["revenue"] for i in labor_items)
    parts_total = sum(i["revenue"] for i in parts_items)
    equip_total = sum(i["revenue"] for i in equipment_items)
    non_rev_total = sum(i["revenue"] for i in non_revenue_items)
    grand_total = labor_total + parts_total + equip_total + non_rev_total

    return {
        "period_months": months_back,
        "grand_total": round(grand_total, 2),
        "labor": {
            "total": round(labor_total, 2),
            "pct": round(labor_total / grand_total * 100, 1) if grand_total > 0 else 0,
            "items": sorted(labor_items, key=lambda x: -x["revenue"])[:15],
        },
        "parts": {
            "total": round(parts_total, 2),
            "pct": round(parts_total / grand_total * 100, 1) if grand_total > 0 else 0,
            "items": sorted(parts_items, key=lambda x: -x["revenue"])[:15],
        },
        "equipment": {
            "total": round(equip_total, 2),
            "pct": round(equip_total / grand_total * 100, 1) if grand_total > 0 else 0,
            "items": sorted(equipment_items, key=lambda x: -x["revenue"])[:15],
        },
        "non_revenue": {
            "total": round(non_rev_total, 2),
            "pct": round(non_rev_total / grand_total * 100, 1) if grand_total > 0 else 0,
            "items": sorted(non_revenue_items, key=lambda x: -x["revenue"])[:10],
        },
    }


# ── Parts profit (PARTS ONLY — excludes labor, equipment, non-revenue) ──

def get_parts_profit(months_back: int = 12, top_n: int = 20) -> Dict[str, Any]:
    cutoff = (datetime.utcnow() - timedelta(days=months_back * 30)).strftime("%Y-%m-%d")

    # Exclude labor + non-revenue + equipment
    all_exclude = LABOR_PATTERNS + NON_REVENUE_PATTERNS + EQUIPMENT_PATTERNS
    excl_clause, excl_params = _not_like_clauses("p.name", all_exclude, "ex")
    excl_clause += " AND p.name != 'Parts' AND p.name != 'Commercial Parts' AND p.name != 'Accessories'"

    revenue_rows = _execute(f"""
        SELECT p.name AS part_name,
               SUM(rpu.quantity * rpu.unit_sale_price) AS total_revenue,
               SUM(rpu.quantity) AS total_qty_sold
        FROM repair_part_usage rpu
        JOIN parts p ON p.id = rpu.part_id
        JOIN repair_orders ro ON ro.id = rpu.repair_id
        WHERE rpu.unit_sale_price IS NOT NULL AND rpu.unit_sale_price > 0
          AND ro.opened_at >= :cutoff AND {excl_clause}
        GROUP BY p.name
        HAVING total_revenue > 0
    """, {"cutoff": cutoff, **excl_params})

    if not revenue_rows:
        return {"top_by_profit": [], "top_by_margin_pct": [], "sold_below_cost": [], "period_months": months_back}

    # Cost from email receipt lines
    cost_rows = _execute("""
        SELECT erl.part_hint, AVG(erl.unit_cost) AS avg_unit_cost
        FROM email_receipt_lines erl
        WHERE erl.unit_cost IS NOT NULL AND erl.unit_cost > 0
        GROUP BY erl.part_hint
    """, {})

    cost_lookup: Dict[str, float] = {}
    for cr in cost_rows:
        if cr["part_hint"]:
            cost_lookup[cr["part_hint"].lower()] = cr["avg_unit_cost"]

    def _best_cost(part_name: str) -> Optional[float]:
        name_lower = part_name.lower()
        if name_lower in cost_lookup:
            return cost_lookup[name_lower]
        for hint, cost in cost_lookup.items():
            if name_lower in hint or hint in name_lower:
                return cost
        return None

    results = []
    for row in revenue_rows:
        pname = row["part_name"]
        revenue = float(row["total_revenue"])
        qty_sold = float(row["total_qty_sold"])
        avg_cost = _best_cost(pname)
        total_cost = (avg_cost * qty_sold) if avg_cost is not None else None
        margin = (revenue - total_cost) if total_cost is not None else None
        margin_pct = (margin / revenue * 100) if (margin is not None and revenue > 0) else None
        results.append({
            "part_name": pname,
            "total_revenue": round(revenue, 2),
            "total_qty_sold": qty_sold,
            "avg_unit_cost": round(avg_cost, 4) if avg_cost is not None else None,
            "total_cost": round(total_cost, 2) if total_cost is not None else None,
            "margin": round(margin, 2) if margin is not None else None,
            "margin_pct": round(margin_pct, 1) if margin_pct is not None else None,
            "cost_available": avg_cost is not None,
        })

    with_margin = [r for r in results if r["margin"] is not None]
    without_margin = [r for r in results if r["margin"] is None]

    return {
        "period_months": months_back,
        "parts_with_revenue": len(results),
        "parts_with_cost_data": len(with_margin),
        "parts_missing_cost": len(without_margin),
        "top_by_profit": sorted(with_margin, key=lambda r: r["margin"], reverse=True)[:top_n],
        "top_by_margin_pct": sorted(
            [r for r in with_margin if r["margin_pct"] is not None],
            key=lambda r: r["margin_pct"], reverse=True
        )[:top_n],
        "sold_below_cost": sorted(
            [r for r in with_margin if r["margin"] < 0], key=lambda r: r["margin"]
        ),
        "all_parts": sorted(results, key=lambda r: -(r["total_revenue"] or 0)),
    }


# ── Repair velocity ──

def get_repair_velocity(period: str = "week", months_back: int = 3) -> List[RepairTrend]:
    cutoff = (datetime.utcnow() - timedelta(days=months_back * 30)).strftime("%Y-%m-%d")
    if period == "week":
        group_expr = "strftime('%Y-W%W', opened_at)"
    else:
        group_expr = "strftime('%Y-%m', opened_at)"
    rows = _execute(f"""
        SELECT {group_expr} as period, count(*) as cnt
        FROM repair_orders WHERE opened_at >= :cutoff
        GROUP BY period ORDER BY period
    """, {"cutoff": cutoff})
    return [RepairTrend(period=r["period"], count=r["cnt"]) for r in rows]


# ── Repeat customers ──

def get_top_customers(min_repairs: int = 3, months_back: int = 12) -> List[CustomerInsight]:
    cutoff = (datetime.utcnow() - timedelta(days=months_back * 30)).strftime("%Y-%m-%d")
    rows = _execute("""
        SELECT customer_name, count(*) as repairs,
            min(date(opened_at)) as first_repair, max(date(opened_at)) as last_repair
        FROM repair_orders
        WHERE customer_name IS NOT NULL AND opened_at >= :cutoff
        GROUP BY customer_name HAVING repairs >= :min_repairs
        ORDER BY repairs DESC
    """, {"cutoff": cutoff, "min_repairs": min_repairs})

    commercial_indicators = ["company", "llc", "inc", "corp", "city of", "department",
                             "fire", "cafe", "coffee", "restaurant", "hotel", "university"]
    results = []
    for r in rows:
        name = r["customer_name"]
        is_commercial = any(ind in name.lower() for ind in commercial_indicators)
        results.append(CustomerInsight(
            name=name, repair_count=r["repairs"],
            first_repair=r["first_repair"], last_repair=r["last_repair"],
            is_commercial=is_commercial,
        ))
    return results


# ── Brand failure profiles ──

def get_brand_failure_profile(months_back: int = 6) -> Dict[str, Dict[str, Any]]:
    cutoff = (datetime.utcnow() - timedelta(days=months_back * 30)).strftime("%Y-%m-%d")
    brand_keywords = {
        "Breville": ["breville"], "Jura": ["jura"], "La Pavoni": ["la pavoni", "pavoni"],
        "Delonghi": ["delonghi", "de'longhi"], "E-61 Group": ["e-61", "e61"],
        "La Marzocco": ["la marzocco", "marzocco"], "Gaggia": ["gaggia"],
        "Rancilio": ["rancilio"], "Lelit": ["lelit"], "Profitec": ["profitec"],
        "ECM": ["ecm"], "Rocket": ["rocket"], "Saeco": ["saeco"],
    }
    rows = _execute("""
        SELECT p.name as part_name, sum(rpu.quantity) as qty
        FROM repair_part_usage rpu JOIN parts p ON p.id = rpu.part_id
        JOIN repair_orders ro ON ro.id = rpu.repair_id
        WHERE ro.opened_at >= :cutoff GROUP BY p.name ORDER BY qty DESC
    """, {"cutoff": cutoff})

    profiles = {}
    for brand, keywords in brand_keywords.items():
        brand_parts = []
        total_qty = 0
        for r in rows:
            if any(kw in r["part_name"].lower() for kw in keywords):
                brand_parts.append({"part": r["part_name"], "qty": r["qty"]})
                total_qty += r["qty"]
        if brand_parts:
            profiles[brand] = {"total_part_usage": total_qty, "top_parts": brand_parts[:5]}
    return dict(sorted(profiles.items(), key=lambda x: -x[1]["total_part_usage"]))


# ── Dashboard summary ──

def generate_dashboard_summary() -> Dict[str, Any]:
    alerts = get_stockout_alerts()
    burn_rates = get_part_burn_rates(months_back=6, min_usage=3.0)
    velocity = get_repair_velocity(period="week", months_back=3)
    customers = get_top_customers(min_repairs=3)
    brand_profiles = get_brand_failure_profile(months_back=6)
    revenue = get_revenue_breakdown(months_back=12)

    recent_weeks = velocity[-4:] if len(velocity) >= 4 else velocity
    avg_weekly = sum(v.count for v in recent_weeks) / len(recent_weeks) if recent_weeks else 0
    critical_alerts = [a for a in alerts if a.severity == "critical"]
    warning_alerts = [a for a in alerts if a.severity == "warning"]

    return {
        "headline": {
            "avg_weekly_repairs": round(avg_weekly, 1),
            "annualized_repairs": round(avg_weekly * 52),
            "critical_stockout_alerts": len(critical_alerts),
            "warning_stockout_alerts": len(warning_alerts),
            "unique_repeat_customers": len(customers),
            "brands_tracked": len(brand_profiles),
        },
        "revenue_breakdown": revenue,
        "stockout_alerts": [
            {"part": a.part_name, "severity": a.severity, "rate": a.monthly_rate, "message": a.message}
            for a in alerts[:10]
        ],
        "top_burn_rates": [
            {"part": br.part_name, "monthly_rate": br.monthly_rate, "net_supply": br.net_supply}
            for br in burn_rates[:10]
        ],
        "repair_velocity": [{"period": v.period, "count": v.count} for v in velocity],
        "brand_profiles": {
            brand: {"total": data["total_part_usage"], "top_parts": data["top_parts"][:3]}
            for brand, data in list(brand_profiles.items())[:6]
        },
        "repeat_customers": [
            {"name": c.name, "repairs": c.repair_count, "commercial": c.is_commercial}
            for c in customers[:10]
        ],
    }


# ── Technician Analytics (from Pipedrive labels + QB cross-reference) ──

def get_technician_performance(months_back: int = 12) -> Dict[str, Any]:
    """Technician performance using Pipedrive labels cross-referenced with QB invoices."""
    cutoff = (datetime.utcnow() - timedelta(days=months_back * 30)).strftime("%Y-%m-%d")

    techs = _execute("""
        SELECT pd.technician,
               count(DISTINCT ro.id) as qb_repairs,
               sum(ro_totals.invoice_total) as qb_revenue,
               avg(ro_totals.invoice_total) as avg_ticket,
               avg(CASE WHEN julianday(ro.opened_at) >= julianday(pd.add_time)
                   THEN julianday(ro.opened_at) - julianday(pd.add_time) END) as avg_turnaround_days
        FROM pipedrive_deals pd
        JOIN repair_orders ro ON LOWER(TRIM(ro.customer_name)) = LOWER(TRIM(pd.person_name))
            AND julianday(ro.opened_at) >= julianday(pd.add_time)
            AND julianday(ro.opened_at) - julianday(pd.add_time) < 30
        JOIN (
            SELECT rpu.repair_id, sum(rpu.quantity * rpu.unit_sale_price) as invoice_total
            FROM repair_part_usage rpu WHERE rpu.unit_sale_price IS NOT NULL
            GROUP BY rpu.repair_id HAVING invoice_total > 0
        ) ro_totals ON ro_totals.repair_id = ro.id
        WHERE pd.technician IS NOT NULL AND pd.add_time >= :cutoff
        GROUP BY pd.technician
        ORDER BY qb_revenue DESC
    """, {"cutoff": cutoff})

    # Brand specialization per tech
    brands = _execute("""
        SELECT pd.technician,
            CASE
                WHEN p.name LIKE '%%Breville%%' THEN 'Breville'
                WHEN p.name LIKE '%%Jura%%' THEN 'Jura'
                WHEN p.name LIKE '%%Delonghi%%' THEN 'Delonghi'
                WHEN p.name LIKE '%%La Pavoni%%' OR p.name LIKE '%%Pavoni%%' THEN 'La Pavoni'
                WHEN p.name LIKE '%%E-61%%' OR p.name LIKE '%%E61%%' THEN 'E-61'
                WHEN p.name LIKE '%%Rancilio%%' THEN 'Rancilio'
                WHEN p.name LIKE '%%Gaggia%%' OR p.name LIKE '%%Saeco%%' THEN 'Gaggia/Saeco'
                WHEN p.name LIKE '%%La Marzocco%%' THEN 'La Marzocco'
                WHEN p.name LIKE '%%Superauto%%' THEN 'Superauto'
                ELSE NULL
            END as brand,
            count(DISTINCT ro.id) as repairs
        FROM pipedrive_deals pd
        JOIN repair_orders ro ON LOWER(TRIM(ro.customer_name)) = LOWER(TRIM(pd.person_name))
            AND julianday(ro.opened_at) >= julianday(pd.add_time)
            AND julianday(ro.opened_at) - julianday(pd.add_time) < 30
        JOIN repair_part_usage rpu ON rpu.repair_id = ro.id
        JOIN parts p ON p.id = rpu.part_id
        WHERE pd.technician IS NOT NULL AND pd.add_time >= :cutoff AND rpu.unit_sale_price > 0
        GROUP BY pd.technician, brand
        HAVING brand IS NOT NULL AND repairs >= 2
        ORDER BY pd.technician, repairs DESC
    """, {"cutoff": cutoff})

    # Build brand matrix
    brand_matrix = {}
    for row in brands:
        tech = row["technician"]
        if tech not in brand_matrix:
            brand_matrix[tech] = {}
        brand_matrix[tech][row["brand"]] = row["repairs"]

    return {
        "period_months": months_back,
        "technicians": [
            {
                "name": t["technician"],
                "repairs": t["qb_repairs"],
                "revenue": round(t["qb_revenue"], 2),
                "avg_ticket": round(t["avg_ticket"], 2),
                "avg_turnaround_days": round(t["avg_turnaround_days"], 1) if t["avg_turnaround_days"] else None,
                "brand_specialization": brand_matrix.get(t["technician"], {}),
            }
            for t in techs
        ],
    }
