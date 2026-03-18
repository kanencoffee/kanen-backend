from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class ParsedReceiptLine:
    vendor_name: Optional[str]
    part_hint: str
    quantity: float
    unit_cost: Optional[float]


LINE_PATTERNS = [
    re.compile(r"(?P<qty>\d+(?:\.\d+)?)\s*x\s*(?P<name>.+?)\s+\$?(?P<price>\d+\.\d{2})", re.I),
    re.compile(r"(?P<name>.+?)\s+-\s+qty\s*(?P<qty>\d+(?:\.\d+)?)\s+-\s+\$?(?P<price>\d+\.\d{2})", re.I),
]

ENC_LINE = re.compile(
    r"(?P<desc>[A-Z0-9 \-/]+?)\s+(?P<qty>\d+)\s+\d+\s+\d+\s+\d+\.\d{2}\s+(?P<price>\d+\.\d{2})",
    re.I,
)
PART_CODE = re.compile(r"^\d{6,}$")
MULTISPACE = re.compile(r"\s+")

# Shopify order confirmation pattern: "Product Name × N" then price line
# Used by Clive Coffee and Chris' Coffee
# Handles: "Name × 3", "Name ×3", "Name x 3", "Name x3", "Name\u00d73"
SHOPIFY_ITEM = re.compile(
    r"^(?P<name>.+?)\s*[\xd7\u00d7x×]\s*(?P<qty>\d+)\s*$",
    re.I,
)
SHOPIFY_PRICE = re.compile(r"^\$(?P<price>[\d,]+\.\d{2})$")

# EspressoResource Magento format:
# "Product Name\nSKU: XXXX\nQty  $price"  (plain text)
# or  "Product Name\nSKU: XXXX\nQty $price" in forwarded text
ER_ITEM_BLOCK = re.compile(
    r"(?P<name>[^\n]+?)\n\s*SKU:\s*(?P<sku>\S+)\s*\n\s*(?P<qty>\d+)\s+\$(?P<price>[\d,]+\.\d{2})",
    re.I | re.MULTILINE,
)

# Breville USA: order confirmation emails use "Product Name  Qty  $price" tabular layout
# or "Product Name × N\n$price" (same shopify-style if sold via their shop)
BREVILLE_LINE = re.compile(
    r"^(?P<name>(?:Breville|Sage)\s+.+?)\s{2,}(?P<qty>\d+)\s+\$(?P<price>[\d,]+\.\d{2})",
    re.I | re.MULTILINE,
)


def _parse_encompass(text: str, vendor: Optional[str]) -> List[ParsedReceiptLine]:
    lines: List[ParsedReceiptLine] = []
    pending_code: Optional[str] = None
    for raw_line in text.splitlines():
        normalized = raw_line.strip()
        if not normalized:
            continue
        if PART_CODE.match(normalized):
            pending_code = normalized
            continue
        match = ENC_LINE.search(normalized)
        if match:
            desc = MULTISPACE.sub(" ", match.group("desc")).strip()
            qty = float(match.group("qty"))
            price = float(match.group("price"))
            hint = f"{pending_code or ''} {desc}".strip()
            lines.append(ParsedReceiptLine(vendor_name=vendor, part_hint=hint, quantity=qty, unit_cost=price))
            pending_code = None
            continue
        # fallback: treat any line with a price as a potential item
        price_match = re.findall(r"\d+\.\d{2}", normalized)
        if price_match:
            price = float(price_match[-1])
            qty_match = re.search(r"\b(\d{1,3})\b", normalized)
            qty = float(qty_match.group(1)) if qty_match else 1.0
            hint = f"{pending_code or ''} {normalized}".strip()
            lines.append(ParsedReceiptLine(vendor_name=vendor, part_hint=hint, quantity=qty, unit_cost=price))
            pending_code = None
    return lines


def _parse_shopify_style(text: str, vendor: Optional[str]) -> List[ParsedReceiptLine]:
    """Parse Shopify order confirmation emails (Clive Coffee, Chris' Coffee).

    Text format (plain-text version):
        Product Name × Qty
        [Variant line]
        $price
    """
    lines: List[ParsedReceiptLine] = []
    text_lines = [l.strip() for l in text.splitlines()]
    i = 0
    while i < len(text_lines):
        item_match = SHOPIFY_ITEM.match(text_lines[i])
        if item_match:
            name = item_match.group("name").strip()
            qty = float(item_match.group("qty"))
            # Scan ahead (up to 5 lines) for the price
            price: Optional[float] = None
            for j in range(i + 1, min(i + 6, len(text_lines))):
                price_match = SHOPIFY_PRICE.match(text_lines[j])
                if price_match:
                    price = float(price_match.group("price").replace(",", ""))
                    i = j  # advance past the price line
                    break
            if price is not None:
                # unit_cost = price / qty
                unit_cost = round(price / qty, 4) if qty > 0 else price
                lines.append(ParsedReceiptLine(
                    vendor_name=vendor, part_hint=name, quantity=qty, unit_cost=unit_cost,
                ))
        i += 1
    return lines


def _parse_clive_coffee(text: str, vendor: Optional[str] = "Clive Coffee") -> List[ParsedReceiptLine]:
    """Parse Clive Coffee Shopify order confirmations."""
    return _parse_shopify_style(text, vendor)


def _parse_chris_coffee(text: str, vendor: Optional[str] = "Chris' Coffee") -> List[ParsedReceiptLine]:
    """Parse Chris' Coffee Shopify order confirmations."""
    return _parse_shopify_style(text, vendor)


def _parse_espresso_resource(text: str, vendor: Optional[str] = "EspressoResource") -> List[ParsedReceiptLine]:
    """Parse EspressoResource Magento order confirmation emails.

    Plain-text format:
        Product Name

        SKU: XXXX
        Qty $price
    """
    lines: List[ParsedReceiptLine] = []
    # Try block-regex first (captures 3-line item blocks in forwarded plain text)
    for match in ER_ITEM_BLOCK.finditer(text):
        name = MULTISPACE.sub(" ", match.group("name")).strip()
        sku = match.group("sku").strip()
        qty = float(match.group("qty"))
        price = float(match.group("price").replace(",", ""))
        hint = f"{sku} {name}".strip()
        unit_cost = round(price / qty, 4) if qty > 0 else price
        lines.append(ParsedReceiptLine(vendor_name=vendor, part_hint=hint, quantity=qty, unit_cost=unit_cost))
    if lines:
        return lines

    # Fallback: parse line-by-line looking for SKU anchors
    text_lines = [l.strip() for l in text.splitlines()]
    sku_re = re.compile(r"^SKU:\s*(?P<sku>\S+)$", re.I)
    qty_price_re = re.compile(r"^(?P<qty>\d+)\s+\$(?P<price>[\d,]+\.\d{2})$")

    i = 0
    while i < len(text_lines):
        sku_match = sku_re.match(text_lines[i])
        if sku_match:
            sku = sku_match.group("sku")
            # Name is the non-empty line before SKU
            name = ""
            for k in range(i - 1, max(-1, i - 4), -1):
                if text_lines[k]:
                    name = text_lines[k]
                    break
            # qty/price is the next non-empty line
            for j in range(i + 1, min(i + 5, len(text_lines))):
                qp = qty_price_re.match(text_lines[j])
                if qp:
                    qty = float(qp.group("qty"))
                    price = float(qp.group("price").replace(",", ""))
                    hint = f"{sku} {name}".strip()
                    unit_cost = round(price / qty, 4) if qty > 0 else price
                    lines.append(ParsedReceiptLine(
                        vendor_name=vendor, part_hint=hint, quantity=qty, unit_cost=unit_cost,
                    ))
                    i = j
                    break
        i += 1
    return lines


def _parse_breville_usa(text: str, vendor: Optional[str] = "Breville USA") -> List[ParsedReceiptLine]:
    """Parse Breville USA order confirmation emails.

    Two known formats:
    1. Shopify-style (breville.com uses Shopify): 'Product × N\n$price'
    2. Tabular: 'Product Name  Qty  $price' (whitespace-separated columns)
    """
    # Try Shopify style first
    lines = _parse_shopify_style(text, vendor)
    if lines:
        return lines

    # Try tabular style
    lines = []
    for match in BREVILLE_LINE.finditer(text):
        name = MULTISPACE.sub(" ", match.group("name")).strip()
        qty = float(match.group("qty"))
        price = float(match.group("price").replace(",", ""))
        unit_cost = round(price / qty, 4) if qty > 0 else price
        lines.append(ParsedReceiptLine(vendor_name=vendor, part_hint=name, quantity=qty, unit_cost=unit_cost))
    if lines:
        return lines

    # Generic fallback using LINE_PATTERNS
    fallback = []
    for raw_line in text.splitlines():
        normalized = raw_line.strip()
        if not normalized:
            continue
        for pattern in LINE_PATTERNS:
            m = pattern.search(normalized)
            if m:
                qty = float(m.group("qty")) if m.groupdict().get("qty") else 1.0
                name = m.group("name").strip()
                price = float(m.group("price").replace(",", "")) if m.groupdict().get("price") else None
                fallback.append(ParsedReceiptLine(
                    vendor_name=vendor, part_hint=name, quantity=qty, unit_cost=price,
                ))
                break
    return fallback


# Map vendor name keywords → parser functions
_VENDOR_PARSERS = {
    "encompass": _parse_encompass,
    "clive": _parse_clive_coffee,
    "clivecoffee": _parse_clive_coffee,
    "clive coffee": _parse_clive_coffee,
    "breville": _parse_breville_usa,
    "brevilleusa": _parse_breville_usa,
    "breville usa": _parse_breville_usa,
    "chris": _parse_chris_coffee,
    "chriscoffee": _parse_chris_coffee,
    "chris' coffee": _parse_chris_coffee,
    "chris coffee": _parse_chris_coffee,
    "espressoresource": _parse_espresso_resource,
    "espresso resource": _parse_espresso_resource,
}


def parse_receipt_text(text: str, vendor: Optional[str] = None) -> List[ParsedReceiptLine]:
    # Vendor-specific parsing first
    if vendor:
        vendor_lower = vendor.lower()
        for keyword, parser_fn in _VENDOR_PARSERS.items():
            if keyword in vendor_lower:
                parsed = parser_fn(text, vendor)
                if parsed:
                    return parsed

    lines: List[ParsedReceiptLine] = []
    for raw_line in text.splitlines():
        normalized = raw_line.strip()
        if not normalized:
            continue
        match = None
        for pattern in LINE_PATTERNS:
            match = pattern.search(normalized)
            if match:
                break
        if not match:
            continue
        qty = float(match.group("qty")) if match.groupdict().get("qty") else 1
        name = match.group("name").strip()
        price = float(match.group("price")) if match.groupdict().get("price") else None
        lines.append(ParsedReceiptLine(vendor_name=vendor, part_hint=name, quantity=qty, unit_cost=price))
    return lines
