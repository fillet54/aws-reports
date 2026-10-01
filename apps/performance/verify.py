"""
Live checks of the assumptions the app makes about Amazon's APIs.

Run once a brand is authorized:

    python manage.py verify_amazon <brand-slug>

Each check calls the real API through the same client code the syncs use,
then checks one assumption (a column exists, a role is granted, data
arrives N days late, numbers from two sources agree...). Nothing is stored
in the database, except advertising profile IDs looked up for the brand.
Use --save-dir to keep every raw response for inspection.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone as dt_timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable

from apps.catalog.models import Brand, Marketplace
from apps.reports.periods import report_today
from apps.sales import metrics
from apps.sales.amazon import REPORT_BY_LAST_UPDATE
from apps.sales.ingest import REPORT_COLUMNS, decode_report, parse_datetime, parse_rows

from .clients import LOOKBACK_DAYS, NotConfigured
from .ingest import AD_COLUMNS, parse_ad_rows, parse_date, parse_subscriptions, parse_traffic

PASS, WARN, FAIL, SKIP, INFO = "pass", "warn", "fail", "skip", "info"
GROUPS = ("orders", "traffic", "subscriptions", "ads", "dsp")

# Columns the app can't work without; the rest are nice to have.
REQUIRED_ORDER_COLUMNS = [
    "amazon-order-id", "purchase-date", "last-updated-date", "order-status", "sales-channel",
    "product-name", "sku", "asin", "item-status", "quantity", "currency", "item-price",
]
TRAFFIC_FIELDS = {
    "salesByDate": ["orderedProductSales", "unitsOrdered"],
    "trafficByDate": ["sessions", "pageViews", "averageOfferCount"],
}
# How far two sources may disagree before a cross-check warns.
TOLERANCE = Decimal("0.05")


class StopGroup(Exception):
    """Later checks in the group depend on this one; don't run them."""


@dataclass
class Result:
    group: str
    name: str
    status: str
    detail: str = ""


def describe_error(exc: Exception) -> str:
    """Turn API errors into something actionable."""
    text = str(exc)
    code = getattr(exc, "code", None) or getattr(exc, "status_code", None)
    name = type(exc).__name__
    if code in (401, 403) or "Forbidden" in name or "Unauthorized" in text or "403" in text:
        return f"Access denied ({name}). The app or token is missing a permission/role. {text[:300]}"
    return f"{name}: {text[:400]}"


class Verifier:
    def __init__(
        self,
        brand: Brand,
        orders_client,
        perf_client,
        *,
        today: date | None = None,
        save_dir: Path | None = None,
        slow: bool = False,
        progress: Callable[[Result], None] | None = None,
    ):
        self.brand = brand
        self.orders_client = orders_client
        self.perf_client = perf_client
        self.today = today or report_today()
        self.yesterday = self.today - timedelta(days=1)
        self.save_dir = save_dir
        self.slow = slow
        self.progress = progress or (lambda result: None)
        self.results: list[Result] = []
        self.marketplaces = list(brand.marketplaces.all())

    # -- plumbing -------------------------------------------------------

    def record(self, group, name, status, detail=""):
        result = Result(group, name, status, detail)
        self.results.append(result)
        self.progress(result)
        return result

    @contextmanager
    def check(self, group: str, name: str):
        """Run a block as one check: NotConfigured -> skip, any other error -> fail."""
        try:
            yield
        except NotConfigured as exc:
            self.record(group, name, SKIP, f"Not configured: {exc}")
            raise StopGroup
        except StopGroup:
            raise
        except Exception as exc:
            self.record(group, name, FAIL, describe_error(exc))
            raise StopGroup

    def save(self, name: str, payload):
        if not self.save_dir:
            return
        self.save_dir.mkdir(parents=True, exist_ok=True)
        path = self.save_dir / name
        if isinstance(payload, bytes):
            path.write_bytes(payload)
        else:
            path.write_text(json.dumps(payload, indent=2, default=str))

    def run(self, groups=GROUPS) -> list[Result]:
        for group in groups:
            for marketplace in self.marketplaces if group != "orders" else [None]:
                try:
                    getattr(self, f"check_{group}")(marketplace)
                except StopGroup:
                    pass
        return self.results

    # -- orders ---------------------------------------------------------

    def check_orders(self, _):
        g = "orders"
        now = datetime.now(dt_timezone.utc)
        with self.check(g, "Orders report can be requested with the brand's SP-API token"):
            content = self.orders_client.fetch_orders_report(
                self.brand, now - timedelta(days=2), now, REPORT_BY_LAST_UPDATE
            )
            self.save("orders-last-2-days.tsv", content)
            rows = parse_rows(decode_report(content))
        self.record(g, "Orders report can be requested with the brand's SP-API token", PASS,
                    f"{len(rows)} rows for orders updated in the last 2 days")

        header = decode_report(content).split("\n", 1)[0].split("\t")
        missing = [c for c in REQUIRED_ORDER_COLUMNS if c not in header]
        extra_missing = [c for c in REPORT_COLUMNS if c not in header and c not in REQUIRED_ORDER_COLUMNS]
        self.record(g, "Report has every column the app reads", FAIL if missing else PASS,
                    f"missing: {', '.join(missing)}" if missing else
                    (f"optional columns absent: {', '.join(extra_missing)}" if extra_missing else "all present"))

        if not rows:
            self.record(g, "Row-level checks", SKIP, "no orders in the window; rerun when there are recent orders")
            return

        known = {m.sales_channel.lower(): m.code for m in Marketplace.objects.all()}
        channels = {}
        for r in rows:
            channels[r.get("sales-channel", "")] = channels.get(r.get("sales-channel", ""), 0) + 1
        unknown = {c: n for c, n in channels.items() if c.lower() not in known}
        amazon_unknown = {c: n for c, n in unknown.items() if c.lower().startswith("amazon")}
        detail = ", ".join(f"{c or '(blank)'}: {n}" for c, n in channels.items())
        self.record(g, "sales-channel values map to our stores (Amazon.com → US, Amazon.ca → CA)",
                    FAIL if amazon_unknown else (WARN if unknown else PASS),
                    detail + (f". Not counted in reports: {', '.join(unknown)}" if unknown else ""))

        bad_dates = [r["purchase-date"] for r in rows if not parse_datetime(r.get("purchase-date", ""))]
        self.record(g, "purchase-date / last-updated-date parse as ISO 8601 with a time zone",
                    FAIL if bad_dates else PASS,
                    f"unparseable: {bad_dates[:3]}" if bad_dates else f"e.g. {rows[0].get('purchase-date')}")

        multi = [r for r in rows if (r.get("quantity") or "0").isdigit() and int(r["quantity"]) > 1 and r.get("item-price")]
        if multi:
            r = multi[0]
            self.record(g, "item-price is the line total (price × quantity), not the unit price", INFO,
                        f"Check in Seller Central: order {r['amazon-order-id']}, ASIN {r.get('asin')}, "
                        f"quantity {r['quantity']}, item-price {r['item-price']} {r.get('currency')}. "
                        "The traffic cross-check below also tests this.")

        pending = [r for r in rows if r.get("order-status") == "Pending"]
        priced = [r for r in pending if r.get("item-price")]
        self.record(g, "Pending orders have no item-price yet (so they're counted once priced)",
                    INFO if not pending else (WARN if priced else PASS),
                    f"{len(pending)} pending rows, {len(priced)} with a price" if pending else "no pending orders in the window")

    # -- traffic --------------------------------------------------------

    def check_traffic(self, m):
        g = f"traffic {m.code}"
        start = self.today - timedelta(days=7)
        name = "Sales and Traffic report is available (needs the Brand Analytics role)"
        with self.check(g, name):
            payload = self.perf_client.traffic(self.brand, m, start, self.yesterday)
            self.save(f"traffic-{m.code}.json", payload)
        days = payload.get("salesAndTrafficByDate") or []
        self.record(g, name, PASS if days else WARN, f"{len(days)} days returned for {start} – {self.yesterday}")
        if not days:
            return

        missing = sorted({
            f"{section}.{f}" for row in days for section, fields in TRAFFIC_FIELDS.items()
            for f in fields if f not in (row.get(section) or {})
        })
        self.record(g, "Each day has sessions, units, ordered product sales and average offer count",
                    FAIL if missing else PASS, f"missing: {', '.join(missing)}" if missing else "")

        dates = sorted(parse_date(r["date"]) for r in days)
        self.record(g, "One row per day (dateGranularity=DAY)",
                    PASS if len(dates) == len(set(dates)) else FAIL, f"{dates[0]} … {dates[-1]}")
        lag = (self.today - dates[-1]).days
        self.record(g, "Data arrives about 1–2 days late (dashboard shows “—” until then)",
                    PASS if lag <= 3 else WARN, f"latest day {dates[-1]} = {lag} day(s) before today")

        parsed = parse_traffic(payload)
        sample_day = dates[-1]
        offers = parsed[sample_day]["average_offer_count"]
        self.record(g, "averageOfferCount matches Seller Central's offer count", INFO,
                    f"{sample_day}: averageOfferCount = {offers}. Compare with the Business Report in Seller Central.")

        # Cross-check against the order data the app already has.
        scope = metrics.Scope(brands=(self.brand,), marketplace=m)
        compared = []
        for day in dates[-3:]:
            ours = metrics.totals(scope, day, day)
            theirs = parsed[day]
            if not ours["units"] and not theirs["units_ordered"]:
                continue
            compared.append((day, ours, theirs))
        name = "Order data agrees with Amazon's daily units and sales (tests item-price and time zone)"
        if not compared:
            self.record(g, name, SKIP, "no order data stored for these days; sync or import orders first")
            return
        worst = Decimal("0")
        lines = []
        for day, ours, theirs in compared:
            for label, a, b in (("units", Decimal(ours["units"]), Decimal(theirs["units_ordered"])),
                                ("sales", ours["sales"], theirs["ordered_product_sales"])):
                diff = abs(a - b) / b if b else (Decimal("1") if a else Decimal("0"))
                worst = max(worst, diff)
                lines.append(f"{day} {label}: ours {a} vs Amazon {b}")
        self.record(g, name, PASS if worst <= TOLERANCE else WARN,
                    "; ".join(lines) + ("" if worst <= TOLERANCE else
                    f". Off by up to {worst:.0%}. Pending orders and day boundaries (REPORT_TIME_ZONE) "
                    "explain small gaps; a gap close to the average quantity per order suggests "
                    "item-price is a unit price."))

    # -- subscriptions --------------------------------------------------

    def check_subscriptions(self, m):
        g = f"subscriptions {m.code}"
        start = self.today - timedelta(days=7)
        name = "Replenishment API (Subscribe & Save) answers (needs the Brand Analytics role)"
        with self.check(g, name):
            payload = self.perf_client.subscriptions(self.brand, m, start, self.yesterday)
            self.save(f"subscriptions-{m.code}.json", payload)
        entries = payload.get("metrics") or []
        self.record(g, name, PASS if entries else WARN,
                    f"{len(entries)} entries" if entries else "no metrics returned; is the brand enrolled in Subscribe & Save?")
        if not entries:
            return
        with self.check(g, "Daily entries with activeSubscriptions (aggregationFrequency=DAY)"):
            parsed = parse_subscriptions(payload)
        has_active = all("activeSubscriptions" in e for e in entries)
        self.record(g, "Daily entries with activeSubscriptions (aggregationFrequency=DAY)",
                    PASS if has_active and len(parsed) == len(entries) else FAIL,
                    f"{len(parsed)} distinct days; latest {max(parsed)}: {parsed[max(parsed)]['active_subscriptions']} active")
        lag = (self.today - max(parsed)).days
        self.record(g, "Subscription data delay", INFO, f"latest day {max(parsed)} = {lag} day(s) before today")

    # -- sponsored ads --------------------------------------------------

    def check_ads(self, m):
        g = f"ads {m.code}"
        name = "Ads API token works and lists advertising profiles"
        with self.check(g, name):
            profiles = self.perf_client.list_ads_profiles(self.brand)
            self.save("ads-profiles.json", profiles)
        sellers = [p for p in profiles if p.get("accountInfo", {}).get("type") == "seller"]
        self.record(g, name, PASS, f"{len(profiles)} profiles, {len(sellers)} seller profiles: "
                    + ", ".join(f"{p.get('countryCode')} {p.get('profileId')}" for p in sellers))

        match = next((p for p in sellers if p.get("countryCode") == m.code), None)
        self.record(g, f"A seller profile exists for {m.code} with currency {m.currency}",
                    FAIL if not match else (PASS if match.get("currencyCode") == m.currency else FAIL),
                    "none found" if not match else f"profile {match['profileId']}, currency {match.get('currencyCode')}")
        if not match:
            return

        start = self.today - timedelta(days=3)
        name = "Sponsored ads reports accept our report types and columns"
        with self.check(g, name):
            payload = self.perf_client.sponsored_ads(self.brand, m, start, self.yesterday)
            self.save(f"ads-{m.code}.json", payload)
        self.record(g, name, PASS, ", ".join(f"{p}: {len(payload.get(p) or [])} rows" for p in ("SP", "SB", "SD")))

        for product in ("SP", "SB", "SD"):
            rows = payload.get(product) or []
            label = f"{product} rows have date, cost and {AD_COLUMNS[product]['sales']}"
            if not rows:
                self.record(g, label, SKIP, "no rows (no active campaigns of this type?)")
                continue
            needed = ["date", "cost", AD_COLUMNS[product]["sales"]]
            missing = [c for c in needed if c not in rows[0]]
            if missing:
                self.record(g, label, FAIL, f"missing {missing}; got {sorted(rows[0])}")
                continue
            totals = parse_ad_rows(product, rows)
            cost = sum(d["cost"] for d in totals.values())
            sales = sum(d["sales"] for d in totals.values())
            self.record(g, label, PASS, f"{len(totals)} days, cost {cost} / sales {sales} {m.currency}")

        if self.slow:
            for product in ("SP", "SB", "SD"):
                self._check_lookback(g, m, product)

    def _check_lookback(self, g, m, product):
        """Ask for one day just inside the documented lookback limit."""
        days = LOOKBACK_DAYS[product] - 1
        day = self.today - timedelta(days=days)
        name = f"{product} history goes back {days} days (documented limit {LOOKBACK_DAYS[product]})"
        with self.check(g, name):
            payload = self.perf_client.sponsored_ads(self.brand, m, day, day)
        ranges = payload.get("ranges", {}).get(product)
        self.record(g, name, PASS, f"accepted; {len(payload.get(product) or [])} rows"
                    + (f"; requested range {ranges}" if ranges else ""))

    # -- DSP ------------------------------------------------------------

    def check_dsp(self, m):
        g = f"dsp {m.code}"
        advertiser = self.brand.dsp_advertiser_map().get(m.code)
        if not advertiser:
            self.record(g, "DSP advertiser configured", SKIP, f"no DSP advertiser ID for {m.code} on the brand settings page")
            return
        start = self.today - timedelta(days=3)
        name = f"DSP report for advertiser {advertiser} is accepted (metrics totalCost, totalSales)"
        with self.check(g, name):
            rows = self.perf_client.dsp(self.brand, m, start, self.yesterday)
            self.save(f"dsp-{m.code}.json", rows)
        self.record(g, name, PASS, f"{len(rows)} rows")
        if not rows:
            self.record(g, "DSP rows have date, totalCost and totalSales", SKIP, "no rows in the last 3 days")
            return
        missing = [c for c in ("date", "totalCost", "totalSales") if c not in rows[0]]
        self.record(g, "DSP rows have date, totalCost and totalSales", FAIL if missing else PASS,
                    f"missing {missing}; got {sorted(rows[0])}" if missing else f"e.g. date={rows[0]['date']!r}")
        if not missing:
            with self.check(g, "DSP dates parse"):
                parse_ad_rows("DSP", rows)
            self.record(g, "DSP dates parse", PASS)
