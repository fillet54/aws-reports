"""
Clients for traffic, advertising and Subscribe & Save data.

Every client method returns Amazon's raw response shape; parsing lives in
ingest.py, so the dummy client and the real APIs go through the same code.

    traffic()        SP-API Sales and Traffic report (GET_SALES_AND_TRAFFIC_REPORT)
    subscriptions()  SP-API Replenishment API, getSellingPartnerMetrics
    sponsored_ads()  Amazon Ads API v3 reports for SP / SB / SD
    dsp()            Amazon Ads API DSP reports

The real clients are written against Amazon's published models but have not
been run against Amazon yet.
"""

from __future__ import annotations

import json
import logging
import random
import time
from datetime import date, timedelta
from decimal import Decimal
from typing import Protocol

from django.conf import settings

from apps.catalog.models import Brand, Marketplace
from apps.sales.amazon import DummyAmazonClient, _seed

logger = logging.getLogger(__name__)

# How far back each API lets you ask (days before today).
LOOKBACK_DAYS = {"SP": 95, "SB": 95, "SD": 60, "DSP": 90}
# Longest date range a single request may cover.
MAX_RANGE_DAYS = {"traffic": 31, "subscriptions": 31, "ads": 31, "dsp": 31}


class PerformanceClient(Protocol):
    name: str

    def traffic(self, brand: Brand, marketplace: Marketplace, start: date, end: date) -> dict: ...
    def subscriptions(self, brand: Brand, marketplace: Marketplace, start: date, end: date) -> dict: ...
    def sponsored_ads(self, brand: Brand, marketplace: Marketplace, start: date, end: date) -> dict: ...
    def dsp(self, brand: Brand, marketplace: Marketplace, start: date, end: date) -> list: ...


class NotConfigured(Exception):
    """The brand isn't connected for this data source; skip it quietly."""


def get_performance_client(name: str | None = None) -> PerformanceClient:
    name = name or settings.AMAZON_CLIENT
    if name == "dummy":
        return DummyPerformanceClient()
    if name == "sp_api":
        return AmazonPerformanceClient()
    raise ValueError(f"Unknown AMAZON_CLIENT {name!r} (expected 'dummy' or 'sp_api').")


def clamp_start(start: date, product: str, today: date) -> date:
    """Move start forward to the oldest day the API still serves for this ad product."""
    return max(start, today - timedelta(days=LOOKBACK_DAYS[product]))


# ---------------------------------------------------------------------------
# Real Amazon clients
# ---------------------------------------------------------------------------


class AmazonPerformanceClient:
    name = "sp_api"
    poll_seconds = 20
    timeout_seconds = 30 * 60

    def effective_start(self, kind: str, start: date) -> date:
        """The oldest day this API will actually return for a request starting at `start`."""
        today = date.today()
        if kind == "ads":
            return clamp_start(start, "SP", today)
        if kind == "dsp":
            return clamp_start(start, "DSP", today)
        return start

    # SP-API ---------------------------------------------------------------

    def _sp_credentials(self, brand: Brand) -> dict:
        token = brand.get_refresh_token()
        if not token:
            raise NotConfigured("no SP-API refresh token")
        return {
            "refresh_token": token,
            "lwa_app_id": settings.SP_API_LWA_APP_ID,
            "lwa_client_secret": settings.SP_API_LWA_CLIENT_SECRET,
        }

    def traffic(self, brand, marketplace, start, end):
        from sp_api.api import Reports
        from sp_api.base import Marketplaces

        api = Reports(marketplace=Marketplaces.US, credentials=self._sp_credentials(brand))
        created = api.create_report(
            reportType="GET_SALES_AND_TRAFFIC_REPORT",
            dataStartTime=start.isoformat(),
            dataEndTime=end.isoformat(),
            marketplaceIds=[marketplace.amazon_marketplace_id],
            reportOptions={"dateGranularity": "DAY", "asinGranularity": "PARENT"},
        )
        report_id = created.payload["reportId"]
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            report = api.get_report(report_id).payload
            status = report.get("processingStatus")
            if status == "DONE":
                break
            if status == "CANCELLED":
                return {"salesAndTrafficByDate": []}
            if status == "FATAL" or time.monotonic() > deadline:
                raise RuntimeError(f"Sales and Traffic report {report_id} failed ({status}).")
            time.sleep(self.poll_seconds)
        document = api.get_report_document(report["reportDocumentId"], download=True)
        return json.loads(document.payload["document"])

    def subscriptions(self, brand, marketplace, start, end):
        from sp_api.api import Replenishment
        from sp_api.base import Marketplaces

        api = Replenishment(marketplace=Marketplaces.US, credentials=self._sp_credentials(brand))
        response = api.get_selling_partner_metrics(
            aggregationFrequency="DAY",
            timeInterval={
                "startDate": f"{start.isoformat()}T00:00:00Z",
                "endDate": f"{end.isoformat()}T23:59:59Z",
            },
            metrics=["ACTIVE_SUBSCRIPTIONS", "SHIPPED_SUBSCRIPTION_UNITS", "TOTAL_SUBSCRIPTIONS_REVENUE"],
            timePeriodType="PERFORMANCE",
            marketplaceId=marketplace.amazon_marketplace_id,
            programTypes=["SUBSCRIBE_AND_SAVE"],
        )
        return response.payload

    # Ads API --------------------------------------------------------------

    def _ads_credentials(self, brand: Brand, profile_id: str | None = None) -> dict:
        token = brand.get_ads_refresh_token()
        if not token:
            raise NotConfigured("no Ads API refresh token")
        credentials = {
            "refresh_token": token,
            "client_id": settings.AMAZON_ADS_CLIENT_ID,
            "client_secret": settings.AMAZON_ADS_CLIENT_SECRET,
        }
        if profile_id:
            credentials["profile_id"] = str(profile_id)
        return credentials

    def list_ads_profiles(self, brand: Brand) -> list[dict]:
        """Every advertising profile the brand's Ads token can see, as Amazon returns them."""
        from ad_api.api import Profiles
        from ad_api.base import Marketplaces

        return Profiles(credentials=self._ads_credentials(brand), marketplace=Marketplaces.NA).list_profiles().payload

    def discover_profiles(self, brand: Brand) -> dict[str, str]:
        """Advertising profile ID per store code (seller accounts only)."""
        profiles = self.list_ads_profiles(brand)
        return {
            p["countryCode"]: str(p["profileId"])
            for p in profiles
            if p.get("accountInfo", {}).get("type") == "seller"
        }

    def _profile_for(self, brand: Brand, marketplace: Marketplace) -> str:
        if marketplace.code not in brand.ads_profiles:
            brand.ads_profiles = self.discover_profiles(brand)
            brand.save(update_fields=["ads_profiles"])
        profile = brand.ads_profiles.get(marketplace.code)
        if not profile:
            raise NotConfigured(f"no advertising profile for {marketplace.code}")
        return profile

    # Report definitions for the v3 reporting API, one per ad product.
    AD_REPORTS = {
        "SP": {
            "adProduct": "SPONSORED_PRODUCTS",
            "reportTypeId": "spCampaigns",
            "columns": ["date", "cost", "sales7d", "purchases7d", "unitsSoldClicks7d", "impressions", "clicks"],
        },
        "SB": {
            "adProduct": "SPONSORED_BRANDS",
            "reportTypeId": "sbCampaigns",
            "columns": ["date", "cost", "sales", "purchases", "unitsSold", "impressions", "clicks"],
        },
        "SD": {
            "adProduct": "SPONSORED_DISPLAY",
            "reportTypeId": "sdCampaigns",
            "columns": ["date", "cost", "sales", "purchases", "unitsSold", "impressions", "clicks"],
        },
    }

    def sponsored_ads(self, brand, marketplace, start, end):
        from ad_api.api import Reports
        from ad_api.base import Marketplaces

        credentials = self._ads_credentials(brand, self._profile_for(brand, marketplace))
        api = Reports(credentials=credentials, marketplace=Marketplaces.NA)
        today = date.today()
        # Besides Amazon's rows, record which days each product's rows cover:
        # Sponsored Display only goes back 60 days, the others 95.
        out = {"ranges": {}}
        for product, spec in self.AD_REPORTS.items():
            product_start = clamp_start(start, product, today)
            if product_start > end:
                continue
            out["ranges"][product] = [product_start.isoformat(), end.isoformat()]
            body = {
                "name": f"{brand.slug} {product} {product_start}..{end}",
                "startDate": product_start.isoformat(),
                "endDate": end.isoformat(),
                "configuration": {
                    "adProduct": spec["adProduct"],
                    "groupBy": ["campaign"],
                    "columns": spec["columns"],
                    "reportTypeId": spec["reportTypeId"],
                    "timeUnit": "DAILY",
                    "format": "GZIP_JSON",
                },
            }
            report_id = api.post_report(body=json.dumps(body)).payload["reportId"]
            out[product] = self._wait_and_download(lambda: api.get_report(reportId=report_id).payload, api)
        return out

    def dsp(self, brand, marketplace, start, end):
        from ad_api.api.dsp.reports import Reports as DspReports
        from ad_api.base import Marketplaces

        advertiser = brand.dsp_advertiser_map().get(marketplace.code)
        if not advertiser:
            raise NotConfigured(f"no DSP advertiser for {marketplace.code}")
        start = clamp_start(start, "DSP", date.today())
        if start > end:
            return []
        api = DspReports(credentials=self._ads_credentials(brand), marketplace=Marketplaces.NA)
        body = {
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "format": "JSON",
            "type": "CAMPAIGN",
            "timeUnit": "DAILY",
            "metrics": ["totalCost", "totalSales", "impressions"],
        }
        report_id = api.post_report(dspAccountId=advertiser, body=json.dumps(body)).payload["reportId"]
        return self._wait_and_download(
            lambda: api.get_report(dspAccountId=advertiser, reportId=report_id).payload, api
        )

    def _wait_and_download(self, poll, api) -> list:
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            report = poll()
            status = report.get("status")
            if status in ("COMPLETED", "SUCCESS"):
                url = report.get("url") or report.get("location")
                return api.download_report(url=url, format="data").payload or []
            if status in ("FAILED", "FAILURE") or time.monotonic() > deadline:
                raise RuntimeError(f"Ads report failed: {report.get('failureReason') or status}")
            time.sleep(self.poll_seconds)


# ---------------------------------------------------------------------------
# Dummy client
# ---------------------------------------------------------------------------


def _days(start: date, end: date):
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


class DummyPerformanceClient:
    """
    Fake data in Amazon's response shapes, consistent with DummyAmazonClient's
    orders (traffic units/sales match the dummy order history).
    """

    name = "dummy"

    def __init__(self):
        self.orders = DummyAmazonClient()

    def effective_start(self, kind: str, start: date) -> date:
        return start

    def list_ads_profiles(self, brand):
        return [
            {"profileId": 1000 + i, "countryCode": m.code, "currencyCode": m.currency,
             "accountInfo": {"type": "seller", "id": f"DUMMY{i}"}}
            for i, m in enumerate(brand.marketplaces.all())
        ]

    def _day_orders(self, brand, marketplace, day):
        catalog = self.orders.catalog(brand)
        units, sales = 0, Decimal("0")
        for order in self.orders._orders_for_day(brand, catalog, day, marketplace.code):
            if order["cancel"]:
                continue
            for line in order["lines"]:
                units += line["qty"]
                sales += line["unit"] * line["qty"]
        return units, sales, len(catalog)

    # Like Amazon, traffic data for a day only arrives about two days later.
    traffic_lag_days = 2

    def traffic(self, brand, marketplace, start, end):
        from apps.reports.periods import report_today

        rows = []
        last_available = report_today() - timedelta(days=self.traffic_lag_days)
        for day in _days(start, min(end, last_available)):
            rng = random.Random(_seed("traffic", brand.slug, marketplace.code, day.isoformat()))
            units, sales, catalog_size = self._day_orders(brand, marketplace, day)
            conversion = rng.uniform(0.11, 0.21)
            sessions = int(units / conversion) if units else rng.randint(5, 30)
            currency = marketplace.currency
            rows.append({
                "date": day.isoformat(),
                "salesByDate": {
                    "orderedProductSales": {"amount": float(sales), "currencyCode": currency},
                    "unitsOrdered": units,
                    "totalOrderItems": units,
                },
                "trafficByDate": {
                    "sessions": sessions,
                    "pageViews": int(sessions * rng.uniform(1.2, 1.6)),
                    "buyBoxPercentage": round(rng.uniform(92, 100), 2),
                    "unitSessionPercentage": round(100 * units / sessions, 2) if sessions else 0,
                    "averageOfferCount": catalog_size * 3 + rng.randint(0, 4),
                },
            })
        return {
            "reportSpecification": {
                "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
                "dataStartTime": start.isoformat(),
                "dataEndTime": end.isoformat(),
                "marketplaceIds": [marketplace.amazon_marketplace_id],
            },
            "salesAndTrafficByDate": rows,
        }

    def subscriptions(self, brand, marketplace, start, end):
        base = 300 + _seed("subs", brand.slug) % 500
        share = 1.0 if marketplace.code == "US" else 0.15
        metrics = []
        for day in _days(start, end):
            rng = random.Random(_seed("subs", brand.slug, marketplace.code, day.isoformat()))
            age = (day - date(2024, 1, 1)).days
            active = int((base + age * 0.9) * share * rng.uniform(0.98, 1.02))
            shipped = int(active / 30 * rng.uniform(0.8, 1.2))
            metrics.append({
                "timeInterval": {"startDate": f"{day}T00:00:00Z", "endDate": f"{day}T23:59:59Z"},
                "activeSubscriptions": active,
                "shippedSubscriptionUnits": shipped,
                "totalSubscriptionsRevenue": round(shipped * rng.uniform(18, 26), 2),
                "currencyCode": marketplace.currency,
            })
        return {"metrics": metrics}

    def sponsored_ads(self, brand, marketplace, start, end):
        out = {"SP": [], "SB": [], "SD": []}
        spend_share = {"SP": 0.65, "SB": 0.22, "SD": 0.13}
        for day in _days(start, end):
            _, sales, _ = self._day_orders(brand, marketplace, day)
            rng = random.Random(_seed("ads", brand.slug, marketplace.code, day.isoformat()))
            budget = float(sales) * rng.uniform(0.10, 0.17)
            for product, share in spend_share.items():
                # Two campaigns per product, so per-campaign rows get summed like real reports.
                for campaign in (1, 2):
                    cost = round(budget * share * (0.6 if campaign == 1 else 0.4), 2)
                    roas = rng.uniform(2.0, 4.0)
                    attributed = round(cost * roas, 2)
                    orders = int(attributed / 28)
                    clicks = int(cost / rng.uniform(0.9, 1.6))
                    row = {"date": day.isoformat(), "campaignId": f"{product}-{campaign}", "cost": cost,
                           "impressions": clicks * rng.randint(40, 90), "clicks": clicks}
                    if product == "SP":
                        row |= {"sales7d": attributed, "purchases7d": orders, "unitsSoldClicks7d": int(orders * 1.2)}
                    else:
                        row |= {"sales": attributed, "purchases": orders, "unitsSold": int(orders * 1.2)}
                    out[product].append(row)
        return out

    def dsp(self, brand, marketplace, start, end):
        if marketplace.code not in brand.dsp_advertiser_map():
            raise NotConfigured(f"no DSP advertiser for {marketplace.code}")
        # Pretend DSP started about five months ago, so there's no last-year data.
        dsp_start = date.today() - timedelta(days=150)
        rows = []
        for day in _days(max(start, dsp_start), end):
            rng = random.Random(_seed("dsp", brand.slug, marketplace.code, day.isoformat()))
            cost = round(rng.uniform(15, 55), 2)
            rows.append({
                "date": day.strftime("%Y-%m-%d"),
                "orderId": "dummy-order",
                "totalCost": cost,
                "totalSales": round(cost * rng.uniform(1.5, 3.5), 2),
                "impressions": rng.randint(20000, 90000),
            })
        return rows
