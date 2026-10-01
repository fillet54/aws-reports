from datetime import date, timedelta
from decimal import Decimal

import pytest
from django.urls import reverse
from django.utils import timezone

from apps.performance import dashboard, ingest
from apps.performance.clients import DummyPerformanceClient, NotConfigured, clamp_start
from apps.performance.models import DailyAds, DailySubscriptions, DailyTraffic, DataPull
from apps.performance.sync import sync_performance
from apps.sales.ingest import ingest_report
from apps.sales.metrics import Scope
from apps.sales.models import SyncRun

from .conftest import make_report

YESTERDAY = date(2026, 9, 30)
NOW = timezone.now()


def traffic_payload(*days):
    return {"salesAndTrafficByDate": [
        {"date": d.isoformat(),
         "salesByDate": {"orderedProductSales": {"amount": 100.0, "currencyCode": "USD"}, "unitsOrdered": units},
         "trafficByDate": {"sessions": sessions, "pageViews": sessions * 2, "averageOfferCount": offers, "buyBoxPercentage": 97.5}}
        for d, units, sessions, offers in days
    ]}


# ---------------------------------------------------------------------------
# Parsing and storage
# ---------------------------------------------------------------------------


def test_traffic_is_parsed_and_refetch_replaces_days(acme, us):
    ingest.store_traffic(acme, us, YESTERDAY, YESTERDAY, traffic_payload((YESTERDAY, 10, 100, 6)), source="dummy", fetched_at=NOW)
    ingest.store_traffic(acme, us, YESTERDAY, YESTERDAY, traffic_payload((YESTERDAY, 12, 110, 6)), source="dummy", fetched_at=NOW)

    row = DailyTraffic.objects.get()
    assert (row.units_ordered, row.sessions, row.average_offer_count) == (12, 110, Decimal("6.00"))
    assert DataPull.objects.filter(kind="traffic").count() == 2  # raw responses kept


def test_ad_rows_are_summed_per_day_with_product_specific_columns(acme, us):
    payload = {
        "SP": [
            {"date": "2026-09-30", "cost": 10, "sales7d": 40, "purchases7d": 2, "unitsSoldClicks7d": 3, "impressions": 1000, "clicks": 9},
            {"date": "2026-09-30", "cost": 5.5, "sales7d": 20, "purchases7d": 1, "unitsSoldClicks7d": 1, "impressions": 500, "clicks": 4},
        ],
        "SB": [{"date": "2026-09-30", "cost": 3, "sales": 12, "purchases": 1, "unitsSold": 1}],
        "SD": [],
    }
    ingest.store_sponsored_ads(acme, us, YESTERDAY, YESTERDAY, payload, source="dummy", fetched_at=NOW)
    sp = DailyAds.objects.get(ad_product="SP")
    assert (sp.cost, sp.sales, sp.orders, sp.units, sp.clicks) == (Decimal("15.50"), Decimal("60.00"), 3, 4, 13)
    assert DailyAds.objects.get(ad_product="SB").sales == Decimal("12.00")
    assert not DailyAds.objects.filter(ad_product="SD").exists()


def test_ad_ranges_only_replace_days_the_api_returned(acme, us):
    old = YESTERDAY - timedelta(days=80)
    DailyAds.objects.create(brand=acme, marketplace=us, date=old, ad_product="SD", cost=7, sales=20)
    # Sponsored Display only goes back 60 days, so the client reports a shorter range for it.
    payload = {"SP": [], "SB": [], "SD": [], "ranges": {"SD": [str(YESTERDAY - timedelta(days=59)), str(YESTERDAY)]}}
    ingest.store_sponsored_ads(acme, us, YESTERDAY - timedelta(days=94), YESTERDAY, payload, source="sp_api", fetched_at=NOW)
    assert DailyAds.objects.filter(ad_product="SD", date=old).exists()


def test_dsp_and_subscriptions_parse(acme, us):
    ingest.store_dsp(acme, us, YESTERDAY, YESTERDAY, [
        {"date": "20260930", "totalCost": 100, "totalSales": 250, "impressions": 5000},
        {"date": "2026-09-30", "totalCost": 50, "totalSales": 50},
    ], source="dummy", fetched_at=NOW)
    dsp = DailyAds.objects.get(ad_product="DSP")
    assert (dsp.cost, dsp.sales) == (Decimal("150.00"), Decimal("300.00"))

    ingest.store_subscriptions(acme, us, YESTERDAY, YESTERDAY, {"metrics": [
        {"timeInterval": {"startDate": "2026-09-30T00:00:00Z", "endDate": "2026-09-30T23:59:59Z"},
         "activeSubscriptions": 710, "shippedSubscriptionUnits": 25, "totalSubscriptionsRevenue": 512.4}
    ]}, source="dummy", fetched_at=NOW)
    assert DailySubscriptions.objects.get().active_subscriptions == 710


def test_clamp_start_respects_api_lookbacks():
    today = date(2026, 10, 1)
    assert clamp_start(date(2026, 1, 1), "SP", today) == today - timedelta(days=95)
    assert clamp_start(date(2026, 1, 1), "SD", today) == today - timedelta(days=60)
    assert clamp_start(date(2026, 9, 1), "SD", today) == date(2026, 9, 1)


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------


def test_dummy_sync_stores_every_source_and_skips_unconfigured_dsp(acme):
    client = DummyPerformanceClient()
    client.traffic_lag_days = 0
    result = sync_performance(acme, client, days=10, today=YESTERDAY + timedelta(days=1))
    assert set(result.days) == {"traffic", "subscriptions", "ads"}
    assert "dsp" in result.skipped  # no DSP advertiser configured
    assert DailyTraffic.objects.filter(brand=acme).count() == 20  # 10 days x US and CA
    assert SyncRun.objects.filter(brand=acme, kind="ads", status="success").exists()

    acme.dsp_advertisers = "US=123"
    acme.save()
    result = sync_performance(acme, DummyPerformanceClient(), days=10, today=date.today(), kinds=("dsp",))
    assert result.days["dsp"] > 0
    assert DailyAds.objects.filter(brand=acme, ad_product="DSP", marketplace__code="US").exists()
    assert not DailyAds.objects.filter(brand=acme, ad_product="DSP", marketplace__code="CA").exists()


def test_dummy_traffic_arrives_late_like_amazon(acme, us):
    from apps.reports.periods import report_today

    today = report_today()
    payload = DummyPerformanceClient().traffic(acme, us, today - timedelta(days=5), today - timedelta(days=1))
    days = [row["date"] for row in payload["salesAndTrafficByDate"]]
    assert max(days) == str(today - timedelta(days=2))


def test_one_failing_source_does_not_stop_the_others(acme):
    class NoBrandAnalytics(DummyPerformanceClient):
        def traffic(self, *args):
            raise RuntimeError("403 Unauthorized: Brand Analytics role required")

    result = sync_performance(acme, NoBrandAnalytics(), days=3, today=YESTERDAY + timedelta(days=1))
    assert "traffic" in result.failed and "ads" in result.days
    assert SyncRun.objects.get(brand=acme, kind="traffic").status == "failed"


def test_dsp_config_parsing(acme):
    acme.dsp_advertisers = "US=111, ca = 222"
    assert acme.dsp_advertiser_map() == {"US": "111", "CA": "222"}
    acme.dsp_advertisers = "999"
    assert acme.dsp_advertiser_map() == {"US": "999"}


# ---------------------------------------------------------------------------
# Dashboard numbers
# ---------------------------------------------------------------------------


@pytest.fixture
def filled(acme, us, ca):
    """Yesterday: $200 of US orders + CA$100 (=$73) of Canada orders, plus ads/traffic/S&S."""
    ingest_report(acme, make_report(
        {"amazon-order-id": "111-1", "asin": "B000000001", "item-price": "200.00", "quantity": "4",
         "purchase-date": "2026-09-30T19:00:00+00:00"},
        {"amazon-order-id": "702-1", "asin": "B000000001", "item-price": "100.00", "quantity": "1",
         "purchase-date": "2026-09-30T19:00:00+00:00", "sales-channel": "Amazon.ca", "currency": "CAD"},
    ), source="export", fetched_at=NOW)
    DailyAds.objects.create(brand=acme, marketplace=us, date=YESTERDAY, ad_product="SP", cost=20, sales=80)
    DailyAds.objects.create(brand=acme, marketplace=ca, date=YESTERDAY, ad_product="SB", cost=10, sales=40)
    DailyAds.objects.create(brand=acme, marketplace=us, date=YESTERDAY, ad_product="DSP", cost=5, sales=15)
    for day, sessions in ((YESTERDAY - timedelta(days=1), 50), (YESTERDAY, 40)):
        DailyTraffic.objects.create(brand=acme, marketplace=us, date=day, sessions=sessions, units_ordered=10, average_offer_count=6)
    DailySubscriptions.objects.create(brand=acme, marketplace=us, date=YESTERDAY - timedelta(days=1), active_subscriptions=690)
    DailySubscriptions.objects.create(brand=acme, marketplace=us, date=YESTERDAY, active_subscriptions=710)
    return acme


def cell(data, row_key, col_key):
    col = [c.key for c in data["columns"]].index(col_key)
    return next(r for r in data["rows"] if r.spec.key == row_key).cells[col]


def test_dashboard_values_all_stores_in_usd(filled):
    data = dashboard.build(Scope(brands=(filled,)), YESTERDAY)
    assert cell(data, "sales", "yesterday").value == Decimal("273.00")
    assert cell(data, "units", "yesterday").value == 5
    assert cell(data, "search_ad_cost", "yesterday").value == Decimal("27.30")   # 20 + CA$10 x 0.73
    assert cell(data, "search_ad_sales", "yesterday").value == Decimal("109.20")  # 80 + CA$40 x 0.73
    assert cell(data, "dsp_ad_cost", "yesterday").value == Decimal("5.00")
    assert cell(data, "total_ad_cost", "yesterday").value == Decimal("32.30")
    assert cell(data, "tacos", "yesterday").value.quantize(Decimal("0.01")) == Decimal("11.83")  # 32.30 / 273
    assert cell(data, "sessions", "last7").value == 90
    assert cell(data, "conversion", "last7").value.quantize(Decimal("0.01")) == Decimal("22.22")  # 20 / 90
    assert cell(data, "offer_count", "last7").value == Decimal("6.0")
    assert cell(data, "subscriptions", "last7").value == 710  # end of period, not a sum


def test_dashboard_single_store_keeps_its_currency(filled, ca):
    data = dashboard.build(Scope(brands=(filled,), marketplace=ca), YESTERDAY)
    assert cell(data, "sales", "yesterday").value == Decimal("100.00")
    assert cell(data, "search_ad_cost", "yesterday").value == Decimal("10.00")
    assert cell(data, "sessions", "yesterday").state == dashboard.MISSING  # no CA traffic rows


def test_dashboard_marks_missing_and_partial_sources(filled):
    DailyTraffic.objects.filter(date=YESTERDAY).delete()  # traffic arrives a day late
    data = dashboard.build(Scope(brands=(filled,)), YESTERDAY)
    assert cell(data, "sessions", "yesterday").state == dashboard.MISSING
    assert cell(data, "sessions", "yesterday").value is None
    assert cell(data, "sessions", "last7").state == dashboard.PARTIAL
    assert cell(data, "sessions", "last7").value == 50
    assert cell(data, "sales", "yesterday").state == dashboard.COMPLETE


def test_last_year_comparison_and_na_for_new_sources(filled, us):
    ly = YESTERDAY - timedelta(weeks=52)
    DailyAds.objects.create(brand=filled, marketplace=us, date=ly, ad_product="SP", cost=10, sales=40)
    data = dashboard.build(Scope(brands=(filled,)), YESTERDAY)
    sp = cell(data, "search_ad_cost", "yesterday")
    assert sp.ly_value == Decimal("10.00") and round(sp.change) == 173  # 27.30 vs 10
    dsp = cell(data, "dsp_ad_cost", "yesterday")
    assert dsp.ly_value == Decimal("0") and dsp.change is None  # shown as N/A


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------


def test_performance_pages_for_employee(employee_client, filled):
    assert employee_client.get(reverse("performance:overview")).status_code == 200
    assert employee_client.get(reverse("performance:brand", args=["acme"]) + "?store=CA").status_code == 200


def test_performance_pages_for_client(brand_client, filled, globex):
    assert brand_client.get(reverse("performance:brand", args=["acme"])).status_code == 200
    assert brand_client.get(reverse("performance:brand", args=["globex"])).status_code == 404
    assert brand_client.get(reverse("performance:overview")).status_code == 403


def test_brand_form_stores_ads_token_encrypted(employee_client, acme, us):
    employee_client.post(reverse("catalog:brand_edit", args=["acme"]), {
        "name": "Acme", "slug": "acme", "marketplaces": [us.pk],
        "ads_refresh_token": "Atzr|ads-secret", "dsp_advertisers": "US=555",
    })
    acme.refresh_from_db()
    assert acme.get_ads_refresh_token() == "Atzr|ads-secret"
    assert "ads-secret" not in acme.ads_refresh_token_encrypted
    assert acme.dsp_advertiser_map() == {"US": "555"}


def test_employee_can_trigger_performance_sync(employee_client, acme):
    response = employee_client.post(reverse("catalog:brand_sync_performance", args=["acme"]), follow=True)
    assert response.status_code == 200
    assert DailyTraffic.objects.filter(brand=acme).exists()


def test_not_configured_is_an_exception_type():
    assert issubclass(NotConfigured, Exception)
