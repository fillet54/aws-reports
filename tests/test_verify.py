"""The live checks themselves: they must pass on good data and fail on the problems they look for."""

from datetime import date, timedelta
from io import StringIO

import pytest
from django.core.management import CommandError, call_command

from apps.performance.clients import DummyPerformanceClient, NotConfigured
from apps.performance.verify import FAIL, PASS, SKIP, WARN, Verifier
from apps.sales.amazon import DummyAmazonClient
from apps.sales.ingest import ingest_report
from django.utils import timezone

from .conftest import make_report

TODAY = date.today()


def run(brand, orders=None, perf=None, groups=("orders", "traffic", "subscriptions", "ads", "dsp")):
    return Verifier(brand, orders or DummyAmazonClient(), perf or DummyPerformanceClient()).run(groups)


def by_name(results, text, group=None):
    matches = [r for r in results if text in r.name and (group is None or r.group == group)]
    assert matches, f"no check matching {text!r}"
    return matches[0]


def test_dummy_clients_pass_every_check(acme):
    acme.dsp_advertisers = "US=1"
    results = run(acme)
    assert not [r for r in results if r.status == FAIL]
    assert by_name(results, "every column").status == PASS
    assert by_name(results, "seller profile exists for CA").status == PASS
    assert by_name(results, "DSP rows have date", "dsp US").status == PASS
    assert by_name(results, "DSP advertiser configured", "dsp CA").status == SKIP


def test_missing_order_column_fails(acme):
    class NoItemPrice(DummyAmazonClient):
        def fetch_orders_report(self, *args):
            text = super().fetch_orders_report(*args).decode()
            header, rest = text.split("\n", 1)
            return (header.replace("item-price", "item-price-x") + "\n" + rest).encode()

    result = by_name(run(acme, orders=NoItemPrice(), groups=("orders",)), "every column")
    assert result.status == FAIL and "item-price" in result.detail


def test_unknown_amazon_channel_fails_but_non_amazon_only_warns(acme):
    def client_with(channel):
        class Renamed(DummyAmazonClient):
            def fetch_orders_report(self, *args):
                return super().fetch_orders_report(*args).replace(b"Amazon.ca", channel.encode())
        return Renamed()

    assert by_name(run(acme, orders=client_with("Amazon.com.mx"), groups=("orders",)), "sales-channel").status == FAIL
    assert by_name(run(acme, orders=client_with("Non-Amazon"), groups=("orders",)), "sales-channel").status == WARN


def test_missing_role_fails_with_a_hint_and_other_groups_still_run(acme):
    class Forbidden(Exception):
        code = 403

    class NoBrandAnalytics(DummyPerformanceClient):
        def traffic(self, *args):
            raise Forbidden("Access to requested resource is denied.")

    results = run(acme, perf=NoBrandAnalytics(), groups=("traffic", "ads"))
    traffic = by_name(results, "Sales and Traffic report is available", "traffic US")
    assert traffic.status == FAIL and "permission/role" in traffic.detail
    assert by_name(results, "Sponsored ads reports accept", "ads US").status == PASS


def test_missing_traffic_field_fails(acme):
    class NoOffers(DummyPerformanceClient):
        def traffic(self, *args):
            payload = super().traffic(*args)
            for row in payload["salesAndTrafficByDate"]:
                del row["trafficByDate"]["averageOfferCount"]
            return payload

    result = by_name(run(acme, perf=NoOffers(), groups=("traffic",)), "average offer count")
    assert result.status == FAIL and "averageOfferCount" in result.detail


def test_wrong_profile_currency_fails(acme):
    class WrongCurrency(DummyPerformanceClient):
        def list_ads_profiles(self, brand):
            profiles = super().list_ads_profiles(brand)
            profiles[1]["currencyCode"] = "USD"  # CA profile reporting USD
            return profiles

    assert by_name(run(acme, perf=WrongCurrency(), groups=("ads",)), "seller profile exists for CA").status == FAIL


def test_ads_not_connected_is_a_skip(acme):
    class NoAdsToken(DummyPerformanceClient):
        def list_ads_profiles(self, brand):
            raise NotConfigured("no Ads API refresh token")

    result = by_name(run(acme, perf=NoAdsToken(), groups=("ads",)), "Ads API token works")
    assert result.status == SKIP


def test_dsp_rows_without_sales_fail(acme):
    acme.dsp_advertisers = "US=1"

    class OldMetricNames(DummyPerformanceClient):
        def dsp(self, *args):
            return [{"date": "2026-09-29", "totalCost": 10, "sales14d": 30}]

    result = by_name(run(acme, perf=OldMetricNames(), groups=("dsp",)), "DSP rows have date", "dsp US")
    assert result.status == FAIL and "totalSales" in result.detail


def test_cross_check_passes_when_orders_match_amazon(acme, us):
    day = TODAY - timedelta(days=3)

    ingest_report(acme, make_report(
        {"amazon-order-id": "111-1", "asin": "B000000001", "item-price": "50.00", "quantity": "2",
         "purchase-date": f"{day}T18:00:00+00:00"},
    ), source="export", fetched_at=timezone.now())

    class Matching(DummyPerformanceClient):
        def traffic(self, brand, marketplace, start, end):
            return {"salesAndTrafficByDate": [{
                "date": str(day),
                "salesByDate": {"orderedProductSales": {"amount": 50.0}, "unitsOrdered": 2},
                "trafficByDate": {"sessions": 10, "pageViews": 12, "averageOfferCount": 6},
            }]}

    results = run(acme, perf=Matching(), groups=("traffic",))
    assert by_name(results, "Order data agrees", "traffic US").status == PASS


def test_cross_check_warns_when_item_price_looks_like_unit_price(acme, us):
    day = TODAY - timedelta(days=3)
    ingest_report(acme, make_report(
        {"amazon-order-id": "111-1", "asin": "B000000001", "item-price": "25.00", "quantity": "2",
         "purchase-date": f"{day}T18:00:00+00:00"},
    ), source="export", fetched_at=timezone.now())

    class AmazonSaysFifty(DummyPerformanceClient):
        def traffic(self, brand, marketplace, start, end):
            return {"salesAndTrafficByDate": [{
                "date": str(day),
                "salesByDate": {"orderedProductSales": {"amount": 50.0}, "unitsOrdered": 2},
                "trafficByDate": {"sessions": 10, "pageViews": 12, "averageOfferCount": 6},
            }]}

    result = by_name(run(acme, perf=AmazonSaysFifty(), groups=("traffic",)), "Order data agrees", "traffic US")
    assert result.status == WARN and "unit price" in result.detail


def test_command_runs_with_dummy_client_and_saves_responses(acme, tmp_path):
    out = StringIO()
    call_command("verify_amazon", "acme", "--client", "dummy", "--save-dir", str(tmp_path), stdout=out)
    assert "✓ Report has every column the app reads" in out.getvalue()
    assert (tmp_path / "orders-last-2-days.tsv").exists()
    assert (tmp_path / "traffic-US.json").exists()


def test_command_exits_with_error_on_failure(acme, monkeypatch):
    class Broken(DummyAmazonClient):
        def fetch_orders_report(self, *args):
            raise RuntimeError("boom")

    monkeypatch.setattr("apps.performance.management.commands.verify_amazon.get_client", lambda name: Broken())
    with pytest.raises(CommandError, match="failed"):
        call_command("verify_amazon", "acme", "--client", "dummy", "--only", "orders", stdout=StringIO())
