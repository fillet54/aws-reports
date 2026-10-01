"""
Sync traffic, ads, DSP and Subscribe & Save data for a brand.

Each data source runs on its own: a brand without the Brand Analytics role
or without DSP still gets the rest. Each run re-requests a trailing window
because Amazon keeps revising recent days.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone as dt_timezone

from django.utils import timezone

from apps.catalog.models import Brand
from apps.reports.periods import report_today
from apps.sales.models import SyncRun

from . import ingest
from .clients import MAX_RANGE_DAYS, NotConfigured, PerformanceClient, get_performance_client

logger = logging.getLogger(__name__)

# Days re-requested on each regular sync, per source.
TRAILING_DAYS = {
    "traffic": 7,         # finalized after a day or two
    "subscriptions": 7,
    "ads": 14,            # attributed sales keep growing for up to 14 days
    "dsp": 14,
}

STORE = {
    "traffic": ("traffic", ingest.store_traffic),
    "subscriptions": ("subscriptions", ingest.store_subscriptions),
    "ads": ("sponsored_ads", ingest.store_sponsored_ads),
    "dsp": ("dsp", ingest.store_dsp),
}


@dataclass
class PerformanceResult:
    days: dict[str, int] = field(default_factory=dict)
    skipped: dict[str, str] = field(default_factory=dict)
    failed: dict[str, str] = field(default_factory=dict)


def _chunks(start: date, end: date, size: int):
    while start <= end:
        chunk_end = min(start + timedelta(days=size - 1), end)
        yield start, chunk_end
        start = chunk_end + timedelta(days=1)


def sync_performance(
    brand: Brand,
    client: PerformanceClient | None = None,
    *,
    days: int | None = None,
    today: date | None = None,
    kinds: tuple[str, ...] = tuple(STORE),
) -> PerformanceResult:
    """
    Fetch the trailing window for each source (or `days` back, for a backfill)
    through yesterday, and store it.
    """
    client = client or get_performance_client()
    today = today or report_today()
    end = today - timedelta(days=1)
    result = PerformanceResult()
    now = timezone.now()

    for kind in kinds:
        method_name, store = STORE[kind]
        start = end - timedelta(days=(days or TRAILING_DAYS[kind]) - 1)
        start = client.effective_start(kind, start)
        if start > end:
            result.skipped[kind] = "outside the API's lookback window"
            continue

        run = SyncRun.objects.create(
            brand=brand, kind=kind, client=client.name,
            window_start=datetime.combine(start, time.min, tzinfo=dt_timezone.utc),
            window_end=now,
        )
        stored, configured, not_configured = 0, 0, []
        try:
            for marketplace in brand.marketplaces.all():
                try:
                    for chunk_start, chunk_end in _chunks(start, end, MAX_RANGE_DAYS[kind]):
                        payload = getattr(client, method_name)(brand, marketplace, chunk_start, chunk_end)
                        stored += store(
                            brand, marketplace, chunk_start, chunk_end, payload,
                            source=client.name, fetched_at=timezone.now(),
                        )
                    configured += 1
                except NotConfigured as exc:
                    # e.g. DSP set up for the US store only.
                    not_configured.append(f"{marketplace.code}: {exc}")
        except Exception as exc:
            logger.exception("%s sync failed for %s", kind, brand.slug)
            run.status = SyncRun.Status.FAILED
            run.message = str(exc)[:2000]
            run.finished_at = timezone.now()
            run.save()
            result.failed[kind] = str(exc)
            continue

        if not configured:
            run.delete()
            result.skipped[kind] = "; ".join(not_configured) or "no stores"
            continue

        run.status = SyncRun.Status.SUCCESS
        run.new_versions = stored
        run.message = "; ".join(not_configured)
        run.finished_at = timezone.now()
        run.save()
        result.days[kind] = stored

    brand.performance_synced_at = now
    brand.save(update_fields=["performance_synced_at"])
    return result
