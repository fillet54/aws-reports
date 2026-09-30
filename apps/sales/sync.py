"""Pull order reports from Amazon (or the dummy client) and ingest them."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from django.utils import timezone

from apps.catalog.models import Brand

from .amazon import REPORT_BY_LAST_UPDATE, REPORT_BY_ORDER_DATE, AmazonClient, get_client
from .ingest import ingest_report
from .models import RawReport, SyncRun

logger = logging.getLogger(__name__)

# Amazon limits order report date ranges; keep each request well inside that.
MAX_WINDOW = timedelta(days=30)
# Re-request a little of the previous window so late updates aren't missed.
# Re-fetching is cheap: unchanged orders don't create new versions.
OVERLAP = timedelta(hours=6)
FIRST_SYNC_LOOKBACK = timedelta(days=30)


@dataclass
class SyncResult:
    reports: int = 0
    new_versions: int = 0


def _windows(start: datetime, end: datetime):
    while start < end:
        chunk_end = min(start + MAX_WINDOW, end)
        yield start, chunk_end
        start = chunk_end


def _source_for(client: AmazonClient) -> str:
    return RawReport.Source.DUMMY if client.name == "dummy" else RawReport.Source.SP_API


def sync_brand(
    brand: Brand,
    client: AmazonClient | None = None,
    *,
    now: datetime | None = None,
    since: datetime | None = None,
) -> SyncResult:
    """
    Fetch orders updated since the brand's last successful sync and ingest them.
    """
    client = client or get_client()
    now = now or timezone.now()
    if since is None:
        since = (brand.last_synced_at - OVERLAP) if brand.last_synced_at else now - FIRST_SYNC_LOOKBACK

    run = SyncRun.objects.create(brand=brand, client=client.name, window_start=since, window_end=now)
    result = SyncResult()
    try:
        for start, end in _windows(since, now):
            content = client.fetch_orders_report(brand, start, end, REPORT_BY_LAST_UPDATE)
            ingested = ingest_report(
                brand,
                content,
                source=_source_for(client),
                fetched_at=now,
                report_type=REPORT_BY_LAST_UPDATE,
                data_start=start,
                data_end=end,
            )
            result.reports += 1
            result.new_versions += ingested.new_versions
    except Exception as exc:
        logger.exception("Sync failed for %s", brand.slug)
        run.status = SyncRun.Status.FAILED
        run.message = str(exc)[:2000]
        run.finished_at = timezone.now()
        run.reports, run.new_versions = result.reports, result.new_versions
        run.save()
        raise

    brand.last_synced_at = now
    brand.save(update_fields=["last_synced_at"])
    run.status = SyncRun.Status.SUCCESS
    run.finished_at = timezone.now()
    run.reports, run.new_versions = result.reports, result.new_versions
    run.save()
    return result


def backfill_brand(
    brand: Brand,
    days: int,
    client: AmazonClient | None = None,
    *,
    now: datetime | None = None,
    simulate_history: bool = False,
) -> SyncResult:
    """
    Load order history by purchase date in 30-day chunks.

    With simulate_history (dummy client only), each chunk is "fetched" shortly
    after it ends, as if the app had been syncing all along. That gives the
    as-of history realistic timestamps for demos.
    """
    client = client or get_client()
    now = now or timezone.now()
    result = SyncResult()
    for start, end in _windows(now - timedelta(days=days), now):
        fetched_at = min(end + timedelta(hours=1), now) if simulate_history else now
        if simulate_history and hasattr(client, "now"):
            client.now = fetched_at
            # Re-read the previous chunk's last days so orders that were still
            # pending at its fetch time get their later status recorded.
            start = start - timedelta(days=4)
        content = client.fetch_orders_report(brand, start, end, REPORT_BY_ORDER_DATE)
        ingested = ingest_report(
            brand,
            content,
            source=_source_for(client),
            fetched_at=fetched_at,
            report_type=REPORT_BY_ORDER_DATE,
            data_start=start,
            data_end=end,
        )
        result.reports += 1
        result.new_versions += ingested.new_versions
    if simulate_history and hasattr(client, "now"):
        client.now = None
    brand.last_synced_at = now
    brand.save(update_fields=["last_synced_at"])
    return result
