"""Calendar helpers: weeks start on Monday, dates are in REPORT_TIME_ZONE."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from django.conf import settings
from django.utils import timezone

WEEK = "week"
MONTH = "month"
KINDS = (WEEK, MONTH)


def report_today() -> date:
    return timezone.now().astimezone(ZoneInfo(settings.REPORT_TIME_ZONE)).date()


def local_datetime(dt: datetime) -> datetime:
    return dt.astimezone(ZoneInfo(settings.REPORT_TIME_ZONE))


def shift_year(d: date, years: int) -> date:
    try:
        return d.replace(year=d.year + years)
    except ValueError:  # Feb 29
        return d.replace(year=d.year + years, day=28)


@dataclass(frozen=True)
class Period:
    kind: str
    start: date
    end: date

    @classmethod
    def containing(cls, kind: str, day: date) -> "Period":
        if kind == WEEK:
            start = day - timedelta(days=day.weekday())
            return cls(WEEK, start, start + timedelta(days=6))
        start = day.replace(day=1)
        last = calendar.monthrange(day.year, day.month)[1]
        return cls(MONTH, start, day.replace(day=last))

    @property
    def label(self) -> str:
        if self.kind == WEEK:
            return f"Week of {self.start:%b} {self.start.day}, {self.start.year}"
        return f"{self.start:%B %Y}"

    @property
    def range_label(self) -> str:
        return f"{self.start:%b} {self.start.day} – {self.end:%b} {self.end.day}, {self.end.year}"

    def previous(self) -> "Period":
        return Period.containing(self.kind, self.start - timedelta(days=1))

    def next(self) -> "Period":
        return Period.containing(self.kind, self.end + timedelta(days=1))

    def last_year(self) -> "Period":
        if self.kind == WEEK:
            # Same weekday alignment: 52 weeks earlier.
            return Period.containing(WEEK, self.start - timedelta(weeks=52))
        return Period.containing(MONTH, shift_year(self.start, -1))

    def days(self) -> list[date]:
        return [self.start + timedelta(days=i) for i in range((self.end - self.start).days + 1)]

    def clipped_end(self, today: date) -> date:
        """Last day with data so far (for comparing a partial period fairly)."""
        return min(self.end, today)


def pct_change(current, previous) -> float | None:
    if not previous:
        return None
    return float((current - previous) / previous * 100)
