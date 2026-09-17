"""Business-date helpers: ngày nghiệp vụ 02h00 -> 02h00 hôm sau (giờ VN)."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

VN_OFFSET = timezone(timedelta(hours=7))
CUTOFF_HOUR = 2  # 02h00


def to_vn(dt_utc: datetime) -> datetime:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(VN_OFFSET)


def business_date_of(dt_utc: datetime) -> date:
    """Timestamp UTC -> business_date (date VN, cắt lúc 02h00)."""
    vn = to_vn(dt_utc)
    if vn.hour < CUTOFF_HOUR:
        return (vn - timedelta(days=1)).date()
    return vn.date()


def business_day_bounds(business_date: date) -> tuple[datetime, datetime]:
    """Trả về [start_utc, end_utc) của một business_date."""
    start_vn = datetime(business_date.year, business_date.month, business_date.day,
                       CUTOFF_HOUR, 0, 0, tzinfo=VN_OFFSET)
    end_vn = start_vn + timedelta(days=1)
    return start_vn.astimezone(timezone.utc), end_vn.astimezone(timezone.utc)


def hour_vn(dt_utc: datetime) -> int:
    return to_vn(dt_utc).hour
