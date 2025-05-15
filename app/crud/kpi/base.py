from sqlalchemy.orm import Session
from sqlalchemy import func, distinct, select, case, and_, literal
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Union

def _get_date_ranges(start_date: datetime, end_date: datetime) -> tuple:
    """Calculate previous date range mirroring current range"""
    range_length = (end_date - start_date).days + 1
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=range_length - 1)
    return prev_start, prev_end

def _calculate_percentage_change(current: float, previous: float) -> float:
    """Safely calculate percentage change"""
    return ((current - previous) / previous) * 100 if previous > 0 else 0.0

def _format_trend_data(results: list, group_by: str) -> list:
    """Format time series data for consistent response"""
    return [
        {
            "date": row.bucket.strftime("%Y-%m-%d" if group_by == "day" else "%Y-%m"), 
            "value": row.bucket_total
        }
        for row in results
    ]