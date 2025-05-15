from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func
from models.OrderItemsModel import OrderItem


def _calculate_date_range(start_date: datetime, end_date: datetime) -> Dict[str, Any]:
    """Calculate common date range metrics used across all KPI functions."""
    month_diff = (
        (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1
    )
    month_diff = min(month_diff, 3)  # cap at 3 months max

    range_length = (end_date - start_date).days + 1
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=range_length - 1)

    return {
        "month_diff": month_diff,
        "range_length": range_length,
        "prev_start": prev_start,
        "prev_end": prev_end,
    }


def _calculate_trend_data(
    db: Session,
    base_model: Any,
    date_field: Any,
    value_expression: Any,
    start_date: datetime,
    end_date: datetime,
    group_by: str = "day",
    required_joins: Optional[List[Tuple[Any, Any]]] = None,
    additional_filters: Optional[List[Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Calculate trend data with properly structured joins to avoid SQL errors.

    Args:
        db: SQLAlchemy session
        base_model: The primary model to query from (e.g., Order)
        date_field: Field to use for date filtering (e.g., Order.order_date)
        value_expression: SQL expression for the metric (e.g., func.sum(OrderItem.price * OrderItem.quantity))
        start_date: Start date for filtering
        end_date: End date for filtering
        group_by: Time period grouping ('day' or 'month')
        required_joins: List of (model, condition) tuples for required joins
        additional_filters: List of additional filter conditions

    Returns:
        List of {date: str, value: float} dictionaries
    """
    date_trunc_unit = "day" if group_by == "day" else "month"

    # Start query from the base model
    query = db.query(
        func.date_trunc(date_trunc_unit, date_field).label("bucket"),
        value_expression.label("bucket_total"),
    ).select_from(base_model)

    # Apply all required joins with their conditions
    if required_joins:
        for join_model, join_condition in required_joins:
            query = query.join(join_model, join_condition)

    # Apply date range filter
    query = query.filter(date_field.between(start_date, end_date))

    # Apply any additional filters
    if additional_filters:
        for filter_cond in additional_filters:
            query = query.filter(filter_cond)

    # Execute and format results
    results = query.group_by("bucket").order_by("bucket").all()

    return [
        {
            "date": row.bucket.strftime("%Y-%m-%d" if group_by == "day" else "%Y-%m"),
            "value": float(row.bucket_total or 0),
        }
        for row in results
    ]


def _get_kpi_result(
    db: Session,
    title: str,
    current_value,
    previous_value,
    value_format: str,
    start_date: datetime,
    end_date: datetime,
    trend_data: List[Dict[str, Any]],
    is_currency: bool = False,
) -> Dict[str, Any]:
    """Generate a standardized KPI result dictionary."""
    # Calculate percentage change
    percentage_change = (
        ((current_value - previous_value) / previous_value * 100)
        if previous_value > 0
        else 0.0
    )

    # Format values
    def format_value(value, fmt):
        if value is None:
            return "0"
        if is_currency:
            return f"${value:{fmt}}"
        return f"{value:{fmt}}"

    date_range = _calculate_date_range(start_date, end_date)

    return {
        "title": title,
        "value": format_value(current_value, value_format),
        "percentage_change": f"{percentage_change:.2f}%",
        "trend_data": trend_data,
        "previous_total": format_value(previous_value, value_format),
        "current_date_range": (
            f"{start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}"
        ),
        "previous_date_range": (
            f"{date_range['prev_start'].strftime('%b %d, %Y')} "
            f"to {date_range['prev_end'].strftime('%b %d, %Y')}"
        ),
    }
