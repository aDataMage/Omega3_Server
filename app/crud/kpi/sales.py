import json
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct, select, case, and_
from models.OrderModel import Order
from models.OrderItemsModel import OrderItem
from models.CustomersModel import Customer
from models.ProductModel import Product
from models.StoreModel import Store
from models.ReturnsModel import Return as Returns
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Union, Any
from dateutil.relativedelta import relativedelta


def get_total_sales(
    db: Session, start_date: datetime, end_date: Optional[datetime] = None
):
    end_date = end_date or datetime.now(timezone.utc)

    # Calculate number of months in selected range
    month_diff = (
        (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1
    )
    month_diff = min(month_diff, 3)  # cap at 3 months max

    # Total sales in current range
    total = (
        db.query(func.sum(OrderItem.price * OrderItem.quantity))
        .join(Order)
        .filter(Order.order_date.between(start_date, end_date))
        .scalar()
    ) or 0.0

    # Calculate mirror date range
    range_length = (end_date - start_date).days + 1
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=range_length - 1)

    prev_total = (
        db.query(func.sum(OrderItem.price * OrderItem.quantity))
        .join(Order)
        .filter(Order.order_date.between(prev_start, prev_end))
        .scalar()
    ) or 0.0

    if prev_total > 0:
        percentage_change = (total - prev_total) / prev_total * 100
    else:
        percentage_change = 0.0

    date_trunc_unit = "day" if range_length <= 31 else "month"

    time_series = (
        db.query(
            func.date_trunc(date_trunc_unit, Order.order_date).label("bucket"),
            func.sum(OrderItem.price * OrderItem.quantity).label("bucket_total"),
        )
        .join(Order)
        .filter(Order.order_date.between(start_date, end_date))
        .group_by("bucket")
        .order_by("bucket")
        .all()
    )

    trend_data = [
        {
            "date": row.bucket.strftime(
                "%Y-%m-%d" if date_trunc_unit == "day" else "%Y-%m"
            ),
            "value": row.bucket_total,
        }
        for row in time_series
    ]

    return {
        "title": "Total Sales",
        "value": f"${total:,.2f}" if total else "0",
        "percentage_change": f"{percentage_change:.2f}%",
        "trend_data": trend_data,
        "previous_total": f"${prev_total:,.2f}" if prev_total else "0",
        "current_date_range": f"{start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}",
        "previous_date_range": f"{prev_start.strftime('%b %d, %Y')} to {prev_end.strftime('%b %d, %Y')}",
    }


def handleDateInfo(start_date, end_date):
    pass
