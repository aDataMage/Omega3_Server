from sqlalchemy.orm import Session
from sqlalchemy import func
from models.OrderModel import Order
from models.OrderItemsModel import OrderItem
from models.CustomersModel import Customer
from models.ProductModel import Product
from models.StoreModel import Store
from models.ReturnsModel import Return as Returns
from datetime import datetime, timezone, timedelta
from typing import Optional
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

    # Previous period total
    prev_total = (
        db.query(func.sum(OrderItem.price * OrderItem.quantity))
        .join(Order)
        .filter(Order.order_date.between(prev_start, prev_end))
        .scalar()
    ) or 0.0

    # Calculate percentage change safely
    if prev_total > 0:
        percentage_change = (total - prev_total) / prev_total * 100
    else:
        percentage_change = 0.0

     # Time series grouped by day (or change to month if range is long)
    group_by = "day" if range_length <= 31 else "month"
    date_trunc_unit = "day" if group_by == "day" else "month"

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
        {"date": row.bucket.strftime("%Y-%m-%d" if group_by == "day" else "%Y-%m"), "value": row.bucket_total}
        for row in time_series
    ]

    return {
        "title": "Total Sales",
        "value": f"${total:,.2f}" if total else "0",
        "percentage_change": f"{percentage_change:.2f}%",
        "trend_data": trend_data,
        "previous_total" : f"${prev_total:,.2f}" if prev_total else "0",
        "current_date_range": f"{start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}",
        "previous_date_range": f"{prev_start.strftime('%b %d, %Y')} to {prev_end.strftime('%b %d, %Y')}",
    }


def get_total_orders(
    db: Session, start_date: datetime, end_date: Optional[datetime] = None
):
    end_date = end_date or datetime.now(timezone.utc)

    # How many months in selected range?
    month_diff = (
        (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1
    )
    month_diff = min(month_diff, 3)

    # Total orders in selected range
    total = (
        db.query(func.count(Order.order_id))
        .filter(Order.order_date.between(start_date, end_date))
        .scalar()
    ) or 0

    # Calculate mirror date range
    range_length = (end_date - start_date).days + 1
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=range_length - 1)

    prev_total = (
        db.query(func.count(Order.order_id))
        .filter(Order.order_date.between(prev_start, prev_end))
        .scalar()
    ) or 0

    percentage_change = (
        ((total - prev_total) / prev_total) * 100 if prev_total > 0 else 0.0
    )
    
    group_by = "day" if range_length <= 31 else "month"
    date_trunc_unit = "day" if group_by == "day" else "month"

    time_series = (
        db.query(
            func.date_trunc(date_trunc_unit, Order.order_date).label("bucket"),
            func.count(Order.order_id).label("bucket_total"),
        )
        .filter(Order.order_date.between(start_date, end_date))
        .group_by("bucket")
        .order_by("bucket")
        .all()
    )

    trend_data = [
        {"date": row.bucket.strftime("%Y-%m-%d" if group_by == "day" else "%Y-%m"), "value": row.bucket_total}
        for row in time_series
    ]
    
    return {
        "title": "Total Orders",
        "value": f"{total:,.0f}" if total else "0",
        "percentage_change": f"{percentage_change:.2f}%",
        "trend_data": trend_data,
        "previous_total" : f"${prev_total:,.0f}" if prev_total else "0",
        "current_date_range": f"{start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}",
        "previous_date_range": f"{prev_start.strftime('%b %d, %Y')} to {prev_end.strftime('%b %d, %Y')}",
   
    }


def get_new_customers(
    db: Session, start_date: datetime, end_date: Optional[datetime] = None
):
    end_date = end_date or datetime.now(timezone.utc)

    # Subquery: each customer's first order date
    subq = (
        db.query(
            Order.customer_id,
            func.min(Order.order_date).label("first_order_date"),
        )
        .group_by(Order.customer_id)
        .subquery()
    )

    # Total new customers in current range
    total = (
        db.query(func.count())
        .filter(subq.c.first_order_date.between(start_date, end_date))
        .scalar()
    ) or 0

    # Calculate mirror date range
    range_length = (end_date - start_date).days + 1
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=range_length - 1)

    prev_total = (
        db.query(func.count())
        .filter(subq.c.first_order_date.between(prev_start, prev_end))
        .scalar()
    ) or 0

    percentage_change = (
        ((total - prev_total) / prev_total) * 100 if prev_total > 0 else 0.0
    )

    # Time series: how many new customers each day
    group_by = "day" if range_length <= 31 else "month"
    date_trunc_unit = "day" if group_by == "day" else "month"

    time_series = (
        db.query(
            func.date_trunc(date_trunc_unit, subq.c.first_order_date).label("bucket"),
            func.count().label("bucket_total"),
        )
        .group_by("bucket")
        .order_by("bucket")
        .all()
    )

    trend_data = [
        {"date": row.bucket.strftime("%Y-%m-%d" if group_by == "day" else "%Y-%m"), "value": row.bucket_total}
        for row in time_series
    ]

    return {
        "title": "New Customers",
        "value": f"{total:,.0f}" if total else "0",
        "percentage_change": f"{percentage_change:.2f}%",
        "trend_data": trend_data,
    }


def get_active_customers(
    db: Session, start_date: datetime, end_date: Optional[datetime] = None
):
    end_date = end_date or datetime.now(timezone.utc)

    # Active customers = distinct customers who placed orders in range
    total = (
        db.query(Order.customer_id)
        .filter(Order.order_date.between(start_date, end_date))
        .distinct()
        .count()
    ) or 0

    # Calculate mirror date range
    range_length = (end_date - start_date).days + 1
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=range_length - 1)

    prev_total = (
        db.query(Order.customer_id)
        .filter(Order.order_date.between(prev_start, prev_end))
        .distinct()
        .count()
    ) or 0

    percentage_change = (
        ((total - prev_total) / prev_total) * 100 if prev_total > 0 else 0.0
    )

    # Time series: count of unique active customers per day
    subq = (
        db.query(
            Order.customer_id,
            func.date_trunc("month", Order.order_date).label("month"),
        )
        .filter(Order.order_date.between(start_date, end_date))
        .distinct()
        .subquery()
    )
    

    trend_data_raw = (
        db.query(
            subq.c.month,
            func.count().label("month_active_customers"),
        )
        .group_by(subq.c.month)
        .order_by(subq.c.month)
        .all()
    )

    trend_data = [
        {"date": row.month.strftime("%Y-%m"), "value": row.month_active_customers}
        for row in trend_data_raw
    ]

    return {
        "title": "Active Customers",
        "value": f"{total:,.0f}" if total else "0",
        "percentage_change": f"{percentage_change:.2f}%",
        "trend_data": trend_data,
    }


def get_total_profit(
    db: Session, start_date: datetime, end_date: Optional[datetime] = None
):
    end_date = end_date or datetime.now(timezone.utc)

    # Total profit in selected range
    total = (
        db.query(func.sum((Product.price - Product.cost) * OrderItem.quantity))
        .select_from(OrderItem)
        .join(Product)
        .join(Order)
        .filter(Order.order_date.between(start_date, end_date))
        .scalar()
    )

    # Calculate mirror date range
    range_length = (end_date - start_date).days + 1
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=range_length - 1)

    # Profit for previous range
    prev_total = (
        db.query(func.sum((Product.price - Product.cost) * OrderItem.quantity))
        .select_from(OrderItem)
        .join(Product)
        .join(Order)
        .filter(Order.order_date.between(prev_start, prev_end))
        .scalar()
    )

    percentage_change = ((total - prev_total) / prev_total) * 100 if prev_total else 0.0
    
    group_by = "day" if range_length <= 31 else "month"
    date_trunc_unit = "day" if group_by == "day" else "month"
    
    time_series = (
        db.query(
            func.date_trunc(date_trunc_unit, Order.order_date).label("bucket"),
            func.sum((Product.price - Product.cost) * OrderItem.quantity).label("bucket_total"),
        )
        .select_from(OrderItem)
        .join(Product)
        .join(Order)
        .filter(Order.order_date.between(start_date, end_date))
        .group_by("bucket")
        .order_by("bucket")
        .all()
    )

    trend_data = [
        {"date": row.bucket.strftime("%Y-%m-%d" if group_by == "day" else "%Y-%m"), "value": row.bucket_total}
        for row in time_series
    ]

    return {
        "title": "Total Profit",
        "value": f"${total:,.2f}" if total else "0",
        "percentage_change": f"{percentage_change:.2f}%",
        "trend_data": trend_data,
        "previous_total" : f"${prev_total:,.2f}" if prev_total else "0",
        "current_date_range": f"{start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}",
        "previous_date_range": f"{prev_start.strftime('%b %d, %Y')} to {prev_end.strftime('%b %d, %Y')}",
   
    }


def get_total_returns(
    db: Session, start_date: datetime, end_date: Optional[datetime] = None
):
    end_date = end_date or datetime.now(timezone.utc)

    total = (
        db.query(func.count(Returns.return_id))
        .filter(Returns.return_date.between(start_date, end_date))
        .scalar()
    )

    # Calculate mirror date range
    range_length = (end_date - start_date).days + 1
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=range_length - 1)

    prev_total = (
        db.query(func.count(Returns.return_id))
        .filter(Returns.return_date.between(prev_start, prev_end))
        .scalar()
    )

    percentage_change = ((total - prev_total) / prev_total) * 100 if prev_total else 0.0

    # Daily trend data for sparkline
    group_by = "day" if range_length <= 31 else "month"
    date_trunc_unit = "day" if group_by == "day" else "month"

    time_series = (
        db.query(
            func.date_trunc(date_trunc_unit, Returns.return_date).label("bucket"),
            func.count(Returns.return_id).label("bucket_total"),
        )
        .filter(Returns.return_date.between(start_date, end_date))
        .group_by("bucket")
        .order_by("bucket")
        .all()
    )

    trend_data = [
        {"date": row.bucket.strftime("%Y-%m-%d" if group_by == "day" else "%Y-%m"), "value": row.bucket_total}
        for row in time_series
    ]

    return {
        "title": "Total Returns",
        "value": f"{total:,.0f}" if total else "0",
        "percentage_change": f"{percentage_change:.2f}%",
        "trend_data": trend_data,
        "previous_total" : f"${prev_total:,.0f}" if prev_total else "0",
        "current_date_range": f"{start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}",
        "previous_date_range": f"{prev_start.strftime('%b %d, %Y')} to {prev_end.strftime('%b %d, %Y')}",
   
    }


def get_all_kpi(db: Session, start_date: datetime, end_date: Optional[datetime] = None):
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)

    end_date = end_date or datetime.now(timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)

    total_sales = get_total_sales(db, start_date, end_date)
    total_orders = get_total_orders(db, start_date, end_date)
    new_customers = get_new_customers(db, start_date, end_date)
    total_profit = get_total_profit(db, start_date, end_date)
    total_returns = get_total_returns(db, start_date, end_date)
    active_customers = get_active_customers(db, start_date, end_date)

    return [
        total_sales,
        total_profit,
        total_orders,
        total_returns,
    ]
