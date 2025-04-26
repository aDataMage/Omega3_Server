import json
from sqlalchemy.orm import Session
from sqlalchemy import func
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

def fetch_insights(
    db: Session,
    comparison_level: str,
    metric: str,
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Dict[str, Union[List[Dict[str, Any]], List[Dict[str, Any]]]]:
    """
    Fetch insights data with flexible filtering and comparison capabilities.
    Returns dictionary with 'summary' and 'trend' keys.
    """
    # Validate inputs
    valid_comparison_levels = ['region', 'store', 'brand', 'product']
    if comparison_level not in valid_comparison_levels:
        raise ValueError(f"Invalid comparison level. Must be one of: {valid_comparison_levels}")

    valid_metrics = {
        "Total Sales": func.sum(OrderItem.price * OrderItem.quantity),
        "Total Orders": func.count(OrderItem.order_item_id),
        "Total Returns": func.count(Returns.return_id),
        "Total Profit": func.sum((OrderItem.price - Product.cost) * OrderItem.quantity),
    }
    if metric not in valid_metrics:
        raise ValueError(f"Invalid metric. Must be one of: {list(valid_metrics.keys())}")

    # Set default end date to now if not provided
    end_date = end_date or datetime.now(timezone.utc)
    
    # Get current period results
    current_results = _fetch_insights_data(
        db=db,
        comparison_level=comparison_level,
        metric=metric,
        selected_regions=selected_regions,
        selected_stores=selected_stores,
        selected_brands=selected_brands,
        selected_products=selected_products,
        start_date=start_date,
        end_date=end_date,
        metric_expression=valid_metrics[metric]
    )

    # Calculate previous period results if dates are provided
    if start_date and end_date:
        prev_start = start_date - (end_date - start_date) - timedelta(days=1)
        prev_end = start_date - timedelta(days=1)
        
        prev_results = _fetch_insights_data(
            db=db,
            comparison_level=comparison_level,
            metric=metric,
            selected_regions=selected_regions,
            selected_stores=selected_stores,
            selected_brands=selected_brands,
            selected_products=selected_products,
            start_date=prev_start,
            end_date=prev_end,
            metric_expression=valid_metrics[metric]
        )
        
        # Create a lookup for previous results
        prev_lookup = {item['comparison_value']: item for item in prev_results["summary"]}
        
        # Add percentage change to current results
        for current in current_results["summary"]:
            prev_value = prev_lookup.get(current['comparison_value'], {}).get('metric_value', 0)
            curr_value = current['metric_value']
            
            if prev_value > 0:
                current['percentage_change'] = ((curr_value - prev_value) / prev_value) * 100
            else:
                current['percentage_change'] = 0.0 if curr_value == 0 else 100.0
    
    return current_results

def _fetch_insights_data(
    db: Session,
    comparison_level: str,
    metric: str,
    selected_regions: List[str],
    selected_stores: List[str],
    selected_brands: List[str],
    selected_products: List[str],
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    metric_expression: Any
) -> Dict[str, List]:
    """Helper function to fetch data for a specific time period."""
    # Determine the date field based on metric
    date_field = Returns.return_date if metric == "Total Returns" else Order.order_date
    print(selected_products)
    # Common query setup function
    def build_base_query(query, include_date=False, date_trunc_unit='month'):
        """Build the base query with common joins and filters"""
        # Select fields based on comparison level
        if comparison_level == 'region':
            query = query.add_columns(Store.region.label("comparison_value"))
        elif comparison_level == 'store':
            query = query.add_columns(
                Store.name.label("comparison_value"),
            )
        elif comparison_level == 'brand':
            query = query.add_columns(Product.brand.label("comparison_value"))
        elif comparison_level == 'product':
            query = query.add_columns(Product.name.label("comparison_value"))
        
        if include_date:
            query = query.add_columns(
                func.date_trunc(date_trunc_unit, date_field).label("date")
            )
        
        query = query.add_columns(metric_expression.label("metric_value"))
        
        # Build joins based on metric
        if metric in ["Total Sales", "Total Orders", "Total Profit"]:
            query = query.select_from(OrderItem)
            query = query.join(Order, Order.order_id == OrderItem.order_id)
            query = query.join(Product, Product.product_id == OrderItem.product_id)
            
            if comparison_level in ['region', 'store']:
                query = query.join(Store, Store.store_id == Order.store_id)
        elif metric == "Total Returns":
            query = query.select_from(Returns)
            query = query.join(OrderItem, OrderItem.order_item_id == Returns.order_item_id)
            query = query.join(Order, Order.order_id == OrderItem.order_id)
            query = query.join(Product, Product.product_id == OrderItem.product_id)
            
            if comparison_level in ['region', 'store']:
                query = query.join(Store, Store.store_id == Order.store_id)

        # Apply filters
        if selected_regions:
            query = query.filter(Store.region.in_(selected_regions))
        if selected_stores:
            query = query.filter(Store.store_id.in_(selected_stores))
        if selected_brands:
            query = query.filter(Product.brand.in_(selected_brands))
        if selected_products:
            query = query.filter(Product.name.in_(selected_products))
        if start_date and end_date:
            query = query.filter(date_field.between(start_date, end_date))
            
        return query

    # Build and execute summary query
    summary_query = build_base_query(db.query())
    group_by_fields = []
    if comparison_level == 'region':
        group_by_fields.append(Store.region)
    elif comparison_level == 'store':
        group_by_fields.extend([Store.store_id, Store.name])
    elif comparison_level == 'brand':
        group_by_fields.append(Product.brand)
    elif comparison_level == 'product':
        group_by_fields.append(Product.name)
    
    summary_query = summary_query.group_by(*group_by_fields)
    summary_results = summary_query.all()

    # Format summary results
    formatted_summary = []
    for row in summary_results:
        result = {
            'comparison_value': row.comparison_value,
            'metric_value': float(row.metric_value or 0),
            'metric_name': metric
        }
        if hasattr(row, 'store_name'):
            result['store_name'] = row.store_name
        formatted_summary.append(result)

    # Build and execute trend query
    trend_data = []
    if start_date and end_date:
        trend_query = build_base_query(
            db.query(),
            include_date=True,
            date_trunc_unit='month'  # Or 'month' depending on your needs
        )
        
        trend_group_by = group_by_fields.copy()
        trend_group_by.append(func.date_trunc('month', date_field))  # Must match the SELECT
        trend_query = trend_query.group_by(*trend_group_by)
        
        trend_results = trend_query.all()
        
        for row in trend_results:
            trend_data.append({
                'comparison_value': row.comparison_value,
                'date': row.date.isoformat(),
                'metric_value': float(row.metric_value or 0)
            })

    return {
        "summary": formatted_summary,
        "trend": trend_data
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
