from typing import Dict, List, Optional, Union, Any
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct, select, case, and_, not_, exists
from models.OrderModel import Order
from models.OrderItemsModel import OrderItem
from models.CustomersModel import Customer
from models.ProductModel import Product
from models.StoreModel import Store
from models.ReturnsModel import Return as Returns

# Constants for validation
VALID_COMPARISON_LEVELS = ["region", "store", "brand", "product"]
VALID_METRICS = {
    "Total Sales": {
        "expression": func.sum(OrderItem.price * OrderItem.quantity),
        "date_field": Order.order_date,
        "base_model": OrderItem,
    },
    "Total Orders": {
        "expression": func.count(OrderItem.order_item_id),
        "date_field": Order.order_date,
        "base_model": OrderItem,
    },
    "Total Returns": {
        "expression": func.count(Returns.return_id),
        "date_field": Returns.return_date,
        "base_model": Returns,
    },
    "Total Profit": {
        "expression": func.sum((OrderItem.price - Product.cost) * OrderItem.quantity),
        "date_field": Order.order_date,
        "base_model": OrderItem,
    },
}


def _get_comparison_column(comparison_level: str):
    """Get the appropriate column for the comparison level."""
    if comparison_level == "region":
        return Store.region.label("comparison_value")
    elif comparison_level == "store":
        return Store.name.label("comparison_value")
    elif comparison_level == "brand":
        return Product.brand.label("comparison_value")
    elif comparison_level == "product":
        return Product.name.label("comparison_value")
    raise ValueError(f"Invalid comparison level: {comparison_level}")


def _build_base_query(
    db: Session,
    comparison_level: str,
    metric_info: Dict[str, Any],
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    include_date: bool = False,
    date_trunc_unit: str = "month",
):
    """Build the base query with common joins and filters."""
    query = db.query()

    # Add comparison value column
    query = query.add_columns(_get_comparison_column(comparison_level))

    # Add date column if needed for trend data
    if include_date:
        query = query.add_columns(
            func.date_trunc(date_trunc_unit, metric_info["date_field"]).label("date")
        )

    # Add metric value column
    query = query.add_columns(metric_info["expression"].label("metric_value"))

    # Start from the appropriate base model
    query = query.select_from(metric_info["base_model"])

    # Build joins based on metric
    if metric_info["base_model"] == OrderItem:
        query = query.join(Order, Order.order_id == OrderItem.order_id)
        query = query.join(Product, Product.product_id == OrderItem.product_id)
    elif metric_info["base_model"] == Returns:
        query = query.join(OrderItem, OrderItem.order_item_id == Returns.order_item_id)
        query = query.join(Order, Order.order_id == OrderItem.order_id)
        query = query.join(Product, Product.product_id == OrderItem.product_id)

    # Add store join if needed
    if comparison_level in ["region", "store"]:
        query = query.join(Store, Store.store_id == Order.store_id)

    # Apply filters
    if selected_regions:
        query = query.filter(Store.region.in_(selected_regions))
    if selected_stores:
        query = query.filter(Store.store_id.in_(selected_stores))
    if selected_brands:
        query = query.filter(Product.brand.in_(selected_brands))
    if selected_products:
        query = query.filter(Product.product_id.in_(selected_products))
    if start_date and end_date:
        query = query.filter(metric_info["date_field"].between(start_date, end_date))

    return query


def _get_group_by_fields(comparison_level: str, include_date: bool = False):
    """Get the appropriate group by fields based on comparison level."""
    group_by = []
    if comparison_level == "region":
        group_by.append(Store.region)
    elif comparison_level == "store":
        group_by.extend([Store.store_id, Store.name])
    elif comparison_level == "brand":
        group_by.append(Product.brand)
    elif comparison_level == "product":
        group_by.extend([Product.product_id, Product.name])

    if include_date:
        group_by.append(
            func.date_trunc("month", VALID_METRICS["Total Sales"]["date_field"])
        )

    return group_by


def _format_insights_results(
    rows: List, metric: str, include_date: bool = False
) -> Dict[str, List]:
    """Format the query results into summary and trend data."""
    summary = []
    trend = []

    for row in rows:
        result = {
            "comparison_value": row.comparison_value,
            "metric_value": float(row.metric_value or 0),
            "metric_name": metric,
        }

        if hasattr(row, "store_name"):
            result["store_name"] = row.store_name

        # Only include date if it was requested and exists in results
        if include_date and hasattr(row, "date"):
            trend.append(
                {
                    "comparison_value": row.comparison_value,
                    "date": row.date.isoformat(),
                    "metric_value": float(row.metric_value or 0),
                }
            )
        else:
            summary.append(result)

    return {"summary": summary, "trend": trend}


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
    if comparison_level not in VALID_COMPARISON_LEVELS:
        raise ValueError(
            f"Invalid comparison level. Must be one of: {VALID_COMPARISON_LEVELS}"
        )

    if metric not in VALID_METRICS:
        raise ValueError(
            f"Invalid metric. Must be one of: {list(VALID_METRICS.keys())}"
        )

    # Set default end date to now if not provided
    end_date = end_date or datetime.now(timezone.utc)
    metric_info = VALID_METRICS[metric]

    # Get current period results
    current_results = _fetch_insights_data(
        db=db,
        comparison_level=comparison_level,
        metric_info=metric_info,
        selected_regions=selected_regions,
        selected_stores=selected_stores,
        selected_brands=selected_brands,
        selected_products=selected_products,
        start_date=start_date,
        end_date=end_date,
        metric=metric,
    )

    # Calculate previous period results if dates are provided
    if start_date and end_date:
        prev_start = start_date - (end_date - start_date) - timedelta(days=1)
        prev_end = start_date - timedelta(days=1)

        prev_results = _fetch_insights_data(
            db=db,
            comparison_level=comparison_level,
            metric_info=metric_info,
            selected_regions=selected_regions,
            selected_stores=selected_stores,
            selected_brands=selected_brands,
            selected_products=selected_products,
            start_date=prev_start,
            end_date=prev_end,
            metric=metric,
        )

        # Add percentage change to current results
        _add_percentage_change(current_results, prev_results)

    return current_results


def _fetch_insights_data(
    db: Session,
    comparison_level: str,
    metric_info: Dict[str, Any],
    selected_regions: List[str],
    selected_stores: List[str],
    selected_brands: List[str],
    selected_products: List[str],
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    metric: str,
) -> Dict[str, List]:
    """Helper function to fetch data for a specific time period."""
    # Build and execute summary query
    summary_query = _build_base_query(
        db=db,
        comparison_level=comparison_level,
        metric_info=metric_info,
        selected_regions=selected_regions,
        selected_stores=selected_stores,
        selected_brands=selected_brands,
        selected_products=selected_products,
        start_date=start_date,
        end_date=end_date,
    )

    group_by_fields = _get_group_by_fields(comparison_level)
    summary_query = summary_query.group_by(*group_by_fields)
    summary_results = summary_query.all()

    # Build and execute trend query if dates are provided
    trend_results = []
    if start_date and end_date:
        trend_query = _build_base_query(
            db=db,
            comparison_level=comparison_level,
            metric_info=metric_info,
            selected_regions=selected_regions,
            selected_stores=selected_stores,
            selected_brands=selected_brands,
            selected_products=selected_products,
            start_date=start_date,
            end_date=end_date,
            include_date=True,
        )

        # Use the correct date field from metric_info for grouping
        trend_group_by = _get_group_by_fields(comparison_level, include_date=True)
        trend_group_by.append(
            func.date_trunc("month", metric_info["date_field"])
        )  # Use the metric's date field

        trend_query = trend_query.group_by(*trend_group_by)
        trend_query = trend_query.order_by("comparison_value")

        trend_results = trend_query.all()

    return _format_insights_results(
        summary_results + trend_results, metric, include_date=bool(trend_results)
    )


def _add_percentage_change(current_results: Dict, prev_results: Dict):
    """Add percentage change to current results based on previous results."""
    prev_lookup = {item["comparison_value"]: item for item in prev_results["summary"]}

    for current in current_results["summary"]:
        prev_value = prev_lookup.get(current["comparison_value"], {}).get(
            "metric_value", 0
        )
        curr_value = current["metric_value"]

        if prev_value > 0:
            current["percentage_change"] = (
                (curr_value - prev_value) / prev_value
            ) * 100
        else:
            current["percentage_change"] = 0.0 if curr_value == 0 else 100.0
