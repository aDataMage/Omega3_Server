from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple, Union
from collections import defaultdict
from fastapi import Query
from sqlalchemy.orm import Session
from sqlalchemy import func, case, distinct
from crud.ProductCrud import get_product_by_id
from models.OrderModel import Order
from models.OrderItemsModel import OrderItem
from models.CustomersModel import Customer
from models.ProductModel import Product
from models.StoreModel import Store
from models.ReturnsModel import Return as Returns


# Constants for validation
VALID_SEGMENTS = [
    "age",
    "gender",
    "income_bracket",
    "country",
    "marital_status",
    "education_level",
    "employment_status",
]
VALID_METRICS = [
    "Total Customers",
    "New Customers",
    "Average Revenue per Customer",
    "Repeat Customer Rate",
]
VALID_COMPARISON_LEVELS = ["region", "store", "brand", "product"]


def _get_segment_column(segment_by: str):
    """Get the appropriate segmentation column with binning if needed."""
    if segment_by == "age":
        return case(
            [
                (Customer.age < 25, "18-24"),
                (Customer.age < 35, "25-34"),
                (Customer.age < 50, "35-49"),
                (Customer.age < 65, "50-64"),
            ],
            else_="65+",
        ).label("segment")
    return getattr(Customer, segment_by).label("segment")


# def _apply_query_filters(
#     query,
#     selected_regions: Optional[List[str]] = None,
#     selected_stores: Optional[List[str]] = None,
#     selected_brands: Optional[List[str]] = None,
#     selected_products: Optional[List[str]] = None,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
# ):
#     """Apply common filters to queries."""
#     if start_date and end_date:
#         query = query.filter(Order.order_date.between(start_date, end_date))
#     if selected_regions:
#         query = query.filter(Store.region.in_(selected_regions))
#     if selected_stores:
#         query = query.filter(Store.store_id.in_(selected_stores))
#     if selected_brands:
#         query = query.filter(Product.brand.in_(selected_brands))
#     if selected_products:
#         query = query.filter(Product.product_id.in_(selected_products))
#     return query


# def _build_base_query(
#     db: Session,
#     segment_by: Optional[str] = None,
#     comparison_level: Optional[str] = None,
# ):
#     """Build base query with common joins and selected columns."""
#     query = db.query()

#     if segment_by:
#         query = query.add_columns(_get_segment_column(segment_by))

#     if comparison_level:
#         query = query.add_columns(
#             _get_comparison_column(comparison_level).label("comparison_value")
#         )

#     # Start with Order table and add necessary joins
#     query = query.select_from(Order)
#     query = query.join(OrderItem, OrderItem.order_id == Order.order_id)
#     query = query.join(Product, Product.product_id == OrderItem.product_id)
#     query = query.join(Customer, Customer.customer_id == Order.customer_id)

#     # Only join Store if needed for comparison or filtering
#     if comparison_level in ["region", "store"]:
#         query = query.join(Store, Store.store_id == Order.store_id)

#     return query


# def _apply_query_filters(
#     query: Query,
#     selected_regions: Optional[List[str]] = None,
#     selected_stores: Optional[List[str]] = None,
#     selected_brands: Optional[List[str]] = None,
#     selected_products: Optional[List[str]] = None,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
#     join_tables: bool = True,
# ) -> Query:
#     """Apply common filters to queries."""
#     if join_tables:
#         # Only join Store if needed and not already joined
#         if (selected_regions or selected_stores) and not any(
#             isinstance(j.right, Store) for j in query._join_entities
#         ):
#             query = query.join(Store, Store.store_id == Order.store_id)

#         # Only join Product if needed and not already joined
#         if (selected_brands or selected_products) and not any(
#             isinstance(j.right, Product) for j in query._join_entities
#         ):
#             query = query.join(Product, Product.product_id == OrderItem.product_id)

#     if selected_regions:
#         query = query.filter(Store.region.in_(selected_regions))
#     if selected_stores:
#         query = query.filter(Store.store_id.in_(selected_stores))
#     if selected_brands:
#         query = query.filter(Product.brand.in_(selected_brands))
#     if selected_products:
#         query = query.filter(Product.product_id.in_(selected_products))
#     if start_date and end_date:
#         query = query.filter(Order.order_date.between(start_date, end_date))

#     return query


def _get_metric_expression(db: Session, metric_name: str, start_date, end_date):
    """Get the SQL expression for the requested metric."""
    if metric_name == "Total Customers":
        return func.count(distinct(Order.customer_id)).label("value")
    elif metric_name == "New Customers":
        first_purchases = (
            db.query(
                Order.customer_id,
                func.min(Order.order_date).label("first_purchase_date"),
            )
            .group_by(Order.customer_id)
            .subquery()
        )
        return func.count(
            distinct(
                case(
                    [
                        (
                            first_purchases.c.first_purchase_date.between(
                                start_date, end_date
                            ),
                            Order.customer_id,
                        )
                    ],
                    else_=None,
                )
            )
        ).label("value")
    elif metric_name == "Average Revenue per Customer":
        return (
            func.sum(OrderItem.price * OrderItem.quantity)
            / func.nullif(func.count(distinct(Order.customer_id)), 0)
        ).label("value")
    elif metric_name == "Repeat Customer Rate":
        subq = (
            db.query(Order.customer_id)
            .group_by(Order.customer_id)
            .having(func.count(Order.order_id) > 1)
            .subquery()
        )
        return (
            func.count(distinct(subq.c.customer_id))
            / func.nullif(func.count(distinct(Order.customer_id)), 0)
        ).label("value")
    raise ValueError(f"Invalid metric: {metric_name}")


# def _format_segmented_results(
#     rows: List[Tuple], segment_by: str
# ) -> List[Dict[str, Any]]:
#     """Format segmented query results."""
#     return [{"name": str(row.segment), "value": float(row.value or 0)} for row in rows]


def _format_comparison_results(rows: List[Tuple]) -> Dict[str, List[Dict[str, Any]]]:
    """Format comparison query results by grouping by comparison value."""
    grouped = defaultdict(list)
    for row in rows:
        grouped[row.comparison_value].append(
            {"name": str(row.segment), "value": float(row.value or 0)}
        )
    return [{"compare_value": k, "data": v} for k, v in grouped.items()]


def fetch_segmented_customer_metric(
    db: Session,
    metric_name: str,
    segment_by: str,
    comparison_level: Optional[str] = None,
    selected_regions: Optional[List[str]] = None,
    selected_stores: Optional[List[str]] = None,
    selected_brands: Optional[List[str]] = None,
    selected_products: Optional[List[str]] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Fetch customer metrics segmented by demographic attributes with optional comparison."""
    # Validate inputs
    if segment_by not in VALID_SEGMENTS:
        raise ValueError(f"Invalid segment. Must be one of: {VALID_SEGMENTS}")
    if metric_name not in VALID_METRICS:
        raise ValueError(f"Invalid metric. Must be one of: {VALID_METRICS}")
    if comparison_level and comparison_level not in VALID_COMPARISON_LEVELS:
        raise ValueError(
            f"Invalid comparison level. Must be one of: {VALID_COMPARISON_LEVELS}"
        )

    end_date = end_date or datetime.now(timezone.utc)

    # Build base query with all required joins
    query = _build_base_query(db, segment_by, comparison_level)

    # Handle metric-specific calculations with proper joins
    if metric_name == "New Customers":
        # First purchases subquery with all necessary joins
        first_purchases = (
            db.query(
                Order.customer_id,
                func.min(Order.order_date).label("first_purchase_date"),
            )
            .join(OrderItem, OrderItem.order_id == Order.order_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .join(Customer, Customer.customer_id == Order.customer_id)
            .group_by(Order.customer_id)
            .subquery()
        )

        # Join subquery to main query and add metric calculation
        query = query.join(
            first_purchases, first_purchases.c.customer_id == Customer.customer_id
        ).add_columns(
            func.count(
                distinct(
                    case(
                        [
                            (
                                first_purchases.c.first_purchase_date.between(
                                    start_date, end_date
                                ),
                                first_purchases.c.customer_id,
                            )
                        ],
                        else_=None,
                    )
                )
            ).label("value")
        )
    elif metric_name == "Repeat Customer Rate":
        # Repeat customers subquery with proper joins
        repeat_customers = (
            db.query(Order.customer_id)
            .join(OrderItem, OrderItem.order_id == Order.order_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .join(Customer, Customer.customer_id == Order.customer_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Order.customer_id)
            .having(func.count(Order.order_id) > 1)
            .subquery()
        )
        query = query.add_columns(
            (
                func.count(distinct(repeat_customers.c.customer_id))
                / func.nullif(func.count(distinct(Order.customer_id)), 0)
                * 100
            ).label("value")
        )
    else:
        # For Total Customers and Average Revenue
        query = query.add_columns(
            _get_metric_expression(db, metric_name, start_date, end_date)
        )

    # Apply filters
    query = _apply_query_filters(
        query,
        selected_regions,
        selected_stores,
        selected_brands,
        selected_products,
        start_date,
        end_date,
    )

    # Group by appropriate columns
    group_by = ["segment"]
    if comparison_level:
        group_by.append("comparison_value")
    query = query.group_by(*group_by)

    # Execute query and process results
    rows = query.all()

    # Process results to avoid duplicates
    if comparison_level:
        general_results = {}
        comparison_results = defaultdict(list)

        for row in rows:
            segment = str(row.segment)
            value = float(row.value or 0)

            # Initialize comparison_value
            comparison_value = str(getattr(row, "comparison_value", ""))

            # Special handling for product comparison level
            if comparison_level == "product" and hasattr(row, "comparison_value"):
                product = get_product_by_id(db, row.comparison_value)
                comparison_value = (
                    product.name if product else str(row.comparison_value)
                )

            # Aggregate general results
            if segment in general_results:
                general_results[segment] += value
            else:
                general_results[segment] = value

            # Group comparison results
            if hasattr(row, "comparison_value"):
                comparison_results[comparison_value].append(
                    {"name": segment, "value": value}
                )

        # Format results
        general_results = [{"name": k, "value": v} for k, v in general_results.items()]
        comparison_results = [
            {"compare_value": k, "data": sorted(v, key=lambda x: x["name"])}
            for k, v in comparison_results.items()
        ]
    else:
        general_results = [
            {"name": str(row.segment), "value": float(row.value or 0)} for row in rows
        ]
        comparison_results = []

    return {
        "seg": segment_by,
        "metric": metric_name,
        "general": sorted(general_results, key=lambda x: x["name"]),
        "comparison": comparison_results,
    }


def _build_base_query(
    db: Session,
    segment_by: str,
    comparison_level: Optional[str] = None,
) -> Query:
    """Build base query with proper joins and selected columns."""
    query = db.query()

    # Add segment column
    query = query.add_columns(_get_segment_column(segment_by))

    # Add comparison column if needed
    if comparison_level:
        query = query.add_columns(
            _get_comparison_column(comparison_level).label("comparison_value")
        )

    # Start with Order table and add required joins
    query = query.select_from(Order)
    query = query.join(OrderItem, OrderItem.order_id == Order.order_id)
    query = query.join(Product, Product.product_id == OrderItem.product_id)
    query = query.join(Customer, Customer.customer_id == Order.customer_id)

    # Add store join if needed for comparison or filtering
    if comparison_level in ["region", "store"]:
        query = query.join(Store, Store.store_id == Order.store_id)

    return query


# def _apply_query_filters(
#     query: Query,
#     selected_regions: Optional[List[str]] = None,
#     selected_stores: Optional[List[str]] = None,
#     selected_brands: Optional[List[str]] = None,
#     selected_products: Optional[List[str]] = None,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
# ) -> Query:
#     """Apply filters to query with proper join handling."""
#     # Join Store if needed for region/store filters but not already joined
#     if (selected_regions or selected_stores) and not any(
#         isinstance(j.right, Store) for j in query._join_entities
#     ):
#         query = query.join(Store, Store.store_id == Order.store_id)

#     # Apply filters
#     if selected_regions:
#         query = query.filter(
#             Store.region.in_([r.replace("RegionEnum.", "") for r in selected_regions])
#         )
#     if selected_stores:
#         query = query.filter(Store.store_id.in_(selected_stores))
#     if selected_brands:
#         query = query.filter(Product.brand.in_(selected_brands))
#     if selected_products:
#         query = query.filter(Product.product_id.in_(selected_products))
#     if start_date and end_date:
#         query = query.filter(Order.order_date.between(start_date, end_date))

#     return query


def _format_segmented_results(
    rows: List[Tuple], segment_by: str, include_comparison: bool = False
) -> List[Dict[str, Any]]:
    """Format segmented query results."""
    if include_comparison:
        return [
            {
                "name": str(row.segment),
                "value": float(row.value or 0),
                "comparison_value": str(row.comparison_value)
                if hasattr(row, "comparison_value")
                else None,
            }
            for row in rows
        ]
    return [{"name": str(row.segment), "value": float(row.value or 0)} for row in rows]


def _format_comparison_results(rows: List[Tuple]) -> List[Dict[str, Any]]:
    """Format comparison query results by grouping by comparison value."""
    grouped = defaultdict(list)
    for row in rows:
        if hasattr(row, "comparison_value"):
            grouped[str(row.comparison_value)].append(
                {"name": str(row.segment), "value": float(row.value or 0)}
            )
    return [{"compare_value": k, "data": v} for k, v in grouped.items()]


def fetch_customer_metric_trend(
    db: Session,
    metric_name: str,
    comparison_level: Optional[str] = None,
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    interval: str = "month",
) -> List[Dict[str, Any]]:
    """Fetch time series trend data for customer metrics with proper column selection."""
    # Validate inputs
    if metric_name not in VALID_METRICS:
        raise ValueError(f"Invalid metric. Must be one of: {VALID_METRICS}")
    if comparison_level and comparison_level not in VALID_COMPARISON_LEVELS:
        raise ValueError(
            f"Invalid comparison level. Must be one of: {VALID_COMPARISON_LEVELS}"
        )

    end_date = end_date or datetime.now(timezone.utc)
    if not start_date:
        raise ValueError("start_date must be provided")

    # Build base query with proper joins
    query = db.query(func.date_trunc(interval, Order.order_date).label("period"))

    # Start with Order as the base table
    query = query.select_from(Order)

    # Add required joins with proper join conditions
    query = query.join(OrderItem, OrderItem.order_id == Order.order_id)
    query = query.join(Product, Product.product_id == OrderItem.product_id)
    query = query.join(Customer, Customer.customer_id == Order.customer_id)

    # Add store join if needed
    if comparison_level in ["region", "store"] or selected_regions or selected_stores:
        query = query.join(Store, Store.store_id == Order.store_id)

    # Add comparison column if needed
    if comparison_level:
        comparison_column = _get_comparison_column(comparison_level)
        query = query.add_columns(comparison_column.label("comparison_value"))

    # Handle subqueries with proper joins
    if metric_name == "Repeat Customer Rate":
        repeat_customers = (
            db.query(Order.customer_id)
            .join(OrderItem, OrderItem.order_id == Order.order_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .join(Customer, Customer.customer_id == Order.customer_id)
            .join(Store, Store.store_id == Order.store_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Order.customer_id)
            .having(func.count(Order.order_id) > 1)
            .subquery()
        )
        # Add the calculation to the main query
        query = query.add_columns(
            (
                func.count(distinct(repeat_customers.c.customer_id))
                / func.nullif(func.count(distinct(Order.customer_id)), 0)
                * 100
            ).label("value")
        )
    elif metric_name == "New Customers":
        # First build the first_purchases subquery with proper joins
        first_purchases = (
            db.query(
                Order.customer_id,
                func.min(Order.order_date).label("first_purchase_date"),
            )
            .join(Customer, Customer.customer_id == Order.customer_id)  # Explicit join
            .group_by(Order.customer_id)
            .subquery()
        )

        # Then join this subquery properly in the main query
        query = query.join(
            first_purchases,
            first_purchases.c.customer_id == Customer.customer_id,  # Join condition
        ).add_columns(
            func.count(
                distinct(
                    case(
                        [
                            (
                                first_purchases.c.first_purchase_date.between(
                                    start_date, end_date
                                ),
                                first_purchases.c.customer_id,  # Reference from subquery
                            )
                        ],
                        else_=None,
                    )
                )
            ).label("value")
        )
    else:
        # For Total Customers and Average Revenue per Customer
        query = query.add_columns(
            func.count(distinct(Order.customer_id)).label("value")
            if metric_name == "Total Customers"
            else (
                func.sum(OrderItem.price * OrderItem.quantity)
                / func.nullif(func.count(distinct(Order.customer_id)), 0)
            ).label("value")
        )

    # Apply filters
    if selected_regions:
        query = query.filter(
            Store.region.in_([r.replace("RegionEnum.", "") for r in selected_regions])
        )
    if selected_stores:
        query = query.filter(Store.store_id.in_(selected_stores))
    if selected_brands:
        query = query.filter(Product.brand.in_(selected_brands))
    if selected_products:
        query = query.filter(Product.product_id.in_(selected_products))
    if start_date and end_date:
        query = query.filter(Order.order_date.between(start_date, end_date))

    # Group by appropriate columns
    group_by = ["period"]
    if comparison_level:
        group_by.append("comparison_value")
    query = query.group_by(*group_by)

    # Execute query and process results
    rows = query.all()

    # Rest of your processing logic remains the same...
    if not comparison_level:
        return [
            {
                "name": "general",
                "trend": [
                    {
                        "period": row.period.strftime(
                            "%Y-%m-%d" if interval == "day" else "%Y-%m"
                        )
                        or 0,
                        "value": float(row.value or 0),
                    }
                    for row in rows
                ],
            }
        ]

    trend_data = defaultdict(lambda: defaultdict(float))
    for row in rows:
        period = row.period.strftime("%Y-%m-%d" if interval == "day" else "%Y-%m")
        comp_value = getattr(row, "comparison_value", None)
        comp_value = (
            get_product_by_id(db, comp_value).name
            if comparison_level == "product"
            else str(comp_value)
        )
        value = float(row.value or 0)

        if comp_value is not None:
            trend_data[comp_value][period] = value
            trend_data["__all__"][period] += value

    result = [
        {
            "name": "general",
            "trend": [
                {"period": period, "value": value}
                for period, value in sorted(trend_data["__all__"].items())
            ],
        }
    ]

    result.extend(
        {
            "name": str(comp_value),
            "trend": [
                {"period": period, "value": value}
                for period, value in sorted(trend.items())
            ],
        }
        for comp_value, trend in trend_data.items()
        if comp_value != "__all__"
    )

    return result


# def fetch_customer_metrics(
#     db: Session,
#     comparison_level: str,
#     selected_regions: List[str] = None,
#     selected_stores: List[str] = None,
#     selected_brands: List[str] = None,
#     selected_products: List[str] = None,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
# ) -> List[Dict[str, Any]]:
#     """Fetch all customer metrics with comparison capabilities."""
#     if comparison_level not in VALID_COMPARISON_LEVELS:
#         raise ValueError(
#             f"Invalid comparison level. Must be one of: {VALID_COMPARISON_LEVELS}"
#         )

#     # Get comparison data once
#     comparison_data = fetch_segmented_customer_metric(
#         db=db,
#         metric_name="Total Customers",  # Just to get the base data
#         segment_by="gender",  # Dummy segment since we'll use comparison_level
#         comparison_level=comparison_level,
#         selected_regions=selected_regions,
#         selected_stores=selected_stores,
#         selected_brands=selected_brands,
#         selected_products=selected_products,
#         start_date=start_date,
#         end_date=end_date,
#     )

#     # Reorganize the data by metric
#     results = []
#     for metric_name in VALID_METRICS:
#         comparisons = []
#         total_value = 0.0

#         for comp in comparison_data.get("comparison", []):
#             value = 0.0
#             if metric_name == "Total Customers":
#                 value = sum(item["value"] for item in comp["data"])
#             elif metric_name == "New Customers":
#                 # Implement specific calculation for new customers
#                 pass
#             # Add other metric calculations

#             comparisons.append({"name": comp["compare_value"], "value": value})
#             total_value += value

#         results.append(
#             {
#                 "metric_name": metric_name,
#                 "total_value": total_value,
#                 "comparisons": comparisons,
#             }
#         )

#     return results


def fetch_customer_metrics(
    db: Session,
    comparison_level: str,
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Fetch all customer metrics with comparison capabilities."""
    if comparison_level not in VALID_COMPARISON_LEVELS:
        raise ValueError(
            f"Invalid comparison level. Must be one of: {VALID_COMPARISON_LEVELS}"
        )

    # Get base data for all comparison groups
    comparison_groups = _get_comparison_groups(
        db=db,
        comparison_level=comparison_level,
        selected_regions=selected_regions,
        selected_stores=selected_stores,
        selected_brands=selected_brands,
        selected_products=selected_products,
        start_date=start_date,
        end_date=end_date,
    )

    results = []
    for metric_name in VALID_METRICS:
        metric_data = _calculate_metric(
            db=db,
            metric_name=metric_name,
            comparison_level=comparison_level,
            comparison_groups=comparison_groups,
            selected_regions=selected_regions,
            selected_stores=selected_stores,
            selected_brands=selected_brands,
            selected_products=selected_products,
            start_date=start_date,
            end_date=end_date,
        )
        results.append(metric_data)

    return results


def _get_comparison_groups(
    db: Session,
    comparison_level: str,
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> List[str]:
    """Get distinct comparison groups for the given filters."""
    # Start with a clean query for the comparison column
    query = db.query(distinct(_get_comparison_column(comparison_level)))

    # Determine which tables we need to join
    need_store_join = (
        comparison_level in ["region", "store"] or selected_regions or selected_stores
    )
    need_order_join = start_date is not None or need_store_join

    # Join tables only once and only if needed
    if need_order_join:
        query = query.select_from(Order)

        if need_store_join:
            query = query.join(Store, Store.store_id == Order.store_id)

        # Join Product if needed for brand/product filters
        if selected_brands or selected_products:
            query = query.join(OrderItem, OrderItem.order_id == Order.order_id)
            query = query.join(Product, Product.product_id == OrderItem.product_id)

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
        query = query.filter(Order.order_date.between(start_date, end_date))

    return [str(row[0]) for row in query.all()]


def _calculate_metric(
    db: Session,
    metric_name: str,
    comparison_level: str,
    comparison_groups: List[str],
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Calculate a specific metric with comparisons."""
    comparisons = []
    total_value = Decimal("0.0")

    for group in comparison_groups:
        print(group)
        # Calculate metric value for each comparison group
        if metric_name == "Total Customers":
            value = Decimal(
                str(
                    _count_customers(
                        db,
                        comparison_level,
                        group,
                        selected_regions,
                        selected_stores,
                        selected_brands,
                        selected_products,
                        start_date,
                        end_date,
                    )
                )
            )
        elif metric_name == "New Customers":
            value = Decimal(
                str(
                    _count_new_customers(
                        db,
                        comparison_level,
                        group,
                        selected_regions,
                        selected_stores,
                        selected_brands,
                        selected_products,
                        start_date,
                        end_date,
                    )
                )
            )
        elif metric_name == "Repeat Customer Rate":
            value = Decimal(
                str(
                    _calculate_repeat_rate(
                        db,
                        comparison_level,
                        group,
                        selected_regions,
                        selected_stores,
                        selected_brands,
                        selected_products,
                        start_date,
                        end_date,
                    )
                )
            )
        elif metric_name == "Average Revenue per Customer":
            value = Decimal(
                str(
                    _calculate_avg_revenue(
                        db,
                        comparison_level,
                        group,
                        selected_regions,
                        selected_stores,
                        selected_brands,
                        selected_products,
                        start_date,
                        end_date,
                    )
                )
            )
        else:
            value = Decimal("0.0")
        if comparison_level == "product":
            group = get_product_by_id(db, group).name
        comparisons.append({"name": group, "value": float(value)})
        total_value += value

    return {
        "metric_name": metric_name,
        "total_value": total_value,
        "comparisons": comparisons,
    }


def _count_customers(
    db: Session,
    comparison_level: str,
    comparison_value: str,
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> float:
    """Count distinct customers for a specific comparison group."""
    query = (
        db.query(func.count(distinct(Order.customer_id)))
        .join(Store, Store.store_id == Order.store_id)
        .join(OrderItem, OrderItem.order_id == Order.order_id)
        .join(Product, Product.product_id == OrderItem.product_id)
    )
    query = _apply_comparison_filter(query, comparison_level, comparison_value)
    query = _apply_query_filters(
        query,
        selected_regions,
        selected_stores,
        selected_brands,
        selected_products,
        start_date,
        end_date,
    )
    return float(query.scalar() or 0)


def _count_new_customers(
    db: Session,
    comparison_level: str,
    comparison_value: str,
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> float:
    """Count new customers for a specific comparison group."""

    comparison_value = _handle_enum_column(
        _get_comparison_column(comparison_level), comparison_value
    )

    # First build the base query with all needed joins
    base_query = db.query(
        Order.customer_id,
        func.min(Order.order_date).label("first_purchase_date"),
        _get_comparison_column(comparison_level).label("comp_value"),
    ).join(Store, Store.store_id == Order.store_id)

    # Apply filters to the base query before making it a subquery
    base_query = _apply_query_filters(
        base_query,
        selected_regions,
        selected_stores,
        selected_brands,
        selected_products,
        start_date=None,  # We'll filter dates after grouping
        end_date=None,
        join_tables=False,
    )

    # Create the subquery after applying filters
    first_purchases = (
        base_query.group_by(Order.customer_id)
        .group_by(_get_comparison_column((comparison_level)))
        .subquery()
    )

    # Build the final counting query
    query = db.query(func.count(distinct(first_purchases.c.customer_id)))
    query = query.filter(first_purchases.c.comp_value == comparison_value)

    # Apply date filter to first purchase date
    if start_date and end_date:
        query = query.filter(
            first_purchases.c.first_purchase_date.between(start_date, end_date)
        )

    return float(query.scalar() or 0)


def _calculate_repeat_rate(
    db: Session,
    comparison_level: str,
    comparison_value: str,
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> float:
    """Calculate repeat customer rate for a specific comparison group."""

    comparison_value = _handle_enum_column(
        _get_comparison_column(comparison_level), comparison_value
    )

    # Build base query for all customers
    all_customers_query = db.query(
        Order.customer_id, _get_comparison_column(comparison_level).label("comp_value")
    )

    # Apply filters to the base query
    all_customers_query = _apply_query_filters(
        all_customers_query,
        selected_regions,
        selected_stores,
        selected_brands,
        selected_products,
        start_date,
        end_date,
        join_tables=False,
    )

    # Create subquery after applying filters
    all_customers = all_customers_query.group_by(
        Order.customer_id, "comp_value"
    ).subquery()

    # Build query for repeat customers (same filters)
    repeat_customers_query = db.query(
        Order.customer_id, _get_comparison_column(comparison_level).label("comp_value")
    )

    repeat_customers_query = _apply_query_filters(
        repeat_customers_query,
        selected_regions,
        selected_stores,
        selected_brands,
        selected_products,
        start_date,
        end_date,
        join_tables=False,
    )

    repeat_customers = (
        repeat_customers_query.group_by(Order.customer_id, "comp_value")
        .having(func.count(Order.order_id) > 1)
        .subquery()
    )

    # Count total and repeat customers
    total_count = (
        db.query(func.count(distinct(all_customers.c.customer_id)))
        .filter(all_customers.c.comp_value == comparison_value)
        .scalar()
        or 0
    )

    repeat_count = (
        db.query(func.count(distinct(repeat_customers.c.customer_id)))
        .filter(repeat_customers.c.comp_value == comparison_value)
        .scalar()
        or 0
    )

    return (repeat_count / total_count * 100) if total_count > 0 else 0.0


def _handle_enum_column(column, value: str):
    """Handle enum column values by converting string to proper enum type."""
    if hasattr(column, "type") and hasattr(column.type, "enum_class"):
        enum_class = column.type.enum_class
        # Strip enum class prefix if present (e.g., "RegionEnum.Region1" -> "Region1")
        if isinstance(value, str) and "." in value:
            value = value.split(".")[-1]
        try:
            return enum_class(value)
        except ValueError:
            raise ValueError(f"Invalid enum value: {value}")
    return value


def _calculate_revenue_query(
    db: Session,
    comparison_level: str,
    comparison_value: str,
    start_date: datetime,
    end_date: datetime,
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
) -> Query:
    """Build the revenue query with proper enum and GROUP BY handling."""
    comparison_column = _get_comparison_column(comparison_level)
    comparison_value = _handle_enum_column(comparison_column, comparison_value)

    query = (
        db.query(
            func.sum(OrderItem.price * OrderItem.quantity).label("total_revenue"),
            comparison_column.label("comparison_value"),
        )
        .join(Order, OrderItem.order_id == Order.order_id)
        .join(Store, Store.store_id == Order.store_id)
        .join(Product, Product.product_id == OrderItem.product_id)
        .filter(Order.order_date.between(start_date, end_date))
        .filter(comparison_column == comparison_value)
        .group_by(comparison_column)  # Required for SQL compliance
    )

    # Apply additional filters
    return _apply_query_filters(
        query,
        selected_regions,
        selected_stores,
        selected_brands,
        selected_products,
        start_date,
        end_date,
    )


def _calculate_avg_revenue(
    db: Session,
    comparison_level: str,
    comparison_value: str,
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> float:
    """Calculate average revenue per customer with optimized queries."""
    # Get properly formatted comparison value
    comparison_column = _get_comparison_column(comparison_level)
    comparison_value = _handle_enum_column(comparison_column, comparison_value)

    # Build and execute revenue query
    revenue_query = _calculate_revenue_query(
        db=db,
        comparison_level=comparison_level,
        comparison_value=comparison_value,
        start_date=start_date,
        end_date=end_date,
        selected_regions=selected_regions,
        selected_stores=selected_stores,
        selected_brands=selected_brands,
        selected_products=selected_products,
    )
    revenue_query = revenue_query
    total_revenue = revenue_query.scalar() or 0

    # Build and execute customer count query
    customer_query = (
        db.query(
            func.count(distinct(Order.customer_id)).label("customer_count"),
            comparison_column,
        )
        .filter(Order.order_date.between(start_date, end_date))
        .join(OrderItem, OrderItem.order_id == Order.order_id)
        .join(Store, Store.store_id == Order.store_id)
        .join(Product, Product.product_id == OrderItem.product_id)
        .filter(comparison_column == comparison_value)
        .group_by(comparison_column)
    )

    # Apply same filters to customer query
    customer_query = _apply_query_filters(
        customer_query,
        selected_regions,
        selected_stores,
        selected_brands,
        selected_products,
        start_date,
        end_date,
    )
    customer_count = customer_query.scalar() or 0

    return (total_revenue / customer_count) if customer_count > 0 else 0.0


def _apply_comparison_filter(
    query: Query, comparison_level: str, comparison_value: str
) -> Query:
    """Apply filter for a specific comparison value."""
    if not comparison_level:
        return query

    comparison_column = _get_comparison_column(comparison_level)

    # Handle enum columns generically
    if hasattr(comparison_column, "type") and hasattr(
        comparison_column.type, "enum_class"
    ):
        enum_class = comparison_column.type.enum_class
        # Strip enum class prefix if present (e.g., "RegionEnum.Region1" -> "Region1")
        if "." in comparison_value:
            comparison_value = comparison_value.split(".")[-1]
        try:
            enum_value = enum_class(comparison_value)
            return query.filter(comparison_column == enum_value)
        except ValueError:
            raise ValueError(f"Invalid {comparison_level} value: {comparison_value}")

    # Default case for non-enum comparisons
    return query.filter(comparison_column == comparison_value)


def _apply_query_filters(
    query: Query,
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    join_tables: bool = False,
) -> Query:
    """Apply common filters to a query."""
    if join_tables:
        if selected_regions or selected_stores:
            query = query.join(Order).join(Store, Store.store_id == Order.store_id)
        if selected_brands or selected_products:
            query = query.join(OrderItem).join(
                Product, Product.product_id == OrderItem.product_id
            )

    if selected_regions:
        query = query.filter(Store.region.in_(selected_regions))
    if selected_stores:
        query = query.filter(Store.store_id.in_(selected_stores))
    if selected_brands:
        query = query.filter(Product.brand.in_(selected_brands))
    if selected_products:
        query = query.filter(Product.product_id.in_(selected_products))
    if start_date and end_date:
        query = query.filter(Order.order_date.between(start_date, end_date))

    return query


def _get_comparison_column(level: str):
    """Get the appropriate column for comparison level."""
    if level == "region":
        return Store.region
    elif level == "store":
        return Store.name
    elif level == "brand":
        return Product.brand
    elif level == "product":
        return Product.product_id
    raise ValueError(f"Invalid comparison level: {level}")


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

    return group_by
