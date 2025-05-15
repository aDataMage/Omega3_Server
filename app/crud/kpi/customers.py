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


def _get_new_customers_expression(start_date: datetime, end_date: datetime):
    """Helper function to create SQL expression for counting new customers"""
    if not start_date or not end_date:
        return func.count(distinct(Order.customer_id))
    
    # Subquery to find first purchase date for each customer
    first_purchase_subq = (
        select([
            Order.customer_id,
            func.min(Order.order_date).label('first_purchase_date')
        ])
        .group_by(Order.customer_id)
        .alias('first_purchases')
    )
    
    return func.count(distinct(
        case([
            (and_(
                Order.customer_id == first_purchase_subq.c.customer_id,
                first_purchase_subq.c.first_purchase_date.between(start_date, end_date)
            ), Order.customer_id)
        ])
    ))

def _get_repeat_customer_rate_expression(start_date: datetime, end_date: datetime):
    """Helper function to create SQL expression for repeat customer rate"""
    if not start_date or not end_date:
        return literal(0.0)
    
    # Count of customers who made purchases in the period
    total_customers = func.count(distinct(Order.customer_id))
    
    # Subquery to count orders per customer in the period
    customer_order_counts = (
        select([
            Order.customer_id,
            func.count(Order.order_id).label('order_count')
        ])
        .where(Order.order_date.between(start_date, end_date))
        .group_by(Order.customer_id)
        .alias('customer_counts')
    )
    
    # Count of customers with >1 order in the period
    repeat_customers = func.count(distinct(
        case([
            (customer_order_counts.c.order_count > 1, customer_order_counts.c.customer_id)
        ])
    ))
    
    return case(
        [(total_customers > 0, (repeat_customers / total_customers) * 100)],
        else_=0.0
    )

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
    """
    Fetch all four customer metrics with comparison capabilities.
    Returns a list of metric objects with total values and comparisons.
    """
    # Validate comparison level
    valid_comparison_levels = ['region', 'store', 'brand', 'product']
    if comparison_level not in valid_comparison_levels:
        raise ValueError(f"Invalid comparison level. Must be one of: {valid_comparison_levels}")

    # Set default end date to now if not provided
    end_date = end_date or datetime.now(timezone.utc)

    # Define all customer metrics we want to fetch
    customer_metrics = [
        ("Total Customers", func.count(distinct(Order.customer_id))),
        ("New Customers", _get_new_customers_expression(start_date, end_date)),
        ("Average Revenue per Customer", 
         func.sum(OrderItem.price * OrderItem.quantity) / func.nullif(func.count(distinct(Order.customer_id)), 0)),
        ("Repeat Customer Rate", _get_repeat_customer_rate_expression(start_date, end_date))
    ]

    results = []
    
    for metric_name, metric_expression in customer_metrics:
        # Get total value across all selected dimensions
        total_query = _build_customer_metric_query(
            db=db,
            metric_expression=metric_expression,
            selected_regions=selected_regions,
            selected_stores=selected_stores,
            selected_brands=selected_brands,
            selected_products=selected_products,
            start_date=start_date,
            end_date=end_date
        )
        total_value = total_query.scalar() or 0

        # Get comparison breakdown
        comparison_query = _build_customer_metric_query(
            db=db,
            metric_expression=metric_expression,
            comparison_level=comparison_level,
            selected_regions=selected_regions,
            selected_stores=selected_stores,
            selected_brands=selected_brands,
            selected_products=selected_products,
            start_date=start_date,
            end_date=end_date
        )
        
        # Add group by based on comparison level
        if comparison_level == 'region':
            comparison_query = comparison_query.group_by(Store.region)
        elif comparison_level == 'store':
            comparison_query = comparison_query.group_by(Store.store_id, Store.name)
        elif comparison_level == 'brand':
            comparison_query = comparison_query.group_by(Product.brand)
        elif comparison_level == 'product':
            comparison_query = comparison_query.group_by(Product.name)

        comparison_results = comparison_query.all()

        # Format comparisons
        comparisons = []
        for row in comparison_results:
            comparisons.append({
                "name": row.comparison_value,
                "value": float(row.metric_value or 0)
            })

        # Format the metric result
        results.append({
            "metric_name": metric_name,
            "total_value": float(total_value),
            "comparisons": comparisons
        })

    return results

def _build_customer_metric_query(
    db: Session,
    metric_expression: Any,
    comparison_level: str = None,
    selected_regions: List[str] = None,
    selected_stores: List[str] = None,
    selected_brands: List[str] = None,
    selected_products: List[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
):
    """Helper to build the base query for customer metrics"""
    query = db.query()

    # Add comparison value if needed
    if comparison_level:
        if comparison_level == 'region':
            query = query.add_columns(Store.region.label("comparison_value"))
        elif comparison_level == 'store':
            query = query.add_columns(Store.name.label("comparison_value"))
        elif comparison_level == 'brand':
            query = query.add_columns(Product.brand.label("comparison_value"))
        elif comparison_level == 'product':
            query = query.add_columns(Product.name.label("comparison_value"))
    
    query = query.add_columns(metric_expression.label("metric_value"))

    # Base joins - all customer metrics start from Order
    query = query.select_from(Order)
    query = query.join(OrderItem, OrderItem.order_id == Order.order_id)
    query = query.join(Product, Product.product_id == OrderItem.product_id)
    query = query.join(Customer, Customer.customer_id == Order.customer_id)
    
    # Join with Store only if needed for region/store comparisons or filters
    if (comparison_level in ['region', 'store'] or 
        selected_regions or 
        selected_stores):
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
        query = query.filter(Order.order_date.between(start_date, end_date))


    return query