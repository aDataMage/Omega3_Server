from datetime import datetime, timezone
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func
from models.OrderModel import Order
from models.OrderItemsModel import OrderItem
from models.ProductModel import Product
from models.ReturnsModel import Return as Returns
from crud.utils import base


def get_total_sales(
    db: Session, start_date: datetime, end_date: Optional[datetime] = None
) -> Dict[str, Any]:
    """Get total sales KPI with trend data."""
    end_date = end_date or datetime.now(timezone.utc)
    date_range = base._calculate_date_range(start_date, end_date)

    # Current period total
    total = (
        db.query(func.sum(OrderItem.price * OrderItem.quantity))
        .join(Order)
        .filter(Order.order_date.between(start_date, end_date))
        .scalar()
    ) or 0.0

    # Previous period total
    prev_total = (
        db.query(func.sum(OrderItem.price * OrderItem.quantity))
        .join(Order)
        .filter(
            Order.order_date.between(date_range["prev_start"], date_range["prev_end"])
        )
        .scalar()
    ) or 0.0

    # Trend data
    group_by = "day" if date_range["range_length"] <= 31 else "month"
    trend_data = base._calculate_trend_data(
        db=db,
        base_model=Order,
        date_field=Order.order_date,
        value_expression=func.sum(OrderItem.price * OrderItem.quantity),
        start_date=start_date,
        end_date=end_date,
        group_by=group_by,
        required_joins=[
            (OrderItem, OrderItem.order_id == Order.order_id),
        ],
    )

    return base._get_kpi_result(
        db=db,
        title="Total Sales",
        current_value=total,
        previous_value=prev_total,
        value_format=",.2f",
        start_date=start_date,
        end_date=end_date,
        trend_data=trend_data,
        is_currency=True,
    )


def get_total_orders(
    db: Session, start_date: datetime, end_date: Optional[datetime] = None
) -> Dict[str, Any]:
    """Get total orders KPI with trend data."""
    end_date = end_date or datetime.now(timezone.utc)
    date_range = base._calculate_date_range(start_date, end_date)

    # Current period total
    total = (
        db.query(func.count(Order.order_id))
        .filter(Order.order_date.between(start_date, end_date))
        .scalar()
    ) or 0

    # Previous period total
    prev_total = (
        db.query(func.count(Order.order_id))
        .filter(
            Order.order_date.between(date_range["prev_start"], date_range["prev_end"])
        )
        .scalar()
    ) or 0

    # Trend data
    group_by = "day" if date_range["range_length"] <= 31 else "month"

    trend_data = base._calculate_trend_data(
        db=db,
        base_model=Order,
        date_field=Order.order_date,
        value_expression=func.count(Order.order_id),
        start_date=start_date,
        end_date=end_date,
        group_by=group_by,
    )

    return base._get_kpi_result(
        db=db,
        title="Total Orders",
        current_value=total,
        previous_value=prev_total,
        value_format=",.0f",
        start_date=start_date,
        end_date=end_date,
        trend_data=trend_data,
    )


def get_total_profit(
    db: Session, start_date: datetime, end_date: Optional[datetime] = None
) -> Dict[str, Any]:
    """Get total profit KPI with trend data."""
    end_date = end_date or datetime.now(timezone.utc)
    date_range = base._calculate_date_range(start_date, end_date)

    # Current period total
    total = (
        db.query(func.sum((Product.price - Product.cost) * OrderItem.quantity))
        .select_from(OrderItem)
        .join(Product)
        .join(Order)
        .filter(Order.order_date.between(start_date, end_date))
        .scalar()
    ) or 0.0

    # Previous period total
    prev_total = (
        db.query(func.sum((Product.price - Product.cost) * OrderItem.quantity))
        .select_from(OrderItem)
        .join(Product)
        .join(Order)
        .filter(
            Order.order_date.between(date_range["prev_start"], date_range["prev_end"])
        )
        .scalar()
    ) or 0.0

    # Trend data
    group_by = "day" if date_range["range_length"] <= 31 else "month"
    trend_data = base._calculate_trend_data(
        db=db,
        base_model=Order,
        date_field=Order.order_date,
        value_expression=func.sum((Product.price - Product.cost) * OrderItem.quantity),
        start_date=start_date,
        end_date=end_date,
        required_joins=[
            (OrderItem, OrderItem.order_id == Order.order_id),
            (Product, Product.product_id == OrderItem.product_id),
        ],
        group_by=group_by,
    )

    return base._get_kpi_result(
        db=db,
        title="Total Profit",
        current_value=total,
        previous_value=prev_total,
        value_format=",.2f",
        start_date=start_date,
        end_date=end_date,
        trend_data=trend_data,
        is_currency=True,
    )


def get_total_returns(
    db: Session, start_date: datetime, end_date: Optional[datetime] = None
) -> Dict[str, Any]:
    """Get total returns KPI with trend data."""
    end_date = end_date or datetime.now(timezone.utc)
    date_range = base._calculate_date_range(start_date, end_date)

    # Current period total
    total = (
        db.query(func.count(Returns.return_id))
        .filter(Returns.return_date.between(start_date, end_date))
        .scalar()
    ) or 0

    # Previous period total
    prev_total = (
        db.query(func.count(Returns.return_id))
        .filter(
            Returns.return_date.between(
                date_range["prev_start"], date_range["prev_end"]
            )
        )
        .scalar()
    ) or 0

    # Trend data
    group_by = "day" if date_range["range_length"] <= 31 else "month"
    trend_data = base._calculate_trend_data(
        db=db,
        base_model=Returns,
        date_field=Returns.return_date,
        value_expression=func.count(Returns.return_id),
        start_date=start_date,
        end_date=end_date,
        group_by=group_by,
    )

    return base._get_kpi_result(
        db=db,
        title="Total Returns",
        current_value=total,
        previous_value=prev_total,
        value_format=",.0f",
        start_date=start_date,
        end_date=end_date,
        trend_data=trend_data,
    )


def get_all_kpi(db: Session, start_date: datetime, end_date: Optional[datetime] = None):
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)

    end_date = end_date or datetime.now(timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)

    total_sales = get_total_sales(db, start_date, end_date)
    total_orders = get_total_orders(db, start_date, end_date)
    total_profit = get_total_profit(db, start_date, end_date)
    total_returns = get_total_returns(db, start_date, end_date)

    return [
        total_sales,
        total_profit,
        total_orders,
        total_returns,
    ]
