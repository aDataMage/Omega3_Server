from datetime import datetime, timezone
from typing import Optional
from sqlalchemy.orm import Session
from app.models.ProductModel import Product
from app.models.OrderModel import Order
from app.models.OrderItemsModel import OrderItem
from app.models.ReturnsModel import Return as Returns
from app.models.StoreModel import Store
from app.schemas.ProductSchema import ProductCreate, ProductUpdate
from sqlalchemy import func, desc
from datetime import datetime
from typing import Literal, Optional

# Create a new product


def create_product(db: Session, product: ProductCreate):
    db_product = Product(
        product_name=product.product_name,
        category=product.category,
        brand=product.brand,
        price=product.price,
        cost=product.cost,
        stock_quantity=product.stock_quantity,
        store_id=product.store_id,
    )
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    return db_product


# Get all products


def get_products(db: Session, skip: int = 0, limit: int = 100):
    return db.query(Product).offset(skip).limit(limit).all()


# Get product by ID


def get_product_by_id(db: Session, product_id: str):
    return db.query(Product).filter(Product.product_id == product_id).first()


# Update a product


def update_product(db: Session, product_id: str, product: ProductUpdate):
    db_product = db.query(Product).filter(Product.product_id == product_id).first()
    if db_product:
        for key, value in product.dict(exclude_unset=True).items():
            setattr(db_product, key, value)
        db.commit()
        db.refresh(db_product)
    return db_product


# Delete a product


def delete_product(db: Session, product_id: str):
    db_product = db.query(Product).filter(Product.product_id == product_id).first()
    if db_product:
        db.delete(db_product)
        db.commit()
    return db_product

def get_top_products_by_metric(
    db: Session,
    metric: str,
    start_date: datetime,
    end_date: Optional[datetime] = None,
    n: int = 10,
):
    end_date = end_date or datetime.now(timezone.utc)

    # Map metric to expression
    metric_expr_map = {
        "Total Sales": func.sum(OrderItem.price * OrderItem.quantity),
        "Total Orders": func.count(OrderItem.order_item_id),
        "Total Returns": func.count(Returns.return_id),
        "Total Profit": func.sum((Product.price - Product.cost) * OrderItem.quantity),
    }

    if metric not in metric_expr_map:
        raise ValueError(f"Unsupported metric: {metric}")

    metric_expr = metric_expr_map[metric]

    query = db.query(
        Product.product_id,
        Product.name,
        metric_expr.label("metric_value"),
    ).join(OrderItem).join(Order).filter(Order.order_date.between(start_date, end_date))

    # If returns metric, join with returns table
    if metric == "total_returns":
        query = (
            db.query(
                Product.product_id,
                Product.name,
                func.count(Returns.return_id).label("metric_value"),
            )
            .select_from(Product)
            .join(OrderItem, OrderItem.product_id == Product.product_id)
            .join(Order, Order.order_id == OrderItem.order_id)
            .join(Returns, Returns.order_item_id == OrderItem.order_item_id)
            .filter(Returns.return_date.between(start_date, end_date))
            .group_by(Product.product_id, Product.name)
            .order_by(func.count(Returns.return_id).desc())
            .limit(n)
        )


    query = (
        query.group_by(Product.product_id)
        .order_by(func.coalesce(metric_expr, 0).desc())
        .limit(n)
    )

    return query.all()
