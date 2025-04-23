from datetime import datetime, timezone
from typing import Optional
from sqlalchemy.orm import Session
from models.ProductModel import Product
from models.OrderModel import Order
from models.OrderItemsModel import OrderItem
from models.ReturnsModel import Return as Returns
from models.StoreModel import Store
from schemas.ProductSchema import ProductCreate, ProductUpdate
from sqlalchemy import func, desc
from datetime import datetime
from typing import Literal, Optional
from sqlalchemy import literal

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

    metric_expr_map = {
        "Total Sales": func.sum(OrderItem.price * OrderItem.quantity),
        "Total Orders": func.count(OrderItem.order_item_id),
        "Total Returns": func.count(Returns.return_id),
        "Total Profit": func.sum((OrderItem.price - Product.cost) * OrderItem.quantity),
    }

    if metric not in metric_expr_map:
        raise ValueError(f"Unsupported metric: {metric}")

    metric_expr = metric_expr_map[metric]

    # Add literal column for identifying the metric in frontend
    metric_key_literal = literal(metric).label("metric_key")

    if metric == "Total Returns":
        query = db.query(
            Product.product_id,
            Product.name.label("product_name"),
            Product.brand.label("brand_name"),
            Product.price,
            Product.cost,
            Product.category,
            func.count(Returns.return_id).label("metric_value"),
            metric_key_literal,
        ).join(OrderItem, OrderItem.product_id == Product.product_id
        ).join(Order, Order.order_id == OrderItem.order_id
        ).join(Returns, Returns.order_item_id == OrderItem.order_item_id
        ).filter(Returns.return_date.between(start_date, end_date))
    else:
        query = db.query(
            Product.product_id,
            Product.name.label("product_name"),
            Product.brand.label("brand_name"),
            Product.price,
            Product.cost,
            Product.category,
            metric_expr.label("metric_value"),
            metric_key_literal,
        ).join(OrderItem, OrderItem.product_id == Product.product_id
        ).join(Order, Order.order_id == OrderItem.order_id
        ).filter(Order.order_date.between(start_date, end_date))

    result = (
        query.group_by(
            Product.product_id,
            Product.name,
            Product.brand,
            Product.price,
            Product.cost,
            Product.category,
            # Remove metric_key_literal from GROUP BY since it's a constant
        )
        .order_by(desc("metric_value"))
        .limit(n)
        .all()
    )

    return result

# Get all unique product names
def get_unique_product_names(db: Session, selected_brands: list[str] | None = None):
    query = db.query(Product.name)

    if selected_brands and "all" not in selected_brands:
        query = query.filter(Product.brand.in_(selected_brands))

    return [p[0] for p in query.distinct().all()]


# Get all unique brand names
def get_unique_brand_names(db: Session):
    return [b[0] for b in db.query(Product.brand).distinct().all()]

