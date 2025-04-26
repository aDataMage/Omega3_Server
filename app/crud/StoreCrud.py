from datetime import datetime, timezone
from typing import Optional
from sqlalchemy.orm import Session
from models.StoreModel import Store
from schemas.StoreSchema import StoreCreate, StoreUpdate
from models.OrderModel import Order
from models.OrderItemsModel import OrderItem
from models.ReturnsModel import Return as Returns
from models.StoreModel import Store
from sqlalchemy import func, desc, literal, and_
from models.ProductModel import Product
import uuid


def get_all_stores(db: Session):
    return db.query(Store).all()


def create_store(db: Session, store: StoreCreate):
    db_store = Store(
        store_id=str(uuid.uuid4()),
        store_name=store.store_name,
        region=store.region,
        manager_name=store.manager_name,
        opening_date=store.opening_date,
    )
    db.add(db_store)
    db.commit()
    db.refresh(db_store)
    return db_store


def get_store_by_id(db: Session, store_id: str):
    return db.query(Store).filter(Store.store_id == store_id).first()


def update_store(db: Session, store_id: str, store: StoreUpdate):
    db_store = db.query(Store).filter(Store.store_id == store_id).first()
    if db_store:
        if store.store_name:
            db_store.store_name = store.store_name
        if store.region:
            db_store.region = store.region
        if store.manager_name:
            db_store.manager_name = store.manager_name
        if store.opening_date:
            db_store.opening_date = store.opening_date
        db.commit()
        db.refresh(db_store)
    return db_store


def delete_store(db: Session, store_id: str):
    db_store = db.query(Store).filter(Store.store_id == store_id).first()
    if db_store:
        db.delete(db_store)
        db.commit()
        return True
    return False


def get_store_by_name(db: Session, store_name: str):
    return db.query(Store).filter(Store.store_name == store_name).first()


def get_top_stores_by_metric(
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
    metric_key_literal = literal(metric).label("metric_key")

    # Step 1: Create subquery for store metrics
    store_metric_subq = (
        db.query(
            Store.store_id,
            Store.name.label("store_name"),
            Store.manager_name,
            Store.region,
            metric_expr.label("metric_value"),
        )
        .select_from(Store)
        .join(Order, Order.store_id == Store.store_id)
        .join(OrderItem, OrderItem.order_id == Order.order_id)
        .filter(Order.order_date.between(start_date, end_date))
        .group_by(
            Store.store_id,
            Store.name,
            Store.manager_name,
            Store.region,
        )
        .subquery()
    )

    # Step 2: Create subquery for top products per store
    top_product_subq = (
        db.query(
            Order.store_id,
            Product.name.label("top_product_name"),
            func.row_number().over(
                partition_by=Order.store_id,
                order_by=func.sum(OrderItem.quantity).desc()
            ).label("product_rank")
        )
        .select_from(Order)
        .join(OrderItem, OrderItem.order_id == Order.order_id)
        .join(Product, Product.product_id == OrderItem.product_id)
        .filter(Order.order_date.between(start_date, end_date))
        .group_by(Order.store_id, Product.product_id, Product.name)
        .subquery()
    )

    # Step 3: Combine both subqueries
    final_query = (
        db.query(
            store_metric_subq.c.store_id,
            store_metric_subq.c.store_name,
            store_metric_subq.c.manager_name,
            store_metric_subq.c.region,
            top_product_subq.c.top_product_name,
            store_metric_subq.c.metric_value,
            metric_key_literal,
        )
        .select_from(store_metric_subq)
        .join(
            top_product_subq,
           and_(
                top_product_subq.c.store_id == store_metric_subq.c.store_id,
                top_product_subq.c.product_rank == 1
            )
        )
    )

    result = (
        final_query
        .order_by(desc("metric_value"))
        .limit(n)
        .all()
    )

    return result

def get_unique_regions(db: Session):
    return [r[0] for r in db.query(Store.region).distinct().all()]



def get_unique_store_names(db: Session, selected_regions: list[str] | None = None):    
    query = db.query(Store.store_id, Store.name)
    
    if selected_regions and "all" not in selected_regions:
        valid_regions = [r for r in selected_regions if r]

        if valid_regions:
            query = query.filter(Store.region.in_(valid_regions))
    results = query.distinct().order_by(Store.name).all()
    print(f"Debug - Found {len(results)} stores")
    return [{"store_id": str(store_id), "name": name} for store_id, name in results]

