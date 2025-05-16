from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, literal, and_, distinct
from models.StoreModel import Store
from schemas.StoreSchema import StoreCreate, StoreUpdate
from models.OrderModel import Order
from models.OrderItemsModel import OrderItem
from models.ReturnsModel import Return as Returns
from models.ProductModel import Product
import uuid


class StoreCrud:
    @staticmethod
    def get_stores(db: Session, skip: int = 0, limit: int = 100) -> List[Store]:
        return db.query(Store).offset(skip).limit(limit).all()

    @staticmethod
    def create_store(db: Session, store: StoreCreate) -> Store:
        db_store = Store(
            store_id=str(uuid.uuid4()),
            name=store.store_name,
            region=store.region,
            manager_name=store.manager_name,
            opening_date=store.opening_date,
        )
        db.add(db_store)
        db.commit()
        db.refresh(db_store)
        return db_store

    @staticmethod
    def get_store_by_id(db: Session, store_id: str) -> Optional[Store]:
        return db.query(Store).filter(Store.store_id == store_id).first()

    @staticmethod
    def update_store(db: Session, store_id: str, store: StoreUpdate) -> Optional[Store]:
        db_store = db.query(Store).filter(Store.store_id == store_id).first()
        if db_store:
            for key, value in store.dict(exclude_unset=True).items():
                setattr(db_store, key, value)
            db.commit()
            db.refresh(db_store)
        return db_store

    @staticmethod
    def delete_store(db: Session, store_id: str) -> bool:
        db_store = db.query(Store).filter(Store.store_id == store_id).first()
        if db_store:
            db.delete(db_store)
            db.commit()
            return True
        return False

    @staticmethod
    def get_top_stores_by_metric(
        db: Session,
        metric: str = "Total Sales",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        n: int = 10,
    ) -> List[dict]:
        end_date = end_date or datetime.now(timezone.utc)

        metric_expr_map = {
            "Total Sales": func.sum(OrderItem.price * OrderItem.quantity),
            "Total Orders": func.count(OrderItem.order_item_id),
            "Total Returns": func.count(Returns.return_id),
            "Total Profit": func.sum(
                (OrderItem.price - Product.cost) * OrderItem.quantity
            ),
        }

        if metric not in metric_expr_map:
            raise ValueError(f"Unsupported metric: {metric}")

        metric_expr = metric_expr_map[metric]
        metric_key_literal = literal(metric).label("metric_key")

        query = (
            db.query(
                Store.store_id,
                Store.name.label("name"),
                Store.is_active,
                Store.manager_name,
                Store.region,
                metric_expr.label("metric_value"),
                metric_key_literal,
                func.max(Product.name).label("top_product"),
            )
            .join(Order, Order.store_id == Store.store_id)
            .join(OrderItem, OrderItem.order_id == Order.order_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .outerjoin(Returns, Returns.order_item_id == OrderItem.order_item_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Store.store_id)
            .order_by(desc("metric_value"))
            .limit(n)
        )

        return [
            {
                "store_id": row.store_id,
                "name": row.name,
                "is_active": row.is_active,
                "manager_name": row.manager_name,
                "region": row.region,
                "metric_value": row.metric_value,
                "metric_key": row.metric_key,
                "top_product": row.top_product,
            }
            for row in query.all()
        ]

    @staticmethod
    def get_unique_regions(db: Session) -> List[str]:
        return [region[0] for region in db.query(Store.region).distinct().all()]

    @staticmethod
    def get_unique_store_names(
        db: Session, selected_regions: Optional[List[str]] = None
    ) -> List[dict]:
        query = db.query(Store.store_id, Store.name)

        if selected_regions:
            query = query.filter(Store.region.in_(selected_regions))

        return [
            {"store_id": str(store_id), "name": name}
            for store_id, name in query.distinct().order_by(Store.name).all()
        ]

    @staticmethod
    def get_region_table_data(
        db: Session, start_date: datetime, end_date: datetime
    ) -> List[dict]:
        # First get the total sales across all regions for percentage calculation
        total_sales_result = (
            db.query(
                func.sum(OrderItem.price * OrderItem.quantity).label("total_sales")
            )
            .join(Order, Order.order_id == OrderItem.order_id)
            .filter(Order.order_date.between(start_date, end_date))
            .first()
        )

        total_sales = total_sales_result[0] or 0  # Handle None case

        # Get base region data with aggregations
        region_data = (
            db.query(
                Store.region.label("region_name"),
                func.sum(OrderItem.price * OrderItem.quantity).label("total_sales"),
                func.sum((OrderItem.price - Product.cost) * OrderItem.quantity).label(
                    "total_profit"
                ),
                func.count(distinct(Order.order_id)).label("total_orders"),
            )
            .select_from(OrderItem)
            .join(Order, Order.order_id == OrderItem.order_id)
            .join(Store, Store.store_id == Order.store_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Store.region)
            .subquery()
        )

        # Get top store per region
        top_stores = (
            db.query(
                Store.region.label("region"),
                Store.name.label("store_name"),
                func.sum(OrderItem.price * OrderItem.quantity).label("store_sales"),
            )
            .join(Order, Order.store_id == Store.store_id)
            .join(OrderItem, OrderItem.order_id == Order.order_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Store.region, Store.name)
            .subquery()
        )

        top_store_per_region = (
            db.query(top_stores.c.region, top_stores.c.store_name)
            .distinct(top_stores.c.region)
            .order_by(top_stores.c.region, desc(top_stores.c.store_sales))
            .subquery()
        )

        # Get top product per region
        top_products = (
            db.query(
                Store.region.label("region"),
                Product.name.label("product_name"),
                func.sum(OrderItem.price * OrderItem.quantity).label("product_sales"),
            )
            .join(Order, Order.store_id == Store.store_id)
            .join(OrderItem, OrderItem.order_id == Order.order_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Store.region, Product.name)
            .subquery()
        )

        top_product_per_region = (
            db.query(top_products.c.region, top_products.c.product_name)
            .distinct(top_products.c.region)
            .order_by(top_products.c.region, desc(top_products.c.product_sales))
            .subquery()
        )

        # Get returns data
        returns_data = (
            db.query(Store.region, func.count(Returns.return_id).label("total_returns"))
            .join(OrderItem, OrderItem.order_item_id == Returns.order_item_id)
            .join(Order, Order.order_id == OrderItem.order_id)
            .join(Store, Store.store_id == Order.store_id)
            .filter(Returns.return_date.between(start_date, end_date))
            .group_by(Store.region)
            .subquery()
        )

        # Combine all data
        final_query = (
            db.query(
                region_data.c.region_name,
                top_store_per_region.c.store_name.label("top_store"),
                top_product_per_region.c.product_name.label("top_product"),
                region_data.c.total_sales,
                region_data.c.total_profit,
                func.coalesce(returns_data.c.total_returns, 0).label("total_returns"),
                region_data.c.total_orders,
            )
            .outerjoin(
                top_store_per_region,
                top_store_per_region.c.region == region_data.c.region_name,
            )
            .outerjoin(
                top_product_per_region,
                top_product_per_region.c.region == region_data.c.region_name,
            )
            .outerjoin(returns_data, returns_data.c.region == region_data.c.region_name)
        )

        results = final_query.all()

        # Format the results with percentage calculations
        formatted_results = []
        for row in results:
            sales_contribution = (
                (row.total_sales / total_sales * 100) if total_sales > 0 else 0
            )

            formatted_results.append(
                {
                    "region_name": row.region_name,
                    "top_store": row.top_store,
                    "top_product": row.top_product,
                    "total_sales": float(row.total_sales or 0),
                    "total_profit": float(row.total_profit or 0),
                    "total_returns": float(row.total_returns or 0),
                    "total_orders": float(row.total_orders or 0),
                    "sales_contribution_percentage": float(sales_contribution or 0),
                }
            )

        return formatted_results

    @staticmethod
    def get_store_table_data(
        db: Session, start_date: datetime, end_date: datetime
    ) -> List[dict]:
        # First get the total sales across all stores for percentage calculation
        total_sales_result = (
            db.query(
                func.sum(OrderItem.price * OrderItem.quantity).label("total_sales")
            )
            .join(Order, Order.order_id == OrderItem.order_id)
            .filter(Order.order_date.between(start_date, end_date))
            .first()
        )

        total_sales = total_sales_result[0] or 0  # Handle None case

        # Get base store data with aggregations
        store_data = (
            db.query(
                Store.store_id,
                Store.name.label("store_name"),
                Store.region,
                func.sum(OrderItem.price * OrderItem.quantity).label("total_sales"),
                func.sum((OrderItem.price - Product.cost) * OrderItem.quantity).label(
                    "total_profit"
                ),
                func.count(distinct(Order.order_id)).label("total_orders"),
            )
            .select_from(OrderItem)
            .join(Order, Order.order_id == OrderItem.order_id)
            .join(Store, Store.store_id == Order.store_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Store.store_id, Store.name, Store.region)
            .subquery()
        )

        # Get top product per store
        top_products = (
            db.query(
                Order.store_id,
                Product.name.label("product_name"),
                func.sum(OrderItem.price * OrderItem.quantity).label("product_sales"),
            )
            .join(OrderItem, OrderItem.order_id == Order.order_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Order.store_id, Product.name)
            .subquery()
        )

        top_product_per_store = (
            db.query(top_products.c.store_id, top_products.c.product_name)
            .distinct(top_products.c.store_id)
            .order_by(top_products.c.store_id, desc(top_products.c.product_sales))
            .subquery()
        )

        # Get returns data per store
        returns_data = (
            db.query(
                Order.store_id, func.count(Returns.return_id).label("total_returns")
            )
            .join(OrderItem, OrderItem.order_item_id == Returns.order_item_id)
            .join(Order, Order.order_id == OrderItem.order_id)
            .filter(Returns.return_date.between(start_date, end_date))
            .group_by(Order.store_id)
            .subquery()
        )

        # Combine all data
        final_query = (
            db.query(
                store_data.c.store_id,
                store_data.c.store_name,
                store_data.c.region,
                top_product_per_store.c.product_name.label("top_product"),
                store_data.c.total_sales,
                store_data.c.total_profit,
                func.coalesce(returns_data.c.total_returns, 0).label("total_returns"),
                store_data.c.total_orders,
            )
            .outerjoin(
                top_product_per_store,
                top_product_per_store.c.store_id == store_data.c.store_id,
            )
            .outerjoin(returns_data, returns_data.c.store_id == store_data.c.store_id)
        )

        results = final_query.all()

        # Format the results with percentage calculations
        formatted_results = []
        for row in results:
            sales_contribution = (
                (row.total_sales / total_sales * 100) if total_sales > 0 else 0
            )

            formatted_results.append(
                {
                    "store_id": row.store_id,
                    "store_name": row.store_name,
                    "region": row.region,
                    "top_product": row.top_product,
                    "total_sales": float(row.total_sales or 0),
                    "total_profit": float(row.total_profit or 0),
                    "total_returns": float(row.total_returns or 0),
                    "total_orders": float(row.total_orders or 0),
                    "sales_contribution_percentage": float(sales_contribution or 0),
                }
            )

        return formatted_results
