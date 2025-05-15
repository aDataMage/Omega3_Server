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


class StoreCRUD:
    def __init__(self, db: Session):
        self.db = db

    def get_all_stores(self):
        return self.db.query(Store).all()

    def create_store(self, store: StoreCreate):
        db_store = Store(
            store_id=str(uuid.uuid4()),
            store_name=store.store_name,
            region=store.region,
            manager_name=store.manager_name,
            opening_date=store.opening_date,
        )
        self.db.add(db_store)
        self.db.commit()
        self.db.refresh(db_store)
        return db_store

    def get_store_by_id(self, store_id: str):
        return self.db.query(Store).filter(Store.store_id == store_id).first()

    def update_store(self, store_id: str, store: StoreUpdate):
        db_store = self.db.query(Store).filter(Store.store_id == store_id).first()
        if db_store:
            if store.store_name:
                db_store.store_name = store.store_name
            if store.region:
                db_store.region = store.region
            if store.manager_name:
                db_store.manager_name = store.manager_name
            if store.opening_date:
                db_store.opening_date = store.opening_date
            self.db.commit()
            self.db.refresh(db_store)
        return db_store

    def delete_store(self, store_id: str):
        db_store = self.db.query(Store).filter(Store.store_id == store_id).first()
        if db_store:
            self.db.delete(db_store)
            self.db.commit()
            return True
        return False

    def get_store_by_name(self, store_name: str):
        return self.db.query(Store).filter(Store.store_name == store_name).first()

    def get_top_stores_by_metric(
        self,
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
            "Total Profit": func.sum(
                (OrderItem.price - Product.cost) * OrderItem.quantity
            ),
        }

        if metric not in metric_expr_map:
            raise ValueError(f"Unsupported metric: {metric}")

        metric_expr = metric_expr_map[metric]
        metric_key_literal = literal(metric).label("metric_key")

        store_metric_subq = (
            self.db.query(
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

        top_product_subq = (
            self.db.query(
                Order.store_id,
                Product.name.label("top_product_name"),
                func.row_number()
                .over(
                    partition_by=Order.store_id,
                    order_by=func.sum(OrderItem.quantity).desc(),
                )
                .label("product_rank"),
            )
            .select_from(Order)
            .join(OrderItem, OrderItem.order_id == Order.order_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Order.store_id, Product.product_id, Product.name)
            .subquery()
        )

        final_query = (
            self.db.query(
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
                    top_product_subq.c.product_rank == 1,
                ),
            )
        )

        result = final_query.order_by(desc("metric_value")).limit(n).all()

        return result

    def get_unique_regions(self):
        return [r[0] for r in self.db.query(Store.region).distinct().all()]

    def get_unique_store_names(self, selected_regions: list[str] | None = None):
        query = self.db.query(Store.store_id, Store.name)

        if selected_regions and "all" not in selected_regions:
            valid_regions = [r for r in selected_regions if r]

            if valid_regions:
                query = query.filter(Store.region.in_(valid_regions))
        results = query.distinct().order_by(Store.name).all()
        print(f"Debug - Found {len(results)} stores")
        return [{"store_id": str(store_id), "name": name} for store_id, name in results]

    def get_region_table_data(
        self, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        query = (
            self.db.query(
                Store.region,
                func.count(distinct(Store.store_id)).label("store_count"),
                func.sum(OrderItem.price * OrderItem.quantity).label("total_sales"),
                func.sum((OrderItem.price - Product.cost) * OrderItem.quantity).label(
                    "total_profit"
                ),
                func.count(distinct(Order.order_id)).label("total_orders"),
                func.count(distinct(Returns.return_id)).label("total_returns"),
            )
            .join(Order, Order.store_id == Store.store_id)
            .join(OrderItem, OrderItem.order_id == Order.order_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .outerjoin(Returns, Returns.order_id == Order.order_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Store.region)
            .order_by(Store.region)
        )

        return [
            {
                "region": region,
                "store_count": store_count,
                "total_sales": float(total_sales or 0),
                "total_profit": float(total_profit or 0),
                "total_orders": total_orders,
                "total_returns": total_returns,
            }
            for region, store_count, total_sales, total_profit, total_orders, total_returns in query.all()
        ]

    def get_store_table_data(
        self, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        query = (
            self.db.query(
                Store.store_id,
                Store.name.label("store_name"),
                Store.manager_name,
                Store.region,
                func.sum(OrderItem.price * OrderItem.quantity).label("total_sales"),
                func.sum((OrderItem.price - Product.cost) * OrderItem.quantity).label(
                    "total_profit"
                ),
                func.count(distinct(Order.order_id)).label("total_orders"),
                func.count(distinct(Returns.return_id)).label("total_returns"),
            )
            .join(Order, Order.store_id == Store.store_id)
            .join(OrderItem, OrderItem.order_id == Order.order_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .outerjoin(Returns, Returns.order_id == Order.order_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Store.store_id, Store.name, Store.manager_name, Store.region)
            .order_by(Store.name)
        )

        return [
            {
                "store_id": str(store_id),
                "store_name": store_name,
                "manager_name": manager_name,
                "region": region,
                "total_sales": float(total_sales or 0),
                "total_profit": float(total_profit or 0),
                "total_orders": total_orders,
                "total_returns": total_returns,
            }
            for store_id, store_name, manager_name, region, total_sales, total_profit, total_orders, total_returns in query.all()
        ]
