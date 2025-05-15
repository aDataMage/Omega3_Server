from sqlalchemy.orm import Session
from sqlalchemy import func, desc, literal
from models.OrderItemsModel import OrderItem
from models.OrderModel import Order
from models.ProductModel import Product  # Assuming Product is the model
from models.ReturnsModel import Return as Returns
from schemas.ProductSchema import ProductCreate, ProductUpdate
from typing import List, Optional
from datetime import datetime, timezone


class ProductCrud:
    @staticmethod
    def create_product(db: Session, product: ProductCreate):
        db_product = Product(**product.dict())
        db.add(db_product)
        db.commit()
        db.refresh(db_product)
        return db_product

    @staticmethod
    def get_products(db: Session, skip: int = 0, limit: int = 100) -> List[Product]:
        return db.query(Product).offset(skip).limit(limit).all()

    @staticmethod
    def get_product_by_id(db: Session, product_id: str) -> Optional[Product]:
        return db.query(Product).filter(Product.product_id == product_id).first()

    @staticmethod
    def update_product(
        db: Session, product_id: str, product: ProductUpdate
    ) -> Optional[Product]:
        db_product = db.query(Product).filter(Product.product_id == product_id).first()
        if db_product:
            for key, value in product.dict(exclude_unset=True).items():
                setattr(db_product, key, value)
            db.commit()
            db.refresh(db_product)
            return db_product
        return None

    @staticmethod
    def delete_product(db: Session, product_id: str) -> bool:
        db_product = db.query(Product).filter(Product.product_id == product_id).first()
        if db_product:
            db.delete(db_product)
            db.commit()
            return True
        return False

    @staticmethod
    def get_top_products_by_metric(
        db: Session,
        metric: str = "Total Sales",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        n: int = 10,
    ) -> List[Product]:
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

        # Add literal column for identifying the metric in frontend
        metric_key_literal = literal(metric).label("metric_key")

        if metric == "Total Returns":
            query = (
                db.query(
                    Product.product_id,
                    Product.name.label("product_name"),
                    Product.brand.label("brand_name"),
                    Product.price,
                    Product.cost,
                    Product.category,
                    func.count(Returns.return_id).label("metric_value"),
                    metric_key_literal,
                )
                .join(OrderItem, OrderItem.product_id == Product.product_id)
                .join(Order, Order.order_id == OrderItem.order_id)
                .join(Returns, Returns.order_item_id == OrderItem.order_item_id)
                .filter(Returns.return_date.between(start_date, end_date))
            )
        else:
            query = (
                db.query(
                    Product.product_id,
                    Product.name.label("product_name"),
                    Product.brand.label("brand_name"),
                    Product.price,
                    Product.cost,
                    Product.category,
                    metric_expr.label("metric_value"),
                    metric_key_literal,
                )
                .join(OrderItem, OrderItem.product_id == Product.product_id)
                .join(Order, Order.order_id == OrderItem.order_id)
                .filter(Order.order_date.between(start_date, end_date))
            )

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
        return [
            {
                "product_id": row.product_id,
                "product_name": row.product_name,
                "brand_name": row.brand_name,
                "price": row.price,
                "cost": row.cost,
                "category": row.category,
                "metric_value": row.metric_value,
                "metric_key": row.metric_key,
            }
            for row in result
        ]

    @staticmethod
    def get_unique_product_names(
        db: Session, selected_brands: Optional[List[str]] = None
    ) -> List[str]:
        query = db.query(Product.name).distinct()
        if selected_brands:
            query = query.filter(Product.brand.in_(selected_brands))
        return [name[0] for name in query.all()]

    @staticmethod
    def get_unique_brand_names(db: Session) -> List[str]:
        return [brand[0] for brand in db.query(Product.brand).distinct().all()]

    @staticmethod
    def get_brand_table_data(
        db: Session,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        metric: str,
        limit: Optional[int],
        sort: str,
    ) -> List[dict]:
        query = db.query(
            Product.brand,
            func.sum(Product.total_sales).label("total_sales"),
            func.sum(Product.total_orders).label("total_orders"),
            func.sum(Product.total_returns).label("total_returns"),
        )
        if start_date:
            query = query.filter(Product.date >= start_date)
        if end_date:
            query = query.filter(Product.date <= end_date)

        if metric == "Total Sales":
            query = query.group_by(Product.brand).order_by(
                func.sum(Product.total_sales).desc()
                if sort == "desc"
                else func.sum(Product.total_sales).asc()
            )
        elif metric == "Total Orders":
            query = query.group_by(Product.brand).order_by(
                func.sum(Product.total_orders).desc()
                if sort == "desc"
                else func.sum(Product.total_orders).asc()
            )

        return query.limit(limit).all()

    @staticmethod
    def get_product_table_data(
        db: Session,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        metric: str,
        limit: Optional[int],
        sort: str,
    ) -> List[dict]:
        query = db.query(
            Product.name,
            func.sum(Product.total_sales).label("total_sales"),
            func.sum(Product.total_orders).label("total_orders"),
            func.sum(Product.total_returns).label("total_returns"),
        )
        if start_date:
            query = query.filter(Product.date >= start_date)
        if end_date:
            query = query.filter(Product.date <= end_date)

        if metric == "Total Sales":
            query = query.group_by(Product.name).order_by(
                func.sum(Product.total_sales).desc()
                if sort == "desc"
                else func.sum(Product.total_sales).asc()
            )
        elif metric == "Total Orders":
            query = query.group_by(Product.name).order_by(
                func.sum(Product.total_orders).desc()
                if sort == "desc"
                else func.sum(Product.total_orders).asc()
            )

        return query.limit(limit).all()
