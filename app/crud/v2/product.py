from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc, literal, distinct
from models.StoreModel import Store
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
        query = db.query(Product.name, Product.product_id).distinct()
        print("Selected Brands:", selected_brands)
        if selected_brands and selected_brands != [""]:
            query = query.filter(Product.brand.in_(selected_brands))
        return [
            {"name": product[0], "product_id": product[1]} for product in query.all()
        ]

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
        # First get the total sales across all brands for percentage calculation
        total_sales_result = (
            db.query(
                func.sum(OrderItem.price * OrderItem.quantity).label("total_sales")
            )
            .join(Order, Order.order_id == OrderItem.order_id)
            .filter(Order.order_date.between(start_date, end_date))
            .first()
        )

        total_sales = total_sales_result[0] or 0  # Handle None case

        # Get base brand data with aggregations
        brand_data = (
            db.query(
                Product.brand,
                func.sum(OrderItem.price * OrderItem.quantity).label("total_sales"),
                func.sum((OrderItem.price - Product.cost) * OrderItem.quantity).label(
                    "total_profit"
                ),
                func.count(distinct(Order.order_id)).label("total_orders"),
            )
            .select_from(OrderItem)
            .join(Order, Order.order_id == OrderItem.order_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Product.brand)
            .subquery()
        )

        # Get top product per brand
        top_products = (
            db.query(
                Product.brand,
                Product.name.label("product_name"),
                func.sum(OrderItem.price * OrderItem.quantity).label("product_sales"),
            )
            .join(OrderItem, OrderItem.product_id == Product.product_id)
            .join(Order, Order.order_id == OrderItem.order_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Product.brand, Product.name)
            .subquery()
        )

        top_product_per_brand = (
            db.query(top_products.c.brand, top_products.c.product_name)
            .distinct(top_products.c.brand)
            .order_by(top_products.c.brand, desc(top_products.c.product_sales))
            .subquery()
        )

        # Get top region per brand
        top_regions = (
            db.query(
                Product.brand,
                Store.region,
                func.sum(OrderItem.price * OrderItem.quantity).label("region_sales"),
            )
            .join(OrderItem, OrderItem.product_id == Product.product_id)
            .join(Order, Order.order_id == OrderItem.order_id)
            .join(Store, Store.store_id == Order.store_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Product.brand, Store.region)
            .subquery()
        )

        top_region_per_brand = (
            db.query(top_regions.c.brand, top_regions.c.region)
            .distinct(top_regions.c.brand)
            .order_by(top_regions.c.brand, desc(top_regions.c.region_sales))
            .subquery()
        )

        # Get returns data per brand
        returns_data = (
            db.query(
                Product.brand, func.count(Returns.return_id).label("total_returns")
            )
            .join(OrderItem, OrderItem.product_id == Product.product_id)
            .join(Returns, Returns.order_item_id == OrderItem.order_item_id)
            .filter(Returns.return_date.between(start_date, end_date))
            .group_by(Product.brand)
            .subquery()
        )

        # Combine all data
        final_query = (
            db.query(
                brand_data.c.brand,
                top_product_per_brand.c.product_name.label("top_product"),
                top_region_per_brand.c.region.label("top_region"),
                brand_data.c.total_sales,
                brand_data.c.total_profit,
                func.coalesce(returns_data.c.total_returns, 0).label("total_returns"),
                brand_data.c.total_orders,
            )
            .outerjoin(
                top_product_per_brand,
                top_product_per_brand.c.brand == brand_data.c.brand,
            )
            .outerjoin(
                top_region_per_brand, top_region_per_brand.c.brand == brand_data.c.brand
            )
            .outerjoin(returns_data, returns_data.c.brand == brand_data.c.brand)
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
                    "brand_name": row.brand.value
                    if hasattr(row.brand, "value")
                    else row.brand,  # Handle enum if needed
                    "top_product": row.top_product,
                    "top_region": row.top_region,
                    "total_sales": row.total_sales,
                    "total_profit": row.total_profit,
                    "total_returns": row.total_returns,
                    "total_orders": row.total_orders,
                    "sales_contribution_percentage": round(sales_contribution, 2),
                }
            )

        return formatted_results

    @staticmethod
    def get_product_table_data(
        db: Session,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        metric: str,
        limit: Optional[int],
        sort: str,
    ) -> List[dict]:
        total_sales_result = (
            db.query(
                func.sum(OrderItem.price * OrderItem.quantity).label("total_sales")
            )
            .join(Order, Order.order_id == OrderItem.order_id)
            .filter(Order.order_date.between(start_date, end_date))
            .first()
        )

        total_sales = total_sales_result[0] or 0  # Handle None case

        # Get base product data with aggregations
        product_data = (
            db.query(
                Product.product_id,
                Product.name.label("product_name"),
                Product.category,
                Product.brand,
                Product.cost,
                Product.stock_quantity,
                func.sum(OrderItem.price * OrderItem.quantity).label("total_sales"),
                func.sum((OrderItem.price - Product.cost) * OrderItem.quantity).label(
                    "total_profit"
                ),
                func.count(distinct(Order.order_id)).label("total_orders"),
            )
            .select_from(OrderItem)
            .join(Order, Order.order_id == OrderItem.order_id)
            .join(Product, Product.product_id == OrderItem.product_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(
                Product.product_id,
                Product.name,
                Product.category,
                Product.brand,
                Product.cost,
                Product.stock_quantity,
            )
            .subquery()
        )

        # Get top region per product
        top_regions = (
            db.query(
                Product.product_id,
                Store.region,
                func.sum(OrderItem.price * OrderItem.quantity).label("region_sales"),
            )
            .join(OrderItem, OrderItem.product_id == Product.product_id)
            .join(Order, Order.order_id == OrderItem.order_id)
            .join(Store, Store.store_id == Order.store_id)
            .filter(Order.order_date.between(start_date, end_date))
            .group_by(Product.product_id, Store.region)
            .subquery()
        )

        top_region_per_product = (
            db.query(top_regions.c.product_id, top_regions.c.region)
            .distinct(top_regions.c.product_id)
            .order_by(top_regions.c.product_id, desc(top_regions.c.region_sales))
            .subquery()
        )

        # Get returns data per product
        returns_data = (
            db.query(
                Product.product_id, func.count(Returns.return_id).label("total_returns")
            )
            .join(OrderItem, OrderItem.product_id == Product.product_id)
            .join(Returns, Returns.order_item_id == OrderItem.order_item_id)
            .filter(Returns.return_date.between(start_date, end_date))
            .group_by(Product.product_id)
            .subquery()
        )

        # Combine all data
        final_query = (
            db.query(
                product_data.c.product_id,
                product_data.c.product_name,
                product_data.c.category,
                product_data.c.brand,
                product_data.c.cost,
                product_data.c.stock_quantity,
                product_data.c.total_sales,
                product_data.c.total_profit,
                func.coalesce(returns_data.c.total_returns, 0).label("total_returns"),
                product_data.c.total_orders,
                top_region_per_product.c.region.label("top_region"),
            )
            .outerjoin(
                top_region_per_product,
                top_region_per_product.c.product_id == product_data.c.product_id,
            )
            .outerjoin(
                returns_data, returns_data.c.product_id == product_data.c.product_id
            )
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
                    "product_id": row.product_id,
                    "product_name": row.product_name,
                    "category": row.category.value
                    if hasattr(row.category, "value")
                    else row.category,
                    "brand": row.brand.value
                    if hasattr(row.brand, "value")
                    else row.brand,
                    "cost": row.cost,
                    "stock_quantity": row.stock_quantity,
                    "total_sales": row.total_sales,
                    "total_profit": row.total_profit,
                    "total_returns": row.total_returns,
                    "total_orders": row.total_orders,
                    "top_region": row.top_region,
                    "sales_contribution_percentage": round(sales_contribution, 2),
                }
            )

        return formatted_results
