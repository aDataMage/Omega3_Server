from sqlalchemy.orm import Session
from models.CustomersModel import Customer
from schemas.CustomerSchema import CustomerCreate, CustomerUpdate
from uuid import uuid4


class CustomerCrud:
    @staticmethod
    def get_all_customers(db: Session) -> list[Customer]:
        return db.query(Customer).all()

    @staticmethod
    def get_customer_by_id(db: Session, customer_id: str) -> Customer | None:
        return db.query(Customer).filter(Customer.customer_id == customer_id).first()

    @staticmethod
    def create_customer(db: Session, customer: CustomerCreate) -> Customer:
        db_customer = Customer(
            customer_id=str(uuid4()),  # optionally use UUID type directly
            **customer.model_dump(),
        )
        db.add(db_customer)
        db.commit()
        db.refresh(db_customer)
        return db_customer

    @staticmethod
    def update_customer(
        db: Session, customer_id: str, customer: CustomerUpdate
    ) -> Customer | None:
        db_customer = CustomerCrud.get_customer_by_id(db, customer_id)
        if db_customer:
            for key, value in customer.model_dump(exclude_unset=True).items():
                setattr(db_customer, key, value)
            db.commit()
            db.refresh(db_customer)
            return db_customer
        return None

    @staticmethod
    def delete_customer(db: Session, customer_id: str) -> bool:
        db_customer = CustomerCrud.get_customer_by_id(db, customer_id)
        if db_customer:
            db.delete(db_customer)
            db.commit()
            return True
        return False


# # def fetch_customer_metrics(
#     db: Session,
#     comparison_level: str,
#     selected_regions: List[str] = None,
#     selected_stores: List[str] = None,
#     selected_brands: List[str] = None,
#     selected_products: List[str] = None,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
# ) -> List[Dict[str, Any]]:
#     """
#     Fetch all four customer metrics with comparison capabilities.
#     Returns a list of metric objects with total values and comparisons.
#     """
#     valid_comparison_levels = ["region", "store", "brand", "product"]
#     if comparison_level not in valid_comparison_levels:
#         raise ValueError(
#             f"Invalid comparison level. Must be one of: {valid_comparison_levels}"
#         )
#     end_date = end_date or datetime.now(timezone.utc)

#     results = []

#     # First get all comparison data in a single query for consistency
#     comparison_data = _get_all_comparison_data(
#         db=db,
#         comparison_level=comparison_level,
#         selected_regions=selected_regions,
#         selected_stores=selected_stores,
#         selected_brands=selected_brands,
#         selected_products=selected_products,
#         start_date=start_date,
#         end_date=end_date,
#     )

#     # Process each metric
#     metrics = [
#         ("Total Customers", "total_customers"),
#         ("Average Revenue per Customer", "avg_revenue_per_customer"),
#         ("New Customers", "new_customers"),
#         ("Repeat Customer Rate", "repeat_customer_rate"),
#     ]

#     for metric_name, metric_key in metrics:
#         # Calculate total from comparison data to ensure consistency
#         total_value = (
#             sum(
#                 float(item["value"])
#                 for item in comparison_data[metric_key]["comparisons"]
#             )
#             if metric_key in comparison_data
#             else 0
#         )

#         results.append(
#             {
#                 "metric_name": metric_name,
#                 "total_value": total_value,
#                 "comparisons": comparison_data[metric_key]["comparisons"],
#             }
#         )

#     return results


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
#     """
#     Fetch all four customer metrics with comparison capabilities.
#     Ensures total value equals the sum of comparison values.
#     """
#     valid_comparison_levels = ["region", "store", "brand", "product"]
#     if comparison_level not in valid_comparison_levels:
#         raise ValueError(
#             f"Invalid comparison level. Must be one of: {valid_comparison_levels}"
#         )
#     end_date = end_date or datetime.now(timezone.utc)

#     # Get comparison data once
#     comparison_data = _get_all_comparison_data(
#         db=db,
#         comparison_level=comparison_level,
#         selected_regions=selected_regions,
#         selected_stores=selected_stores,
#         selected_brands=selected_brands,
#         selected_products=selected_products,
#         start_date=start_date,
#         end_date=end_date,
#     )

#     results = []

#     metrics = [
#         ("Total Customers", "total_customers"),
#         ("Average Revenue per Customer", "avg_revenue_per_customer"),
#         ("New Customers", "new_customers"),
#         ("Repeat Customer Rate", "repeat_customer_rate"),
#     ]

#     for metric_name, metric_key in metrics:
#         comparisons = comparison_data.get(metric_key, {}).get("comparisons", [])
#         total_value = sum(float(item.get("value", 0)) for item in comparisons)

#         results.append(
#             {
#                 "metric_name": metric_name,
#                 "total_value": total_value,
#                 "comparisons": comparisons,
#             }
#         )

#     return results


# def _get_all_comparison_data(
#     db: Session,
#     comparison_level: str,
#     selected_regions: List[str] = None,
#     selected_stores: List[str] = None,
#     selected_brands: List[str] = None,
#     selected_products: List[str] = None,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
# ) -> Dict[str, Any]:
#     """Get all comparison data in a single consistent query"""
#     # First build the base query properly
#     comparison_column = _get_comparison_column(comparison_level)

#     first_purchases = (
#         db.query(
#             Order.customer_id,
#             func.min(Order.order_date).label("first_purchase_date"),
#         )
#         .group_by(Order.customer_id)
#         .subquery()
#     )

#     query = db.query(
#         comparison_column.label("comparison_value"),
#         func.count(distinct(Order.customer_id)).label("total_customers"),
#         func.sum(OrderItem.price * OrderItem.quantity).label("total_sales"),
#         func.count(
#             distinct(
#                 case(
#                     [
#                         (
#                             first_purchases.c.first_purchase_date.between(
#                                 start_date, end_date
#                             ),
#                             Order.customer_id,
#                         )
#                     ],
#                     else_=None,
#                 )
#             )
#         ).label("new_customers"),
#     )

#     # Joins
#     query = query.join(OrderItem, OrderItem.order_id == Order.order_id)
#     query = query.join(Product, Product.product_id == OrderItem.product_id)
#     query = query.join(Customer, Customer.customer_id == Order.customer_id)
#     query = query.join(
#         first_purchases, first_purchases.c.customer_id == Customer.customer_id
#     )

#     if comparison_level in ["region", "store"]:
#         query = query.join(Store, Store.store_id == Order.store_id)

#     # Apply filters
#     if selected_regions:
#         query = query.filter(Store.region.in_(selected_regions))
#     if selected_stores:
#         query = query.filter(Store.store_id.in_(selected_stores))
#     if selected_brands:
#         query = query.filter(Product.brand.in_(selected_brands))
#     if selected_products:
#         query = query.filter(Product.name.in_(selected_products))
#     if start_date and end_date:
#         query = query.filter(Order.order_date.between(start_date, end_date))

#     # Group by the actual column, not the label
#     query = query.group_by(comparison_column)

#     # Execute query
#     base_results = query.all()

#     # For repeat customers, we need a separate subquery
#     repeat_subq = db.query(
#         comparison_column.label("comparison_value"),
#         Order.customer_id.label("customer_id"),
#         func.count(Order.order_id).label("order_count"),
#     )

#     repeat_subq = repeat_subq.join(OrderItem, OrderItem.order_id == Order.order_id)
#     repeat_subq = repeat_subq.join(Product, Product.product_id == OrderItem.product_id)
#     repeat_subq = repeat_subq.join(Customer, Customer.customer_id == Order.customer_id)

#     if comparison_level in ["region", "store"]:
#         repeat_subq = repeat_subq.join(Store, Store.store_id == Order.store_id)

#     # Apply same filters
#     if selected_regions:
#         repeat_subq = repeat_subq.filter(Store.region.in_(selected_regions))
#     if selected_stores:
#         repeat_subq = repeat_subq.filter(Store.store_id.in_(selected_stores))
#     if selected_brands:
#         repeat_subq = repeat_subq.filter(Product.brand.in_(selected_brands))
#     if selected_products:
#         repeat_subq = repeat_subq.filter(Product.name.in_(selected_products))
#     if start_date and end_date:
#         repeat_subq = repeat_subq.filter(Order.order_date.between(start_date, end_date))

#     # Group by the actual column and customer_id
#     repeat_subq = repeat_subq.group_by(comparison_column, Order.customer_id)
#     repeat_subq = repeat_subq.subquery()

#     repeat_query = db.query(
#         repeat_subq.c.comparison_value,
#         func.count().label("total_customers"),
#         func.sum(case([(repeat_subq.c.order_count > 1, 1)], else_=0)).label(
#             "repeat_customers"
#         ),
#     ).group_by(repeat_subq.c.comparison_value)

#     repeat_results = repeat_query.all()

#     # Organize all results
#     comparison_data = {}

#     # Process base metrics
#     for row in base_results:
#         comparison_value = row.comparison_value
#         comparison_data[comparison_value] = {
#             "total_customers": float(row.total_customers or 0),
#             "total_sales": float(row.total_sales or 0),
#             "new_customers": float(row.new_customers or 0),
#         }

#     # Process repeat customer data
#     for row in repeat_results:
#         comparison_value = row.comparison_value
#         total = float(row.total_customers or 1)  # Avoid division by zero
#         repeat = float(row.repeat_customers or 0)
#         if comparison_value in comparison_data:
#             comparison_data[comparison_value]["repeat_rate"] = (
#                 (repeat / total) * 100 if total > 0 else 0
#             )
#         else:
#             comparison_data[comparison_value] = {
#                 "total_customers": total,
#                 "total_sales": 0,
#                 "new_customers": 0,
#                 "repeat_rate": (repeat / total) * 100 if total > 0 else 0,
#             }

#     # Format final output
#     formatted_data = {
#         "total_customers": {
#             "comparisons": [
#                 {"name": str(k), "value": v["total_customers"]}
#                 for k, v in comparison_data.items()
#             ]
#         },
#         "avg_revenue_per_customer": {
#             "comparisons": [
#                 {
#                     "name": str(k),
#                     "value": v["total_sales"] / v["total_customers"]
#                     if v["total_customers"] > 0
#                     else 0,
#                 }
#                 for k, v in comparison_data.items()
#             ]
#         },
#         "new_customers": {
#             "comparisons": [
#                 {"name": str(k), "value": v["new_customers"]}
#                 for k, v in comparison_data.items()
#             ]
#         },
#         "repeat_customer_rate": {
#             "comparisons": [
#                 {"name": str(k), "value": v.get("repeat_rate", 0)}
#                 for k, v in comparison_data.items()
#             ]
#         },
#     }

#     return formatted_data


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
#     """
#     Fetch all four customer metrics with comparison capabilities.
#     Returns a list of metric objects with total values and comparisons.
#     """
#     valid_comparison_levels = ["region", "store", "brand", "product"]
#     if comparison_level not in valid_comparison_levels:
#         raise ValueError(
#             f"Invalid comparison level. Must be one of: {valid_comparison_levels}"
#         )
#     end_date = end_date or datetime.now(timezone.utc)

#     results = []

#     # Total Customers and ARPC can use the shared query
#     simple_metrics = [
#         ("Total Customers", func.count(distinct(Order.customer_id))),
#         (
#             "Average Revenue per Customer",
#             func.sum(OrderItem.price * OrderItem.quantity)
#             / func.nullif(func.count(distinct(Order.customer_id)), 0),
#         ),
#     ]

#     for metric_name, expression in simple_metrics:
#         total_value = (
#             _build_customer_metric_query(
#                 db=db,
#                 metric_expression=expression,
#                 selected_regions=selected_regions,
#                 selected_stores=selected_stores,
#                 selected_brands=selected_brands,
#                 selected_products=selected_products,
#                 start_date=start_date,
#                 end_date=end_date,
#             ).scalar()
#             or 0
#         )

#         comparison_query = _build_customer_metric_query(
#             db=db,
#             metric_expression=expression,
#             comparison_level=comparison_level,
#             selected_regions=selected_regions,
#             selected_stores=selected_stores,
#             selected_brands=selected_brands,
#             selected_products=selected_products,
#             start_date=start_date,
#             end_date=end_date,
#         )

#         if comparison_level == "region":
#             comparison_query = comparison_query.group_by(Store.region)
#         elif comparison_level == "store":
#             comparison_query = comparison_query.group_by(Store.store_id, Store.name)
#         elif comparison_level == "brand":
#             comparison_query = comparison_query.group_by(Product.brand)
#         elif comparison_level == "product":
#             comparison_query = comparison_query.group_by(Product.name)

#         comparison_results = comparison_query.all()
#         comparisons = [
#             {"name": row.comparison_value, "value": float(row.metric_value or 0)}
#             for row in comparison_results
#         ]

#         results.append(
#             {
#                 "metric_name": metric_name,
#                 "total_value": float(total_value),
#                 "comparisons": comparisons,
#             }
#         )

#         # ---- New Customers ----
#     new_customers_total = _count_new_customers(
#         db, selected_regions, selected_stores, start_date, end_date
#     )
#     new_comparisons = _count_new_customers_by_comparison(
#         db,
#         comparison_level,
#         selected_regions,
#         selected_stores,
#         selected_brands,
#         selected_products,
#         start_date,
#         end_date,
#     )
#     results.append(
#         {
#             "metric_name": "New Customers",
#             "total_value": new_customers_total,
#             "comparisons": new_comparisons,
#         }
#     )

#     # ---- Repeat Customer Rate ----
#     repeat_rate_total = _calculate_repeat_customer_rate(
#         db, selected_regions, selected_stores, start_date, end_date
#     )
#     repeat_comparisons = _calculate_repeat_customer_rate_by_comparison(
#         db,
#         comparison_level,
#         selected_regions,
#         selected_stores,
#         selected_brands,
#         selected_products,
#         start_date,
#         end_date,
#     )
#     results.append(
#         {
#             "metric_name": "Repeat Customer Rate",
#             "total_value": repeat_rate_total,
#             "comparisons": repeat_comparisons,
#         }
#     )

#     return results


# def _build_customer_metric_query(
#     db: Session,
#     metric_expression: Any,
#     comparison_level: str = None,
#     selected_regions: List[str] = None,
#     selected_stores: List[str] = None,
#     selected_brands: List[str] = None,
#     selected_products: List[str] = None,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
# ):
#     """Helper to build the base query for customer metrics with proper joins"""
#     query = db.query()

#     # Add comparison value if needed
#     if comparison_level:
#         if comparison_level == "region":
#             query = query.add_columns(Store.region.label("comparison_value"))
#         elif comparison_level == "store":
#             query = query.add_columns(Store.name.label("comparison_value"))
#         elif comparison_level == "brand":
#             query = query.add_columns(Product.brand.label("comparison_value"))
#         elif comparison_level == "product":
#             query = query.add_columns(Product.name.label("comparison_value"))

#     query = query.add_columns(metric_expression.label("metric_value"))

#     # Start from Order and join all necessary tables
#     query = query.select_from(Order)
#     query = query.join(OrderItem, OrderItem.order_id == Order.order_id)
#     query = query.join(Product, Product.product_id == OrderItem.product_id)
#     query = query.join(Customer, Customer.customer_id == Order.customer_id)

#     # Join Store only if needed
#     if comparison_level in ["region", "store"] or selected_regions or selected_stores:
#         query = query.join(Store, Store.store_id == Order.store_id)

#     # Apply filters
#     if selected_regions:
#         query = query.filter(Store.region.in_(selected_regions))
#     if selected_stores:
#         query = query.filter(Store.store_id.in_(selected_stores))
#     if selected_brands:
#         query = query.filter(Product.brand.in_(selected_brands))
#     if selected_products:
#         query = query.filter(Product.name.in_(selected_products))
#     if start_date and end_date:
#         query = query.filter(Order.order_date.between(start_date, end_date))

#     return query


# def _count_new_customers(db, selected_regions, selected_stores, start_date, end_date):
#     if not start_date or not end_date:
#         return 0

#     subq = (
#         db.query(
#             Order.customer_id, func.min(Order.order_date).label("first_order_date")
#         )
#         .join(Store, Store.store_id == Order.store_id)
#         .group_by(Order.customer_id)
#     )
#     if selected_regions:
#         subq = subq.filter(Store.region.in_(selected_regions))
#     if selected_stores:
#         subq = subq.filter(Store.store_id.in_(selected_stores))

#     subq = subq.subquery()

#     count = (
#         db.query(func.count())
#         .select_from(subq)
#         .filter(subq.c.first_order_date.between(start_date, end_date))
#         .scalar()
#     )
#     return count or 0


# def _calculate_repeat_customer_rate(
#     db, selected_regions, selected_stores, start_date, end_date
# ):
#     if not start_date or not end_date:
#         return 0.0

#     base_query = db.query(Order).join(Store, Store.store_id == Order.store_id)
#     if selected_regions:
#         base_query = base_query.filter(Store.region.in_(selected_regions))
#     if selected_stores:
#         base_query = base_query.filter(Store.store_id.in_(selected_stores))
#     base_query = base_query.filter(Order.order_date.between(start_date, end_date))

#     subq = (
#         base_query.with_entities(
#             Order.customer_id, func.count(Order.order_id).label("order_count")
#         )
#         .group_by(Order.customer_id)
#         .subquery()
#     )

#     total_customers = db.query(func.count()).select_from(subq).scalar()
#     repeat_customers = (
#         db.query(func.count()).select_from(subq).filter(subq.c.order_count > 1).scalar()
#     )

#     if not total_customers:
#         return 0.0
#     return round((repeat_customers / total_customers) * 100, 2)


# def _count_new_customers_by_comparison(
#     db,
#     comparison_level,
#     selected_regions,
#     selected_stores,
#     selected_brands,
#     selected_products,
#     start_date,
#     end_date,
# ):
#     if not start_date or not end_date:
#         return []

#     # Subquery: first purchase date per customer
#     first_purchase_subq = (
#         db.query(
#             Order.customer_id.label("customer_id"),
#             func.min(Order.order_date).label("first_order_date"),
#         )
#         .join(Store, Store.store_id == Order.store_id)
#         .join(OrderItem, OrderItem.order_id == Order.order_id)
#         .join(Product, Product.product_id == OrderItem.product_id)
#         .group_by(Order.customer_id)
#         .subquery()
#     )

#     # Join back with order, store, product
#     query = db.query()
#     if comparison_level == "region":
#         query = query.add_columns(Store.region.label("comparison_value"))
#     elif comparison_level == "store":
#         query = query.add_columns(Store.name.label("comparison_value"))
#     elif comparison_level == "brand":
#         query = query.add_columns(Product.brand.label("comparison_value"))
#     elif comparison_level == "product":
#         query = query.add_columns(Product.name.label("comparison_value"))

#     query = query.add_columns(func.count().label("metric_value"))
#     query = query.select_from(first_purchase_subq)
#     query = query.join(Order, Order.customer_id == first_purchase_subq.c.customer_id)
#     query = query.join(Store, Store.store_id == Order.store_id)
#     query = query.join(OrderItem, OrderItem.order_id == Order.order_id)
#     query = query.join(Product, Product.product_id == OrderItem.product_id)

#     query = query.filter(
#         first_purchase_subq.c.first_order_date.between(start_date, end_date)
#     )

#     # Apply filters
#     if selected_regions:
#         query = query.filter(Store.region.in_(selected_regions))
#     if selected_stores:
#         query = query.filter(Store.store_id.in_(selected_stores))
#     if selected_brands:
#         query = query.filter(Product.brand.in_(selected_brands))
#     if selected_products:
#         query = query.filter(Product.name.in_(selected_products))

#     # Group by
#     if comparison_level == "region":
#         query = query.group_by(Store.region)
#     elif comparison_level == "store":
#         query = query.group_by(Store.name)
#     elif comparison_level == "brand":
#         query = query.group_by(Product.brand)
#     elif comparison_level == "product":
#         query = query.group_by(Product.name)

#     return [
#         {"name": row.comparison_value, "value": float(row.metric_value or 0)}
#         for row in query.all()
#     ]


# def _calculate_repeat_customer_rate_by_comparison(
#     db,
#     comparison_level,
#     selected_regions,
#     selected_stores,
#     selected_brands,
#     selected_products,
#     start_date,
#     end_date,
# ):
#     if not start_date or not end_date:
#         return []

#     # Query base: customer + order count
#     query = db.query()

#     if comparison_level == "region":
#         query = query.add_columns(Store.region.label("comparison_value"))
#     elif comparison_level == "store":
#         query = query.add_columns(Store.name.label("comparison_value"))
#     elif comparison_level == "brand":
#         query = query.add_columns(Product.brand.label("comparison_value"))
#     elif comparison_level == "product":
#         query = query.add_columns(Product.name.label("comparison_value"))

#     query = query.add_columns(
#         Order.customer_id, func.count(Order.order_id).label("order_count")
#     )
#     query = query.select_from(Order)
#     query = query.join(Store, Store.store_id == Order.store_id)
#     query = query.join(OrderItem, OrderItem.order_id == Order.order_id)
#     query = query.join(Product, Product.product_id == OrderItem.product_id)

#     query = query.filter(Order.order_date.between(start_date, end_date))

#     if selected_regions:
#         query = query.filter(Store.region.in_(selected_regions))
#     if selected_stores:
#         query = query.filter(Store.store_id.in_(selected_stores))
#     if selected_brands:
#         query = query.filter(Product.brand.in_(selected_brands))
#     if selected_products:
#         query = query.filter(Product.name.in_(selected_products))

#     if comparison_level == "region":
#         query = query.group_by(Store.region, Order.customer_id)
#     elif comparison_level == "store":
#         query = query.group_by(Store.name, Order.customer_id)
#     elif comparison_level == "brand":
#         query = query.group_by(Product.brand, Order.customer_id)
#     elif comparison_level == "product":
#         query = query.group_by(Product.name, Order.customer_id)

#     # Materialize subquery
#     subq = query.subquery()

#     # Count total + repeat per comparison group
#     group_query = db.query(subq.c.comparison_value)
#     group_query = group_query.add_columns(
#         func.count().label("total_customers"),
#         func.sum(case([(subq.c.order_count > 1, 1)], else_=0)).label(
#             "repeat_customers"
#         ),
#     ).group_by(subq.c.comparison_value)

#     results = []
#     for row in group_query.all():
#         if row.total_customers:
#             rate = (row.repeat_customers / row.total_customers) * 100
#         else:
#             rate = 0.0
#         results.append({"name": row.comparison_value, "value": round(rate, 2)})

#     return results


# def fetch_customer_metric_trend(
#     db: Session,
#     metric_name: str,
#     comparison_level: Optional[str] = None,
#     selected_regions: List[str] = None,
#     selected_stores: List[str] = None,
#     selected_brands: List[str] = None,
#     selected_products: List[str] = None,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
#     interval: str = "month",
# ) -> List[Dict[str, Any]]:
#     if metric_name not in [
#         "Total Customers",
#         "New Customers",
#         "Average Revenue per Customer",
#         "Repeat Customer Rate",
#     ]:
#         raise ValueError("Unsupported customer metric")

#     end_date = end_date or datetime.now(timezone.utc)
#     time_bucket = func.date_trunc(interval, Order.order_date).label("period")

#     def apply_filters(q):
#         if start_date and end_date:
#             q = q.filter(Order.order_date.between(start_date, end_date))
#         if selected_regions:
#             q = q.filter(Store.region.in_(selected_regions))
#         if selected_stores:
#             q = q.filter(Store.store_id.in_(selected_stores))
#         if selected_brands:
#             q = q.filter(Product.brand.in_(selected_brands))
#         if selected_products:
#             q = q.filter(Product.name.in_(selected_products))
#         return q

#     # GENERAL TREND
#     general_query = db.query(time_bucket)
#     if metric_name == "Total Customers":
#         metric_col = func.count(distinct(Order.customer_id)).label("value")
#         general_query = general_query.add_columns(metric_col)
#         general_query = general_query.select_from(Order)
#         general_query = general_query.join(
#             OrderItem, OrderItem.order_id == Order.order_id
#         )
#         general_query = general_query.join(
#             Product, Product.product_id == OrderItem.product_id
#         )
#         general_query = general_query.join(
#             Customer, Customer.customer_id == Order.customer_id
#         )
#         if selected_regions or selected_stores:
#             general_query = general_query.join(Store, Store.store_id == Order.store_id)
#         general_query = apply_filters(general_query).group_by("period")
#         general_results = [
#             {"name": "general", "trend": _format_trend_result(general_query.all())}
#         ]
#     elif metric_name == "Average Revenue per Customer":
#         metric_col = (
#             func.sum(OrderItem.price * OrderItem.quantity)
#             / func.nullif(func.count(distinct(Order.customer_id)), 0)
#         ).label("value")
#         general_query = general_query.add_columns(metric_col)
#         general_query = general_query.select_from(Order)
#         general_query = general_query.join(
#             OrderItem, OrderItem.order_id == Order.order_id
#         )
#         general_query = general_query.join(
#             Product, Product.product_id == OrderItem.product_id
#         )
#         general_query = general_query.join(
#             Customer, Customer.customer_id == Order.customer_id
#         )
#         if selected_regions or selected_stores:
#             general_query = general_query.join(Store, Store.store_id == Order.store_id)
#         general_query = apply_filters(general_query).group_by("period")
#         general_results = [
#             {"name": "general", "trend": _format_trend_result(general_query.all())}
#         ]
#     elif metric_name == "Repeat Customer Rate":
#         subq = (
#             db.query(
#                 Order.customer_id,
#                 func.count(Order.order_id).label("order_count"),
#                 func.date_trunc(interval, Order.order_date).label("period"),
#             )
#             .filter(Order.order_date.between(start_date, end_date))
#             .group_by(Order.customer_id, "period")
#             .subquery()
#         )
#         general_query = db.query(
#             subq.c.period.label("period"),
#             (
#                 func.sum(case((subq.c.order_count > 1, 1), else_=0))
#                 / func.nullif(func.count(), 0)
#                 * 100
#             ).label("value"),
#         ).group_by(subq.c.period)
#         general_results = [
#             {"name": "general", "trend": _format_trend_result(general_query.all())}
#         ]
#         # Fall through to comparison if needed
#     elif metric_name == "New Customers":
#         first_purchases = _get_new_customers_subquery()
#         general_query = (
#             db.query(
#                 func.date_trunc(interval, first_purchases.c.first_purchase_date).label(
#                     "period"
#                 ),
#                 func.count(distinct(first_purchases.c.customer_id)).label("value"),
#             )
#             .filter(first_purchases.c.first_purchase_date.between(start_date, end_date))
#             .group_by("period")
#         )
#         general_results = [
#             {"name": "general", "trend": _format_trend_result(general_query.all())}
#         ]
#     else:
#         general_query = general_query.add_columns(metric_col)
#         general_query = general_query.select_from(Order)
#         general_query = general_query.join(
#             OrderItem, OrderItem.order_id == Order.order_id
#         )
#         general_query = general_query.join(
#             Product, Product.product_id == OrderItem.product_id
#         )
#         general_query = general_query.join(
#             Customer, Customer.customer_id == Order.customer_id
#         )
#         if selected_regions or selected_stores:
#             general_query = general_query.join(Store, Store.store_id == Order.store_id)
#         general_query = apply_filters(general_query).group_by("period")
#         general_results = [
#             {"name": "general", "trend": _format_trend_result(general_query.all())}
#         ]

#     # COMPARISON TREND (optional)
#     if comparison_level:
#         comp_col = _get_comparison_column(comparison_level)
#         comparison_query = db.query(time_bucket, comp_col)
#         if metric_name == "Total Customers":
#             metric_col = func.count(distinct(Order.customer_id)).label("value")
#         elif metric_name == "Average Revenue per Customer":
#             metric_col = (
#                 func.sum(OrderItem.price * OrderItem.quantity)
#                 / func.nullif(func.count(distinct(Order.customer_id)), 0)
#             ).label("value")
#         elif metric_name == "Repeat Customer Rate":
#             comparison_col = _get_comparison_column(comparison_level)

#             base_query = (
#                 db.query(
#                     Order.customer_id.label("customer_id"),
#                     func.date_trunc(interval, Order.order_date).label("period"),
#                     comparison_col.label("comparison_value"),
#                     Order.order_id.label("order_id"),
#                 )
#                 .join(Store, Store.store_id == Order.store_id)
#                 .join(OrderItem, OrderItem.order_id == Order.order_id)
#                 .join(Product, Product.product_id == OrderItem.product_id)
#                 .join(Customer, Customer.customer_id == Order.customer_id)
#             )

#             base_query = apply_filters(base_query)
#             base_subq = base_query.subquery()

#             customer_orders_subq = (
#                 db.query(
#                     base_subq.c.customer_id,
#                     base_subq.c.period,
#                     base_subq.c.comparison_value,
#                     func.count(base_subq.c.order_id).label("order_count"),
#                 )
#                 .group_by(
#                     base_subq.c.customer_id,
#                     base_subq.c.period,
#                     base_subq.c.comparison_value,
#                 )
#                 .subquery()
#             )

#             comparison_query = db.query(
#                 customer_orders_subq.c.period,
#                 customer_orders_subq.c.comparison_value,
#                 (
#                     func.sum(case((customer_orders_subq.c.order_count > 1, 1), else_=0))
#                     / func.nullif(func.count(), 0)
#                     * 100
#                 ).label("value"),
#             ).group_by(
#                 customer_orders_subq.c.period,
#                 customer_orders_subq.c.comparison_value,
#             )

#             return general_results + _format_comparison_trend(comparison_query.all())

#         elif metric_name == "New Customers":
#             first_purchases = (
#                 db.query(
#                     Order.customer_id,
#                     func.min(Order.order_date).label("first_purchase_date"),
#                     _get_comparison_column(comparison_level).label("comparison_value"),
#                 )
#                 .join(OrderItem, OrderItem.order_id == Order.order_id)
#                 .join(Product, Product.product_id == OrderItem.product_id)
#                 .join(Customer, Customer.customer_id == Order.customer_id)
#             )

#             if comparison_level in ["region", "store"]:
#                 first_purchases = first_purchases.join(
#                     Store, Store.store_id == Order.store_id
#                 )

#             if selected_regions:
#                 first_purchases = first_purchases.filter(
#                     Store.region.in_(selected_regions)
#                 )
#             if selected_stores:
#                 first_purchases = first_purchases.filter(
#                     Store.store_id.in_(selected_stores)
#                 )
#             if selected_brands:
#                 first_purchases = first_purchases.filter(
#                     Product.brand.in_(selected_brands)
#                 )
#             if selected_products:
#                 first_purchases = first_purchases.filter(
#                     Product.name.in_(selected_products)
#                 )

#             first_purchases = first_purchases.group_by(
#                 Order.customer_id, "comparison_value"
#             ).subquery()

#             comparison_query = (
#                 db.query(
#                     func.date_trunc(
#                         interval, first_purchases.c.first_purchase_date
#                     ).label("period"),
#                     first_purchases.c.comparison_value,
#                     func.count(distinct(first_purchases.c.customer_id)).label("value"),
#                 )
#                 .filter(
#                     first_purchases.c.first_purchase_date.between(start_date, end_date)
#                 )
#                 .group_by("period", "comparison_value")
#             )

#             return general_results + _format_comparison_trend(comparison_query.all())

#         comparison_query = comparison_query.add_columns(metric_col)
#         comparison_query = comparison_query.select_from(Order)
#         comparison_query = comparison_query.join(
#             OrderItem, OrderItem.order_id == Order.order_id
#         )
#         comparison_query = comparison_query.join(
#             Product, Product.product_id == OrderItem.product_id
#         )
#         comparison_query = comparison_query.join(
#             Customer, Customer.customer_id == Order.customer_id
#         )
#         comparison_query = comparison_query.join(
#             Store, Store.store_id == Order.store_id
#         )
#         comparison_query = apply_filters(comparison_query).group_by(
#             "period", "comparison_value"
#         )
#         return general_results + _format_comparison_trend(comparison_query.all())
#     return general_results


# def fetch_customer_metric_trend(
#     db: Session,
#     metric_name: str,
#     comparison_level: Optional[str] = None,
#     selected_regions: List[str] = None,
#     selected_stores: List[str] = None,
#     selected_brands: List[str] = None,
#     selected_products: List[str] = None,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
#     interval: str = "month",
# ) -> List[Dict[str, Any]]:
#     if metric_name not in [
#         "Total Customers",
#         "New Customers",
#         "Average Revenue per Customer",
#         "Repeat Customer Rate",
#     ]:
#         raise ValueError("Unsupported customer metric")

#     end_date = end_date or datetime.now(timezone.utc)
#     time_bucket = func.date_trunc(interval, Order.order_date).label("period")

#     def apply_filters(q):
#         if start_date and end_date:
#             q = q.filter(Order.order_date.between(start_date, end_date))
#         if selected_regions:
#             q = q.filter(Store.region.in_(selected_regions))
#         if selected_stores:
#             q = q.filter(Store.store_id.in_(selected_stores))
#         if selected_brands:
#             q = q.filter(Product.brand.in_(selected_brands))
#         if selected_products:
#             q = q.filter(Product.name.in_(selected_products))
#         return q

#     # GENERAL TREND
#     general_query = db.query(time_bucket)

#     if metric_name == "Total Customers":
#         # Use same calculation as in fetch_customer_metrics
#         metric_col = func.count(distinct(Order.customer_id)).label("value")
#         general_query = general_query.add_columns(metric_col)
#         general_query = general_query.select_from(Order)
#         general_query = general_query.join(
#             OrderItem, OrderItem.order_id == Order.order_id
#         )
#         general_query = general_query.join(
#             Product, Product.product_id == OrderItem.product_id
#         )
#         general_query = general_query.join(
#             Customer, Customer.customer_id == Order.customer_id
#         )
#         if selected_regions or selected_stores:
#             general_query = general_query.join(Store, Store.store_id == Order.store_id)
#         general_query = apply_filters(general_query).group_by("period")

#     elif metric_name == "Average Revenue per Customer":
#         # Consistent with fetch_customer_metrics calculation
#         metric_col = (
#             func.sum(OrderItem.price * OrderItem.quantity)
#             / func.nullif(func.count(distinct(Order.customer_id)), 0)
#         ).label("value")
#         general_query = general_query.add_columns(metric_col)
#         general_query = general_query.select_from(Order)
#         general_query = general_query.join(
#             OrderItem, OrderItem.order_id == Order.order_id
#         )
#         general_query = general_query.join(
#             Product, Product.product_id == OrderItem.product_id
#         )
#         general_query = general_query.join(
#             Customer, Customer.customer_id == Order.customer_id
#         )
#         if selected_regions or selected_stores:
#             general_query = general_query.join(Store, Store.store_id == Order.store_id)
#         general_query = apply_filters(general_query).group_by("period")

#     elif metric_name == "Repeat Customer Rate":
#         # Use same approach as in fetch_customer_metrics
#         subq = (
#             db.query(
#                 Order.customer_id,
#                 func.count(Order.order_id).label("order_count"),
#                 func.date_trunc(interval, Order.order_date).label("period"),
#             )
#             .join(OrderItem, OrderItem.order_id == Order.order_id)
#             .join(Product, Product.product_id == OrderItem.product_id)
#             .join(Customer, Customer.customer_id == Order.customer_id)
#         )
#         if selected_regions or selected_stores:
#             subq = subq.join(Store, Store.store_id == Order.store_id)
#         subq = apply_filters(subq).group_by(Order.customer_id, "period").subquery()

#         general_query = db.query(
#             subq.c.period.label("period"),
#             (
#                 func.sum(case((subq.c.order_count > 1, 1), else_=0))
#                 / func.nullif(func.count(), 0)
#                 * 100
#             ).label("value"),
#         ).group_by(subq.c.period)

#     elif metric_name == "New Customers":
#         # Match the new customer definition from fetch_customer_metrics
#         first_purchases = (
#             db.query(
#                 Order.customer_id,
#                 func.min(Order.order_date).label("first_purchase_date"),
#             )
#             .join(OrderItem, OrderItem.order_id == Order.order_id)
#             .join(Product, Product.product_id == OrderItem.product_id)
#             .join(Customer, Customer.customer_id == Order.customer_id)
#         )
#         if selected_regions or selected_stores:
#             first_purchases = first_purchases.join(
#                 Store, Store.store_id == Order.store_id
#             )
#         first_purchases = apply_filters(first_purchases)
#         first_purchases = first_purchases.group_by(Order.customer_id).subquery()

#         general_query = (
#             db.query(
#                 func.date_trunc(interval, first_purchases.c.first_purchase_date).label(
#                     "period"
#                 ),
#                 func.count(distinct(first_purchases.c.customer_id)).label("value"),
#             )
#             .filter(first_purchases.c.first_purchase_date.between(start_date, end_date))
#             .group_by("period")
#         )

#     general_results = [
#         {"name": "general", "trend": _format_trend_result(general_query.all())}
#     ]

#     # COMPARISON TREND (optional)
#     if comparison_level:
#         comp_col = _get_comparison_column(comparison_level)

#         if metric_name == "Total Customers":
#             comparison_query = db.query(time_bucket, comp_col)
#             comparison_query = comparison_query.add_columns(
#                 func.count(distinct(Order.customer_id)).label("value")
#             )
#             comparison_query = comparison_query.select_from(Order)
#             comparison_query = comparison_query.join(
#                 OrderItem, OrderItem.order_id == Order.order_id
#             )
#             comparison_query = comparison_query.join(
#                 Product, Product.product_id == OrderItem.product_id
#             )
#             comparison_query = comparison_query.join(
#                 Customer, Customer.customer_id == Order.customer_id
#             )
#             comparison_query = comparison_query.join(
#                 Store, Store.store_id == Order.store_id
#             )
#             comparison_query = apply_filters(comparison_query).group_by(
#                 "period", "comparison_value"
#             )

#         elif metric_name == "Average Revenue per Customer":
#             comparison_query = db.query(time_bucket, comp_col)
#             comparison_query = comparison_query.add_columns(
#                 (
#                     func.sum(OrderItem.price * OrderItem.quantity)
#                     / func.nullif(func.count(distinct(Order.customer_id)), 0)
#                 ).label("value")
#             )
#             comparison_query = comparison_query.select_from(Order)
#             comparison_query = comparison_query.join(
#                 OrderItem, OrderItem.order_id == Order.order_id
#             )
#             comparison_query = comparison_query.join(
#                 Product, Product.product_id == OrderItem.product_id
#             )
#             comparison_query = comparison_query.join(
#                 Customer, Customer.customer_id == Order.customer_id
#             )
#             comparison_query = comparison_query.join(
#                 Store, Store.store_id == Order.store_id
#             )
#             comparison_query = apply_filters(comparison_query).group_by(
#                 "period", "comparison_value"
#             )

#         elif metric_name == "Repeat Customer Rate":
#             base_query = (
#                 db.query(
#                     Order.customer_id.label("customer_id"),
#                     func.date_trunc(interval, Order.order_date).label("period"),
#                     comp_col.label("comparison_value"),
#                     Order.order_id.label("order_id"),
#                 )
#                 .join(Store, Store.store_id == Order.store_id)
#                 .join(OrderItem, OrderItem.order_id == Order.order_id)
#                 .join(Product, Product.product_id == OrderItem.product_id)
#                 .join(Customer, Customer.customer_id == Order.customer_id)
#             )
#             base_query = apply_filters(base_query)
#             base_subq = base_query.subquery()

#             customer_orders_subq = (
#                 db.query(
#                     base_subq.c.customer_id,
#                     base_subq.c.period,
#                     base_subq.c.comparison_value,
#                     func.count(base_subq.c.order_id).label("order_count"),
#                 )
#                 .group_by(
#                     base_subq.c.customer_id,
#                     base_subq.c.period,
#                     base_subq.c.comparison_value,
#                 )
#                 .subquery()
#             )

#             comparison_query = db.query(
#                 customer_orders_subq.c.period,
#                 customer_orders_subq.c.comparison_value,
#                 (
#                     func.sum(case((customer_orders_subq.c.order_count > 1, 1), else_=0))
#                     / func.nullif(func.count(), 0)
#                     * 100
#                 ).label("value"),
#             ).group_by(
#                 customer_orders_subq.c.period, customer_orders_subq.c.comparison_value
#             )

#         elif metric_name == "New Customers":
#             first_purchases = (
#                 db.query(
#                     Order.customer_id,
#                     func.min(Order.order_date).label("first_purchase_date"),
#                     comp_col.label("comparison_value"),
#                 )
#                 .join(OrderItem, OrderItem.order_id == Order.order_id)
#                 .join(Product, Product.product_id == OrderItem.product_id)
#                 .join(Customer, Customer.customer_id == Order.customer_id)
#             )
#             if comparison_level in ["region", "store"]:
#                 first_purchases = first_purchases.join(
#                     Store, Store.store_id == Order.store_id
#                 )
#             first_purchases = apply_filters(first_purchases)
#             first_purchases = first_purchases.group_by(
#                 Order.customer_id, "comparison_value"
#             ).subquery()

#             comparison_query = (
#                 db.query(
#                     func.date_trunc(
#                         interval, first_purchases.c.first_purchase_date
#                     ).label("period"),
#                     first_purchases.c.comparison_value,
#                     func.count(distinct(first_purchases.c.customer_id)).label("value"),
#                 )
#                 .filter(
#                     first_purchases.c.first_purchase_date.between(start_date, end_date)
#                 )
#                 .group_by("period", "comparison_value")
#             )

#         return general_results + _format_comparison_trend(comparison_query.all())

#     return general_results


# def _get_new_customers_subquery():
#     return (
#         select(
#             [
#                 Order.customer_id.label("customer_id"),
#                 func.min(Order.order_date).label("first_purchase_date"),
#             ]
#         )
#         .group_by(Order.customer_id)
#         .alias("first_purchases")
#     )


# def _get_comparison_column(level: str):
#     if level == "region":
#         return Store.region.label("comparison_value")
#     elif level == "store":
#         return Store.name.label("comparison_value")
#     elif level == "brand":
#         return Product.brand.label("comparison_value")
#     elif level == "product":
#         return Product.name.label("comparison_value")
#     raise ValueError(f"Invalid comparison level: {level}")


# def _apply_joins(query, comparison_level: Optional[str]):
#     query = query.join(OrderItem, OrderItem.order_id == Order.order_id)
#     query = query.join(Product, Product.product_id == OrderItem.product_id)
#     query = query.join(Customer, Customer.customer_id == Order.customer_id)
#     if comparison_level in ["region", "store"]:
#         query = query.join(Store, Store.store_id == Order.store_id)
#     return query


# def _format_trend_result(rows: List[Tuple[datetime, Any]]) -> List[Dict[str, Any]]:
#     return [
#         {"period": row[0].strftime("%Y-%m-%d"), "value": float(row[1] or 0)}
#         for row in rows
#     ]


# def _format_comparison_trend(
#     rows: List[Tuple[datetime, str, Any]],
# ) -> List[Dict[str, Any]]:
#     trend_map: Dict[str, List[Dict[str, Any]]] = {}

#     for period, comparison_value, value in rows:
#         # Strip enum prefix if present
#         name = str(comparison_value).split(".")[-1]

#         if name not in trend_map:
#             trend_map[name] = []
#         trend_map[name].append(
#             {"period": period.strftime("%Y-%m-%d"), "value": float(value or 0)}
#         )

#     return [{"name": name, "trend": trend} for name, trend in trend_map.items()]


# def fetch_segmented_customer_metric(
#     db: Session,
#     metric_name: str,
#     segment_by: str,
#     comparison_level: Optional[str] = None,
#     selected_regions: Optional[List[str]] = None,
#     selected_stores: Optional[List[str]] = None,
#     selected_brands: Optional[List[str]] = None,
#     selected_products: Optional[List[str]] = None,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
# ) -> Dict[str, Any]:
#     """Fetch customer metrics segmented by demographic attributes with optional comparison."""
#     # Validate inputs
#     VALID_SEGMENTS = [
#         "age",
#         "gender",
#         "income_bracket",
#         "country",
#         "marital_status",
#         "education_level",
#         "employment_status",
#     ]
#     VALID_METRICS = [
#         "Total Customers",
#         "New Customers",
#         "Average Revenue per Customer",
#         "Repeat Customer Rate",
#     ]

#     if segment_by not in VALID_SEGMENTS:
#         raise ValueError(f"Invalid segment. Must be one of: {VALID_SEGMENTS}")
#     if metric_name not in VALID_METRICS:
#         raise ValueError(f"Invalid metric. Must be one of: {VALID_METRICS}")

#     end_date = end_date or datetime.now(timezone.utc)

#     # Define segmentation bins
#     SEGMENT_BINS = {
#         "age": case(
#             [
#                 (Customer.age < 25, "18-24"),
#                 (Customer.age < 35, "25-34"),
#                 (Customer.age < 50, "35-49"),
#                 (Customer.age < 65, "50-64"),
#             ],
#             else_="65+",
#         ).label("segment"),
#         "gender": Customer.gender.label("segment"),
#         "income_bracket": Customer.income_bracket.label("segment"),
#         "country": Customer.country.label("segment"),
#         "marital_status": Customer.marital_status.label("segment"),
#         "education_level": Customer.education_level.label("segment"),
#         "employment_status": Customer.employment_status.label("segment"),
#     }

#     segment_col = SEGMENT_BINS[segment_by]

#     def apply_filters(query):
#         """Apply common filters to queries"""
#         if start_date and end_date:
#             query = query.filter(Order.order_date.between(start_date, end_date))
#         if selected_regions:
#             query = query.filter(Store.region.in_(selected_regions))
#         if selected_stores:
#             query = query.filter(Store.store_id.in_(selected_stores))
#         if selected_brands:
#             query = query.filter(Product.brand.in_(selected_brands))
#         if selected_products:
#             query = query.filter(Product.product_id.in_(selected_products))
#         return query

#     # Base query with common joins
#     def build_base_query(include_segment=True):
#         query = db.query()
#         if include_segment:
#             query = query.add_columns(segment_col)
#         return (
#             query.select_from(Order)
#             .join(OrderItem, OrderItem.order_id == Order.order_id)
#             .join(Product, Product.product_id == OrderItem.product_id)
#             .join(Customer, Customer.customer_id == Order.customer_id)
#             .join(Store, Store.store_id == Order.store_id)
#         )

#     # Metric-specific calculations
#     if metric_name == "Total Customers":
#         query = (
#             build_base_query()
#             .add_columns(func.count(distinct(Order.customer_id)).label("value"))
#             .group_by("segment")
#         )
#         query = apply_filters(query)

#     elif metric_name == "New Customers":
#         first_purchases = (
#             db.query(
#                 Order.customer_id,
#                 func.min(Order.order_date).label("first_purchase_date"),
#             )
#             .group_by(Order.customer_id)
#             .subquery()
#         )

#         query = (
#             build_base_query()
#             .add_columns(
#                 func.count(
#                     distinct(
#                         case(
#                             [
#                                 (
#                                     first_purchases.c.first_purchase_date.between(
#                                         start_date, end_date
#                                     ),
#                                     Order.customer_id,
#                                 )
#                             ],
#                             else_=None,
#                         )
#                     )
#                 ).label("value"),
#             )
#             .join(first_purchases, first_purchases.c.customer_id == Order.customer_id)
#             .group_by("segment")
#         )
#         query = apply_filters(query)

#     elif metric_name == "Average Revenue per Customer":
#         query = (
#             build_base_query()
#             .add_columns(
#                 (
#                     func.sum(OrderItem.price * OrderItem.quantity)
#                     / func.nullif(func.count(distinct(Order.customer_id)), 0)
#                 ).label("value")
#             )
#             .group_by("segment")
#         )
#         query = apply_filters(query)

#     elif metric_name == "Repeat Customer Rate":
#         subq = (
#             db.query(Order.customer_id)
#             .join(Store)
#             .filter(Order.order_date.between(start_date, end_date))
#             .group_by(Order.customer_id)
#             .having(func.count(Order.order_id) > 1)
#             .subquery()
#         )

#         query = (
#             build_base_query()
#             .add_columns(
#                 (
#                     func.count(distinct(subq.c.customer_id))
#                     / func.nullif(func.count(distinct(Order.customer_id)), 0)
#                 ).label("value")
#             )
#             .outerjoin(subq, subq.c.customer_id == Order.customer_id)
#             .group_by("segment")
#         )
#         query = apply_filters(query)

#     # Execute general query
#     general_results = [
#         {"name": row.segment, "value": float(row.value or 0)} for row in query.all()
#     ]

#     # Handle comparison if requested
#     comparison_results = []
#     if comparison_level:
#         comparison_col = _get_comparison_column(comparison_level)
#         comp_query = query.add_columns(comparison_col)
#         comp_query = comp_query.group_by("segment", "comparison_value")

#         rows = comp_query.all()

#         # Group by comparison value
#         grouped = defaultdict(list)
#         for row in rows:
#             grouped[row.comparison_value].append(
#                 {"name": row.segment, "value": float(row.value or 0)}
#             )

#         comparison_results = [
#             {"compare_value": k, "data": v} for k, v in grouped.items()
#         ]

#     return {
#         "seg": segment_by,
#         "metric": metric_name,
#         "general": general_results,
#         "comparison": comparison_results,
#     }


# def fetch_customer_info(
#     db: Session,
#     metric_name: str,
#     return_trend: bool = False,
#     segment_by: Optional[str] = None,
#     comparison_level: Optional[str] = None,
#     selected_regions: Optional[List[str]] = None,
#     selected_stores: Optional[List[str]] = None,
#     selected_brands: Optional[List[str]] = None,
#     selected_products: Optional[List[str]] = None,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
#     interval: str = "month",
# ) -> Dict[str, Any]:
#     # Validations
#     if metric_name not in [
#         "Total Customers",
#         "New Customers",
#         "Average Revenue per Customer",
#         "Repeat Customer Rate",
#     ]:
#         raise ValueError("Unsupported customer metric")

#     # Define helpers
#     def apply_filters(q):
#         if start_date and end_date:
#             q = q.filter(Order.order_date.between(start_date, end_date))
#         if selected_regions:
#             q = q.filter(Store.region.in_(selected_regions))
#         if selected_stores:
#             q = q.filter(Store.store_id.in_(selected_stores))
#         if selected_brands:
#             q = q.filter(Product.brand.in_(selected_brands))
#         if selected_products:
#             q = q.filter(Product.name.in_(selected_products))
#         return q

#     def _get_comparison_column(level: str):
#         return {
#             "region": Store.region.label("comparison_value"),
#             "store": Store.store_id.label("comparison_value"),
#             "brand": Product.brand.label("comparison_value"),
#             "product": Product.product_id.label("comparison_value"),
#         }[level]

#     def _format_trend_result(rows):
#         return [
#             {"period": row[0].strftime("%Y-%m-%d"), "value": float(row[1] or 0)}
#             for row in rows
#         ]

#     def _format_comparison_trend(rows):
#         trend_map = defaultdict(list)
#         for period, comparison_value, value in rows:
#             name = str(comparison_value).split(".")[-1]
#             trend_map[name].append(
#                 {"period": period.strftime("%Y-%m-%d"), "value": float(value or 0)}
#             )
#         return [{"name": name, "trend": trend} for name, trend in trend_map.items()]

#     def _get_segment_column(seg):
#         return {
#             "age": case(
#                 [
#                     (Customer.age < 25, "18-24"),
#                     (Customer.age < 35, "25-34"),
#                     (Customer.age < 50, "35-49"),
#                     (Customer.age < 65, "50-64"),
#                 ],
#                 else_="65+",
#             ).label("segment"),
#             "gender": Customer.gender.label("segment"),
#             "income_bracket": Customer.income_bracket.label("segment"),
#             "country": Customer.country.label("segment"),
#             "marital_status": Customer.marital_status.label("segment"),
#             "education_level": Customer.education_level.label("segment"),
#             "employment_status": Customer.employment_status.label("segment"),
#         }[seg]

#     # ============================
#     # TREND RETURN
#     # ============================
#     if return_trend:
#         # General Trend
#         time_bucket = func.date_trunc(interval, Order.order_date).label("period")
#         general_query = db.query(time_bucket)

#         if metric_name == "Total Customers":
#             general_query = general_query.add_columns(
#                 func.count(distinct(Order.customer_id)).label("value")
#             )
#             general_query = (
#                 general_query.select_from(Order)
#                 .join(OrderItem)
#                 .join(Product)
#                 .join(Customer)
#             )
#             if selected_regions or selected_stores:
#                 general_query = general_query.join(Store)
#             general_query = apply_filters(general_query).group_by("period")
#         elif metric_name == "Average Revenue per Customer":
#             general_query = general_query.add_columns(
#                 (
#                     func.sum(OrderItem.price * OrderItem.quantity)
#                     / func.nullif(func.count(distinct(Order.customer_id)), 0)
#                 ).label("value")
#             )
#             general_query = (
#                 general_query.select_from(Order)
#                 .join(OrderItem)
#                 .join(Product)
#                 .join(Customer)
#             )
#             if selected_regions or selected_stores:
#                 general_query = general_query.join(Store)
#             general_query = apply_filters(general_query).group_by("period")
#         elif metric_name == "Repeat Customer Rate":
#             subq = (
#                 db.query(
#                     Order.customer_id,
#                     func.count(Order.order_id).label("order_count"),
#                     func.date_trunc(interval, Order.order_date).label("period"),
#                 )
#                 .join(OrderItem)
#                 .join(Product)
#                 .join(Customer)
#             )
#             if selected_regions or selected_stores:
#                 subq = subq.join(Store)
#             subq = apply_filters(subq).group_by(Order.customer_id, "period").subquery()

#             general_query = db.query(
#                 subq.c.period,
#                 (
#                     func.sum(case((subq.c.order_count > 1, 1), else_=0))
#                     / func.nullif(func.count(), 0)
#                     * 100
#                 ).label("value"),
#             ).group_by(subq.c.period)
#         elif metric_name == "New Customers":
#             first_purchases = (
#                 db.query(
#                     Order.customer_id,
#                     func.min(Order.order_date).label("first_purchase_date"),
#                 )
#                 .join(OrderItem)
#                 .join(Product)
#                 .join(Customer)
#             )
#             if selected_regions or selected_stores:
#                 first_purchases = first_purchases.join(Store)
#             first_purchases = apply_filters(first_purchases)
#             first_purchases = first_purchases.group_by(Order.customer_id).subquery()

#             general_query = (
#                 db.query(
#                     func.date_trunc(
#                         interval, first_purchases.c.first_purchase_date
#                     ).label("period"),
#                     func.count(distinct(first_purchases.c.customer_id)).label("value"),
#                 )
#                 .filter(
#                     first_purchases.c.first_purchase_date.between(start_date, end_date)
#                 )
#                 .group_by("period")
#             )

#         results = [
#             {"name": "general", "trend": _format_trend_result(general_query.all())}
#         ]

#         # Comparison Trend
#         if comparison_level:
#             comp_col = _get_comparison_column(comparison_level)

#             if metric_name == "Total Customers":
#                 comparison_query = db.query(time_bucket, comp_col)
#                 comparison_query = comparison_query.add_columns(
#                     func.count(distinct(Order.customer_id)).label("value")
#                 )
#                 comparison_query = comparison_query.select_from(Order)
#                 comparison_query = comparison_query.join(
#                     OrderItem, OrderItem.order_id == Order.order_id
#                 )
#                 comparison_query = comparison_query.join(
#                     Product, Product.product_id == OrderItem.product_id
#                 )
#                 comparison_query = comparison_query.join(
#                     Customer, Customer.customer_id == Order.customer_id
#                 )
#                 comparison_query = comparison_query.join(
#                     Store, Store.store_id == Order.store_id
#                 )
#                 comparison_query = apply_filters(comparison_query).group_by(
#                     "period", "comparison_value"
#                 )
#             elif metric_name == "Average Revenue per Customer":
#                 comparison_query = db.query(time_bucket, comp_col)
#                 comparison_query = comparison_query.add_columns(
#                     (
#                         func.sum(OrderItem.price * OrderItem.quantity)
#                         / func.nullif(func.count(distinct(Order.customer_id)), 0)
#                     ).label("value")
#                 )
#                 comparison_query = comparison_query.select_from(Order)
#                 comparison_query = comparison_query.join(
#                     OrderItem, OrderItem.order_id == Order.order_id
#                 )
#                 comparison_query = comparison_query.join(
#                     Product, Product.product_id == OrderItem.product_id
#                 )
#                 comparison_query = comparison_query.join(
#                     Customer, Customer.customer_id == Order.customer_id
#                 )
#                 comparison_query = comparison_query.join(
#                     Store, Store.store_id == Order.store_id
#                 )
#                 comparison_query = apply_filters(comparison_query).group_by(
#                     "period", "comparison_value"
#                 )
#             elif metric_name == "Repeat Customer Rate":
#                 base_query = (
#                     db.query(
#                         Order.customer_id.label("customer_id"),
#                         func.date_trunc(interval, Order.order_date).label("period"),
#                         comp_col.label("comparison_value"),
#                         Order.order_id.label("order_id"),
#                     )
#                     .join(Store, Store.store_id == Order.store_id)
#                     .join(OrderItem, OrderItem.order_id == Order.order_id)
#                     .join(Product, Product.product_id == OrderItem.product_id)
#                     .join(Customer, Customer.customer_id == Order.customer_id)
#                 )
#                 base_query = apply_filters(base_query)
#                 base_subq = base_query.subquery()

#                 customer_orders_subq = (
#                     db.query(
#                         base_subq.c.customer_id,
#                         base_subq.c.period,
#                         base_subq.c.comparison_value,
#                         func.count(base_subq.c.order_id).label("order_count"),
#                     )
#                     .group_by(
#                         base_subq.c.customer_id,
#                         base_subq.c.period,
#                         base_subq.c.comparison_value,
#                     )
#                     .subquery()
#                 )

#                 comparison_query = db.query(
#                     customer_orders_subq.c.period,
#                     customer_orders_subq.c.comparison_value,
#                     (
#                         func.sum(
#                             case((customer_orders_subq.c.order_count > 1, 1), else_=0)
#                         )
#                         / func.nullif(func.count(), 0)
#                         * 100
#                     ).label("value"),
#                 ).group_by(
#                     customer_orders_subq.c.period,
#                     customer_orders_subq.c.comparison_value,
#                 )
#             elif metric_name == "New Customers":
#                 first_purchases = (
#                     db.query(
#                         Order.customer_id,
#                         func.min(Order.order_date).label("first_purchase_date"),
#                         comp_col.label("comparison_value"),
#                     )
#                     .join(OrderItem, OrderItem.order_id == Order.order_id)
#                     .join(Product, Product.product_id == OrderItem.product_id)
#                     .join(Customer, Customer.customer_id == Order.customer_id)
#                 )
#                 if comparison_level in ["region", "store"]:
#                     first_purchases = first_purchases.join(
#                         Store, Store.store_id == Order.store_id
#                     )
#                 first_purchases = apply_filters(first_purchases)
#                 first_purchases = first_purchases.group_by(
#                     Order.customer_id, "comparison_value"
#                 ).subquery()

#                 comparison_query = (
#                     db.query(
#                         func.date_trunc(
#                             interval, first_purchases.c.first_purchase_date
#                         ).label("period"),
#                         first_purchases.c.comparison_value,
#                         func.count(distinct(first_purchases.c.customer_id)).label(
#                             "value"
#                         ),
#                     )
#                     .filter(
#                         first_purchases.c.first_purchase_date.between(
#                             start_date, end_date
#                         )
#                     )
#                     .group_by("period", "comparison_value")
#                 )
#             comparison_rows = comparison_query.all()
#             results += _format_comparison_trend(comparison_rows)

#         return {"trend": results}

#     # ============================
#     # SEGMENTED RETURN
#     # ============================
#     elif segment_by:
#         seg_col = _get_segment_column(segment_by)
#         general_query = db.query(seg_col)

#         if metric_name == "Total Customers":
#             general_query = (
#                 general_query.add_columns(
#                     func.count(distinct(Customer.customer_id)).label("value")
#                 )
#                 .select_from(Customer)
#                 .join(Order)
#                 .join(Store)
#             )
#             general_query = apply_filters(general_query)

#         elif metric_name == "New Customers":
#             general_query = (
#                 general_query.add_columns(
#                     func.count(distinct(Customer.customer_id)).label("value")
#                 )
#                 .select_from(Customer)
#                 .join(Order)
#                 .join(Store)
#             )
#             general_query = general_query.filter(
#                 func.date_trunc("month", Customer.created_at)
#                 == func.date_trunc("month", Order.order_date)
#             )
#             general_query = apply_filters(general_query)

#         elif metric_name == "Average Revenue per Customer":
#             general_query = (
#                 general_query.add_columns(
#                     (
#                         func.sum(OrderItem.price * OrderItem.quantity)
#                         / func.nullif(func.count(distinct(Order.customer_id)), 0)
#                     ).label("value")
#                 )
#                 .select_from(Order)
#                 .join(Customer)
#                 .join(OrderItem)
#                 .join(Product)
#                 .join(Store)
#             )
#             general_query = apply_filters(general_query)

#         elif metric_name == "Repeat Customer Rate":
#             subq = (
#                 db.query(Order.customer_id)
#                 .join(Store)
#                 .filter(Order.order_date.between(start_date, end_date))
#                 .group_by(Order.customer_id)
#                 .having(func.count(Order.order_id) > 1)
#                 .subquery()
#             )
#             general_query = (
#                 general_query.add_columns(
#                     (
#                         func.count(distinct(subq.c.customer_id))
#                         / func.nullif(func.count(distinct(Customer.customer_id)), 0)
#                     ).label("value")
#                 )
#                 .select_from(Customer)
#                 .join(Order)
#                 .join(Store)
#             )
#             general_query = apply_filters(general_query)

#         general_query = general_query.group_by("segment")
#         general_result = [
#             {"name": row.segment, "value": row.value} for row in general_query.all()
#         ]

#         comparison_results = []
#         if comparison_level:

#             def get_comparison_col(level: str):
#                 if level == "region":
#                     return Store.region.label("comparison_value")
#                 elif level == "store":
#                     return Store.store_id.label("comparison_value")
#                 elif level == "brand":
#                     return Product.brand.label("comparison_value")
#                 elif level == "product":
#                     return Product.product_id.label("comparison_value")

#             comparison_col = get_comparison_col(comparison_level)
#             comp_query = db.query(seg_col, comparison_col)

#             if metric_name == "Total Customers":
#                 comp_query = comp_query.add_columns(
#                     func.count(distinct(Customer.customer_id)).label("value")
#                 )
#                 comp_query = comp_query.select_from(Customer).join(Order).join(Store)
#                 comp_query = apply_filters(comp_query)
#             elif metric_name == "New Customers":
#                 comp_query = comp_query.add_columns(
#                     func.count(distinct(Customer.customer_id)).label("value")
#                 )
#                 comp_query = comp_query.select_from(Customer).join(Order).join(Store)
#                 comp_query = comp_query.filter(
#                     func.date_trunc("month", Customer.created_at)
#                     == func.date_trunc("month", Order.order_date)
#                 )
#                 comp_query = apply_filters(comp_query)
#             elif metric_name == "Average Revenue per Customer":
#                 comp_query = comp_query.add_columns(
#                     (
#                         func.sum(OrderItem.price * OrderItem.quantity)
#                         / func.nullif(func.count(distinct(Order.customer_id)), 0)
#                     ).label("value")
#                 )
#                 comp_query = (
#                     comp_query.select_from(Order)
#                     .join(Customer)
#                     .join(OrderItem)
#                     .join(Product)
#                     .join(Store)
#                 )
#                 comp_query = apply_filters(comp_query)
#             elif metric_name == "Repeat Customer Rate":
#                 subq = (
#                     db.query(Order.customer_id.label("rep_customer_id"), comparison_col)
#                     .join(Store)
#                     .filter(Order.order_date.between(start_date, end_date))
#                     .group_by(Order.customer_id, comparison_col)
#                     .having(func.count(Order.order_id) > 1)
#                     .subquery()
#                 )
#                 comp_query = comp_query.add_columns(
#                     (
#                         func.count(distinct(subq.c.rep_customer_id))
#                         / func.nullif(func.count(distinct(Customer.customer_id)), 0)
#                     ).label("value")
#                 )
#                 comp_query = comp_query.select_from(Customer).join(Order).join(Store)
#                 comp_query = apply_filters(comp_query)

#             comp_query = comp_query.group_by("segment", "comparison_value")
#             rows = comp_query.all()

#             grouped = defaultdict(list)
#             for row in rows:
#                 grouped[row.comparison_value].append(
#                     {"name": row.segment, "value": row.value}
#                 )

#             comparison_results = [
#                 {"compare_value": k, "data": v} for k, v in grouped.items()
#             ]

#         return {
#             "seg": segment_by,
#             "metric": metric_name,
#             "general": general_result,
#             "comparison": comparison_results,  # Fill in if comparison_level is used
#         }

#     # ============================
#     # BASIC METRIC RETURN
#     # ============================
#     else:
#         valid_comparison_levels = ["region", "store", "brand", "product"]
#     if comparison_level not in valid_comparison_levels:
#         raise ValueError(
#             f"Invalid comparison level. Must be one of: {valid_comparison_levels}"
#         )
#     end_date = end_date or datetime.now(timezone.utc)

#     # Get comparison data once
#     comparison_data = _get_all_comparison_data(
#         db=db,
#         comparison_level=comparison_level,
#         selected_regions=selected_regions,
#         selected_stores=selected_stores,
#         selected_brands=selected_brands,
#         selected_products=selected_products,
#         start_date=start_date,
#         end_date=end_date,
#     )

#     results = []

#     metrics = [
#         ("Total Customers", "total_customers"),
#         ("Average Revenue per Customer", "avg_revenue_per_customer"),
#         ("New Customers", "new_customers"),
#         ("Repeat Customer Rate", "repeat_customer_rate"),
#     ]

#     for metric_name, metric_key in metrics:
#         comparisons = comparison_data.get(metric_key, {}).get("comparisons", [])
#         total_value = sum(float(item.get("value", 0)) for item in comparisons)

#         results.append(
#             {
#                 "metric_name": metric_name,
#                 "total_value": total_value,
#                 "comparisons": comparisons,
#             }
#         )

#     return results
