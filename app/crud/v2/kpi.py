from fastapi import Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from db.session import get_db
from models.OrderModel import Order
from models.OrderItemsModel import OrderItem
from models.ProductModel import Product
from models.StoreModel import Store
from models.ReturnsModel import Return as Returns
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Tuple, Union, Any
from dateutil.relativedelta import relativedelta


class KPICrud:
    def __init__(self, db: Session = Depends(get_db)):
        pass

    # -- Helper functions for KPI calculations---#
    def _calculate_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> Dict[str, Any]:
        month_diff = (
            (end_date.year - start_date.year) * 12
            + (end_date.month - start_date.month)
            + 1
        )
        month_diff = min(month_diff, 3)
        range_length = (end_date - start_date).days + 1
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=range_length - 1)
        return {
            "month_diff": month_diff,
            "range_length": range_length,
            "prev_start": prev_start,
            "prev_end": prev_end,
        }

    from datetime import timedelta

    def _calculate_trend_data(
        self,
        db,
        base_model: Any,
        date_field: Any,
        value_expression: Any,
        start_date: datetime,
        end_date: datetime,
        group_by: str = "day",
        required_joins: Optional[List[Tuple[Any, Any]]] = None,
        additional_filters: Optional[List[Any]] = None,
    ) -> List[Dict[str, Any]]:
        date_trunc_unit = "day" if group_by == "day" else "month"

        # Run the grouped query
        query = db.query(
            func.date_trunc(date_trunc_unit, date_field).label("bucket"),
            value_expression.label("bucket_total"),
        )
        if required_joins:
            for model, condition in required_joins:
                query = query.join(model, condition)
        if additional_filters:
            for filter_condition in additional_filters:
                query = query.filter(filter_condition)
        query = query.filter(date_field.between(start_date, end_date))
        query = query.group_by("bucket").order_by("bucket")

        results = {
            row.bucket.strftime(
                "%Y-%m-%d" if group_by == "day" else "%Y-%m"
            ): row.bucket_total
            for row in query.all()
        }

        # Generate all expected dates
        buckets = []
        current = start_date.replace(day=1) if group_by == "month" else start_date
        while current <= end_date:
            bucket_label = current.strftime(
                "%Y-%m-%d" if group_by == "day" else "%Y-%m"
            )
            buckets.append(bucket_label)
            current += (
                timedelta(days=1) if group_by == "day" else relativedelta(months=1)
            )

        # Return list with 0-filled gaps
        return [
            {
                "date": bucket,
                "value": results.get(bucket, 0),
            }
            for bucket in buckets
        ]

    def _format_kpi_response(
        self,
        name,
        total,
        percentage_change,
        trend_data,
        previous_total,
        previous_start,
        previous_end,
        start_date,
        end_date,
    ):
        return {
            "metric_name": name,
            "total_value": total if total else 0,
            "percentage_change": percentage_change,
            "trend_data": trend_data,
            "previous_total": previous_total,
            "current_date_range": [
                {start_date.strftime("%b %d, %Y")},
                {end_date.strftime("%b %d, %Y")},
            ],
            "previous_date_range": [
                {previous_start.strftime("%b %d, %Y")},
                {previous_end.strftime("%b %d, %Y")},
            ],
        }

    # -- End --#

    def get_total_sales(
        self, db: Session, start_date: datetime, end_date: Optional[datetime] = None
    ):
        end_date = end_date or datetime.now(timezone.utc)
        date_range = self._calculate_date_range(start_date, end_date)
        _, range_length, prev_start, prev_end = (
            date_range["month_diff"],
            date_range["range_length"],
            date_range["prev_start"],
            date_range["prev_end"],
        )
        total = (
            db.query(func.sum(OrderItem.price * OrderItem.quantity))
            .join(Order)
            .filter(Order.order_date.between(start_date, end_date))
            .scalar()
        ) or 0.0
        prev_total = (
            db.query(func.sum(OrderItem.price * OrderItem.quantity))
            .join(Order)
            .filter(Order.order_date.between(prev_start, prev_end))
            .scalar()
        ) or 0.0
        percentage_change = (
            (total - prev_total) / prev_total * 100 if prev_total > 0 else 0.0
        )
        group_by = "day" if range_length <= 31 else "month"
        trend_data = self._calculate_trend_data(
            base_model=OrderItem,
            db=db,
            date_field=Order.order_date,
            value_expression=func.sum(OrderItem.price * OrderItem.quantity),
            start_date=start_date,
            end_date=end_date,
            group_by=group_by,
            required_joins=[
                (Order, Order.order_id == OrderItem.order_id),
                (Product, Product.product_id == OrderItem.product_id),
            ],
        )
        return self._format_kpi_response(
            "Total Sales",
            total,
            percentage_change,
            trend_data,
            prev_total,
            prev_start,
            prev_end,
            start_date,
            end_date,
        )

    def get_total_orders(
        self, db: Session, start_date: datetime, end_date: Optional[datetime] = None
    ):
        end_date = end_date or datetime.now(timezone.utc)
        date_range = self._calculate_date_range(start_date, end_date)
        _, range_length, prev_start, prev_end = (
            date_range["month_diff"],
            date_range["range_length"],
            date_range["prev_start"],
            date_range["prev_end"],
        )
        total = (
            db.query(func.count(Order.order_id))
            .filter(Order.order_date.between(start_date, end_date))
            .scalar()
        ) or 0
        prev_total = (
            db.query(func.count(Order.order_id))
            .filter(Order.order_date.between(prev_start, prev_end))
            .scalar()
        ) or 0
        percentage_change = (
            ((total - prev_total) / prev_total) * 100 if prev_total > 0 else 0.0
        )
        group_by = "day" if range_length <= 31 else "month"
        trend_data = self._calculate_trend_data(
            base_model=Order,
            db=db,
            date_field=Order.order_date,
            value_expression=func.count(Order.order_id),
            start_date=start_date,
            end_date=end_date,
            group_by=group_by,
            required_joins=[],
        )
        return self._format_kpi_response(
            "Total Orders",
            total,
            percentage_change,
            trend_data,
            prev_total,
            prev_start,
            prev_end,
            start_date,
            end_date,
        )

    def get_total_profit(
        self, db: Session, start_date: datetime, end_date: Optional[datetime] = None
    ):
        end_date = end_date or datetime.now(timezone.utc)
        total = (
            db.query(func.sum((Product.price - Product.cost) * OrderItem.quantity))
            .select_from(OrderItem)
            .join(Product)
            .join(Order)
            .filter(Order.order_date.between(start_date, end_date))
            .scalar()
        )
        date_range = self._calculate_date_range(start_date, end_date)
        _, range_length, prev_start, prev_end = (
            date_range["month_diff"],
            date_range["range_length"],
            date_range["prev_start"],
            date_range["prev_end"],
        )
        prev_total = (
            db.query(func.sum((Product.price - Product.cost) * OrderItem.quantity))
            .select_from(OrderItem)
            .join(Product)
            .join(Order)
            .filter(Order.order_date.between(prev_start, prev_end))
            .scalar()
        )
        percentage_change = (
            ((total - prev_total) / prev_total) * 100 if prev_total else 0.0
        )
        group_by = "day" if range_length <= 31 else "month"
        trend_data = self._calculate_trend_data(
            base_model=OrderItem,
            db=db,
            date_field=Order.order_date,
            value_expression=func.sum(
                (Product.price - Product.cost) * OrderItem.quantity
            ),
            start_date=start_date,
            end_date=end_date,
            group_by=group_by,
            required_joins=[
                (Order, Order.order_id == OrderItem.order_id),
                (Product, Product.product_id == OrderItem.product_id),
            ],
        )
        return self._format_kpi_response(
            "Total Profit",
            total,
            percentage_change,
            trend_data,
            prev_total,
            prev_start,
            prev_end,
            start_date,
            end_date,
        )

    def get_total_returns(
        self, db: Session, start_date: datetime, end_date: Optional[datetime] = None
    ):
        end_date = end_date or datetime.now(timezone.utc)
        total = (
            db.query(func.count(Returns.return_id))
            .filter(Returns.return_date.between(start_date, end_date))
            .scalar()
        )
        date_range = self._calculate_date_range(start_date, end_date)
        _, range_length, prev_start, prev_end = (
            date_range["month_diff"],
            date_range["range_length"],
            date_range["prev_start"],
            date_range["prev_end"],
        )
        prev_total = (
            db.query(func.count(Returns.return_id))
            .filter(Returns.return_date.between(prev_start, prev_end))
            .scalar()
        )
        percentage_change = (
            ((total - prev_total) / prev_total) * 100 if prev_total else 0.0
        )
        group_by = "day" if range_length <= 31 else "month"
        trend_data = self._calculate_trend_data(
            base_model=Returns,
            db=db,
            date_field=Returns.return_date,
            value_expression=func.count(Returns.return_id),
            start_date=start_date,
            end_date=end_date,
            group_by=group_by,
            required_joins=[
                (OrderItem, OrderItem.order_item_id == Returns.order_item_id),
                (Order, Order.order_id == OrderItem.order_id),
                (Product, Product.product_id == OrderItem.product_id),
            ],
        )
        return self._format_kpi_response(
            "Total Returns",
            total,
            percentage_change,
            trend_data,
            prev_total,
            prev_start,
            prev_end,
            start_date,
            end_date,
        )

    def fetch_insights(
        self,
        db,
        comparison_level: str,
        metric: str,
        selected_regions: List[str] = None,
        selected_stores: List[str] = None,
        selected_brands: List[str] = None,
        selected_products: List[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Union[List[Dict[str, Any]], List[Dict[str, Any]]]]:
        valid_comparison_levels = ["region", "store", "brand", "product"]
        if comparison_level not in valid_comparison_levels:
            raise ValueError(
                f"Invalid comparison level. Must be one of: {valid_comparison_levels}"
            )
        valid_metrics = {
            "Total Sales": func.sum(OrderItem.price * OrderItem.quantity),
            "Total Orders": func.count(OrderItem.order_item_id),
            "Total Returns": func.count(Returns.return_id),
            "Total Profit": func.sum(
                (OrderItem.price - Product.cost) * OrderItem.quantity
            ),
        }
        if metric not in valid_metrics:
            raise ValueError(
                f"Invalid metric. Must be one of: {list(valid_metrics.keys())}"
            )
        end_date = end_date or datetime.now(timezone.utc)
        current_results = self._fetch_insights_data(
            comparison_level=comparison_level,
            db=db,
            metric=metric,
            selected_regions=selected_regions,
            selected_stores=selected_stores,
            selected_brands=selected_brands,
            selected_products=selected_products,
            start_date=start_date,
            end_date=end_date,
            metric_expression=valid_metrics[metric],
        )
        if start_date and end_date:
            prev_start = start_date - (end_date - start_date) - timedelta(days=1)
            prev_end = start_date - timedelta(days=1)
            prev_results = self._fetch_insights_data(
                comparison_level=comparison_level,
                db=db,
                metric=metric,
                selected_regions=selected_regions,
                selected_stores=selected_stores,
                selected_brands=selected_brands,
                selected_products=selected_products,
                start_date=prev_start,
                end_date=prev_end,
                metric_expression=valid_metrics[metric],
            )
            prev_lookup = {
                item["comparison_value"]: item for item in prev_results["summary"]
            }
            for current in current_results["summary"]:
                prev_value = prev_lookup.get(current["comparison_value"], {}).get(
                    "metric_value", 0
                )
                curr_value = current["metric_value"]
                if prev_value > 0:
                    current["percentage_change"] = (
                        (curr_value - prev_value) / prev_value
                    ) * 100
                else:
                    current["percentage_change"] = 0.0 if curr_value == 0 else 100.0
        return current_results

    def _fetch_insights_data(
        self,
        db: Session,
        comparison_level: str,
        metric: str,
        selected_regions: List[str],
        selected_stores: List[str],
        selected_brands: List[str],
        selected_products: List[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        metric_expression: Any,
    ) -> Dict[str, List]:
        if metric == "Total Returns":
            date_field = Returns.return_date
        else:
            date_field = Order.order_date

        def build_base_query(query, include_date=False, date_trunc_unit="month"):
            if comparison_level == "region":
                query = query.add_columns(Store.region.label("comparison_value"))
            elif comparison_level == "store":
                query = query.add_columns(
                    Store.name.label("comparison_value"),
                )
            elif comparison_level == "brand":
                query = query.add_columns(Product.brand.label("comparison_value"))
            elif comparison_level == "product":
                query = query.add_columns(Product.name.label("comparison_value"))
            if include_date:
                query = query.add_columns(
                    func.date_trunc(date_trunc_unit, date_field).label("date")
                )
            query = query.add_columns(metric_expression.label("metric_value"))
            if metric in [
                "Total Sales",
                "Total Orders",
                "Total Profit",
                "Total Customers",
            ]:
                query = query.select_from(OrderItem)
                query = query.join(Order, Order.order_id == OrderItem.order_id)
                query = query.join(Product, Product.product_id == OrderItem.product_id)
                if comparison_level in ["region", "store"]:
                    query = query.join(Store, Store.store_id == Order.store_id)
            elif metric == "Total Returns":
                query = query.select_from(Returns)
                query = query.join(
                    OrderItem, OrderItem.order_item_id == Returns.order_item_id
                )
                query = query.join(Order, Order.order_id == OrderItem.order_id)
                query = query.join(Product, Product.product_id == OrderItem.product_id)
                if comparison_level in ["region", "store"]:
                    query = query.join(Store, Store.store_id == Order.store_id)
            if selected_regions:
                query = query.filter(Store.region.in_(selected_regions))
            if selected_stores:
                query = query.filter(Store.store_id.in_(selected_stores))
            if selected_brands:
                query = query.filter(Product.brand.in_(selected_brands))
            if selected_products:
                query = query.filter(Product.name.in_(selected_products))
            if start_date and end_date:
                query = query.filter(date_field.between(start_date, end_date))
            return query

        summary_query = build_base_query(db.query())
        group_by_fields = []
        if comparison_level == "region":
            group_by_fields.append(Store.region)
        elif comparison_level == "store":
            group_by_fields.extend([Store.store_id, Store.name])
        elif comparison_level == "brand":
            group_by_fields.append(Product.brand)
        elif comparison_level == "product":
            group_by_fields.append(Product.name)
        summary_query = summary_query.group_by(*group_by_fields)
        summary_results = summary_query.all()
        formatted_summary = []
        for row in summary_results:
            result = {
                "comparison_value": row.comparison_value,
                "metric_value": float(row.metric_value or 0),
                "metric_name": metric,
            }
            if hasattr(row, "store_name"):
                result["store_name"] = row.store_name
            formatted_summary.append(result)
        trend_data = []
        if start_date and end_date:
            trend_query = build_base_query(
                db.query(),
                include_date=True,
                date_trunc_unit="month",
            )
            trend_group_by = group_by_fields.copy()
            trend_group_by.append(func.date_trunc("month", date_field))
            trend_query = trend_query.group_by(*trend_group_by)
            trend_results = trend_query.order_by("comparison_value").all()
            for row in trend_results:
                trend_data.append(
                    {
                        "comparison_value": row.comparison_value,
                        "date": row.date.isoformat(),
                        "metric_value": float(row.metric_value or 0),
                    }
                )
        return {"summary": formatted_summary, "trend": trend_data}

    def get_all_kpi(
        self, db: Session, start_date: datetime, end_date: Optional[datetime] = None
    ):
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        end_date = end_date or datetime.now(timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        total_sales = self.get_total_sales(db, start_date, end_date)
        total_orders = self.get_total_orders(db, start_date, end_date)
        total_profit = self.get_total_profit(db, start_date, end_date)
        total_returns = self.get_total_returns(db, start_date, end_date)
        return [
            total_sales,
            total_profit,
            total_orders,
            total_returns,
        ]
