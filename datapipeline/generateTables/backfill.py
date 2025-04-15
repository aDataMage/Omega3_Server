from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from generateTables import (
    generate_stores,
    generate_returns,
    generate_orders,
    generate_order_items,
    generate_products,
    generate_customers,
)
from generateTables.upload import upload_all_tables_to_sql
from utils.common import ensure_dirs


def month_range(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += relativedelta(months=1)


if __name__ == "__main__":
    ensure_dirs()
    store_df = generate_stores.generate_stores(30)
    products_df = generate_products.generate_static_products(
        300,
    )
    customer_pool = generate_customers.generate_and_save_customer_pool()
    start = datetime.today().replace(day=1) - relativedelta(years=5)
    end = datetime.today().replace(day=1) - timedelta(days=1)

    for month in month_range(start, end):
        start_date = month.replace(day=1)
        end_date = (start_date + relativedelta(months=1)) - timedelta(days=1)
        today_str = start_date.strftime("%Y%m%d")
        timestamp = start_date.strftime("%Y_%m")
        # Filter customers active as of this month
        eligible_customers = customer_pool[
            pd.to_datetime(customer_pool["created_at"]) <= start_date
        ].copy()
        if eligible_customers.empty:
            print(f"⚠️ No eligible customers for {timestamp}. Skipping.")
            continue
        active_customers = eligible_customers.sample(
            n=min(eligible_customers.shape[0], np.random.randint(200, 500))
        )
        customer_ids = active_customers["customer_id"].tolist()

        orders_df = generate_orders.generate_orders_for_customers_and_stores(
            timestamp,
            customer_ids,
            store_df["store_id"].tolist(),
            start_date,
            end_date,
        )
        order_items_df = generate_order_items.generate_order_items(
            timestamp,
            orders_df,
            products_df,
            today_str,
        )
        generate_returns.generate_returns(
            timestamp, order_items_df, month, start_date, end_date
        )

        print(f"🤖 Data for {timestamp} generated")

    upload_all_tables_to_sql()
    print("🎉 Full 5-year backfill complete.")
