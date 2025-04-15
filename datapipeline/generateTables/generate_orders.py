import random
from faker import Faker
from datetime import datetime
import numpy as np
import pandas as pd
from uuid import uuid4
from schemas.OrderSchema import OrderSchema as Order
from utils.common import DATA_DIR

fake = Faker()

order_statuses = [
    "pending",
    "processing",
    "shipped",
    "delivered",
    "cancelled",
    "refunded",
]

payment_statuses = {
    "pending": ["pending"],
    "processing": ["pending", "completed", "failed"],
    "shipped": ["completed"],
    "cancelled": ["refunded", "failed"],
    "refunded": ["refunded"],
    "delivered": ["completed"],
}


def generate_order(customer_id, store_id, order_date):
    order_id = str(uuid4())
    order_year = order_date.year

    # Apply status logic based on order date
    if order_year < 2020:
        status = random.choice(["completed", "cancelled", "refunded"])
    else:
        status = random.choice(order_statuses)

    if status in ["pending", "processing"] and order_year < 2020:
        status = random.choice(["completed", "cancelled", "refunded"])

    payment_status = random.choice(payment_statuses[status])
    total_amount = round(random.uniform(10.0, 1000.0), 2)
    payment_method = random.choice(
        ["paypal", "credit_card", "debit_card", "stripe", "cash_on_delivery"]
    )

    return {
        "order_id": order_id,
        "store_id": store_id,
        "customer_id": customer_id,
        "total_amount": total_amount,
        "status": status,
        "order_date": order_date,
        "payment_method": payment_method,
        "payment_status": payment_status,
        "created_at": order_date,
        "updated_at": datetime.now(),
    }


def generate_orders_for_customers_and_stores(
    timestamp,
    customer_ids,
    store_ids,
    start_date,
    end_date,
):
    orders = []
    num_orders = np.random.randint(2000, 5500)
    for _ in range(num_orders):
        order_date = fake.date_between(start_date=start_date, end_date=end_date)
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        order_data = generate_order(customer_id, store_id, order_date)
        order = Order.model_validate(order_data)
        orders.append(order)

    orders_df = pd.DataFrame([o.model_dump() for o in orders])
    orders_df = orders_df[
        [
            "order_id",
            "store_id",
            "customer_id",
            "total_amount",
            "status",
            "order_date",
            "payment_method",
            "payment_status",
            "created_at",
            "updated_at",
        ]
    ]
    # Save as single CSV for this month
    orders_path = DATA_DIR / "orders"
    orders_path.mkdir(parents=True, exist_ok=True)
    orders_df.to_csv(orders_path / f"{timestamp}.csv", index=False)
    return orders_df


# For testing standalone
if __name__ == "__main__":
    start = datetime(2022, 1, 1)
    end = datetime(2022, 1, 31)

    dummy_customers = [str(uuid4()) for _ in range(200)]
    dummy_stores = [str(uuid4()) for _ in range(50)]
    generate_orders_for_customers_and_stores(
        timestamp="2022_01",
        customer_ids=dummy_customers,
        store_ids=dummy_stores,
        start_date=start,
        end_date=end,
        num_orders=1500,
    )
