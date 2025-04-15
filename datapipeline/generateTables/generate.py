from pathlib import Path
from typing import List
from faker import Faker
import pandas as pd
import numpy as np
import random
from uuid import uuid4
from datetime import datetime, timedelta
from enumsC import (
    EducationLevelEnum,
    EmploymentStatusEnum,
    IncomeRangeEnum,
    MaritalStatusEnum,
    RegionEnum,
)
from schemas.CustomerSchema import Customer
import sys
from pathlib import Path

# Add your project root to PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # adjust if needed
sys.path.insert(0, str(PROJECT_ROOT))

# Set seeds for reproducibility
fake = Faker()

# Date string for unique ID suffixing
today_str = datetime.today().strftime("%Y%m%d")
today = datetime.today()
first_day_last_month = today.replace(day=1) - timedelta(days=1)
first_day_last_month = first_day_last_month.replace(day=1)

# Calculate the last day of the last month
last_day_last_month = first_day_last_month.replace(day=28) + timedelta(
    days=4
)  # this will give us the last day of the month
last_day_last_month = last_day_last_month - timedelta(days=last_day_last_month.day)


# Record sizes
NUM_CUSTOMERS = np.random.randint(10, 30)
NUM_ORDERS = np.random.randint(5000, 10000)
NUM_ORDER_ITEMS = NUM_ORDERS + np.random.randint(300, 1000)
NUM_RETURNS = np.floor(NUM_ORDER_ITEMS * np.random.uniform(0.05, 0.15)).astype(int)

SCRIPT_DIR = Path(__file__).parent.parent

customer_data = pd.read_csv(SCRIPT_DIR / "data/raw/customers/customers.csv")
products_df = pd.read_csv(SCRIPT_DIR / "data/raw/products/products.csv")
stores = pd.read_csv(SCRIPT_DIR / "data/raw/stores/stores.csv")

customer_ids = [str(uuid4()) for _ in range(NUM_CUSTOMERS)]
# Get existing customer IDs
existing_customers: List[str] = customer_data["customer_id"].unique().tolist()
all_customer_ids = customer_ids + existing_customers


def customers_gen():
    region_values = [region.value for region in RegionEnum]
    income_range_values = [income.value for income in IncomeRangeEnum]
    marital_status_values = [status.value for status in MaritalStatusEnum]
    education_level_values = [level.value for level in EducationLevelEnum]
    employment_status_values = [status.value for status in EmploymentStatusEnum]

    customers_data = [
        {
            "customer_id": customer_ids[i],
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "password_hash": fake.password(),
            "age": np.random.randint(18, 70),
            "gender": random.choice(["Male", "Female", "Non-Binary"]),
            "income_bracket": random.choice(income_range_values),
            "country": fake.country(),
            "region": random.choice(region_values),
            "phone_number": f"{fake.country_calling_code()}-{random.randint(1000000, 9999999)}",  # Using custom phone number provider
            "marital_status": random.choice(marital_status_values),
            "education_level": random.choice(education_level_values),
            "employment_status": random.choice(employment_status_values),
            "created_at": fake.date_between(
                start_date=first_day_last_month, end_date=last_day_last_month
            ),
            "updated_at": datetime.now(),  # Set the same time for simplicity
        }
        for i in range(NUM_CUSTOMERS)
    ]

    customers = [Customer.model_validate(customer) for customer in customers_data]

    # Convert the list of Pydantic models into a DataFrame
    customers_df = pd.DataFrame([customer.model_dump() for customer in customers])

    # Inject errors
    customers_df.loc[
        random.sample(
            range(NUM_CUSTOMERS),
            np.floor(NUM_CUSTOMERS * np.random.uniform(0.01, 0.05)).astype(int),
        ),
        "age",
    ] = np.nan
    customers_df.loc[
        random.sample(
            range(NUM_CUSTOMERS),
            np.floor(NUM_CUSTOMERS * np.random.uniform(0.01, 0.05)).astype(int),
        ),
        "region",
    ] = ""
    customers_df.loc[
        random.sample(
            range(NUM_CUSTOMERS),
            np.floor(NUM_CUSTOMERS * np.random.uniform(0.01, 0.05)).astype(int),
        ),
        "gender",
    ] = "Unknown"
    customers_df = pd.concat([customers_df, customers_df.sample(10)])  # Duplicates

    return customers_df


def order_gen(all_customers_ids):
    order_statuses = [
        "pending",
        "processing",
        "shipped",
        "delivered",
        "cancelled",
        "refunded",
    ]

    payment_statuses = [
        "pending",
        "processing",
        "shipped",
        "cancelled",
        "refunded",
        "delivered",
    ]
    order_ids = [str(uuid4()) for _ in range(1, NUM_ORDERS + 1)]
    order_dates = [
        fake.date_between(start_date=first_day_last_month, end_date=last_day_last_month)
        for _ in range(NUM_ORDERS)
    ]
    orders = pd.DataFrame(
        {
            "order_id": order_ids,
            "customer_id": random.choices(all_customers_ids, k=NUM_ORDERS),
            "order_date": order_dates,
            "total_amount": [
                round(random.uniform(10, 1000), 2) for _ in range(NUM_ORDERS)
            ],
            "payment_method": [
                random.choice(
                    [
                        "paypal",
                        "credit_card",
                        "debit_card",
                        "stripe",
                        "cash_on_delivery",
                    ]
                )
                for _ in range(NUM_ORDERS)
            ],
            "payment_status": random.choice(payment_statuses),
            "created_at": [order_dates for _ in range(NUM_ORDERS)],
            "updated_at": datetime.now(),
            "status": [random.choice(order_statuses) for _ in range(NUM_ORDERS)],
            "store_id": random.choices(stores["store_id"], k=NUM_ORDERS),
        }
    )

    orders.loc[random.sample(range(NUM_ORDERS), 20), "total_amount"] = 0
    orders.loc[random.sample(range(NUM_ORDERS), 15), "payment_method"] = "Cash"
    orders = pd.concat([orders, orders.sample(5)])  # Duplicates
    return orders


def order_item_gen(order_ids):
    order_items = pd.DataFrame(
        {
            "order_item_id": [
                f"ITEM_{today_str}_{i}" for i in range(1, NUM_ORDER_ITEMS + 1)
            ],
            "order_id": random.choices(order_ids, k=NUM_ORDER_ITEMS),
            "product_id": random.choices(products_df["product_id"], k=NUM_ORDER_ITEMS),
            "quantity": [random.randint(1, 5) for _ in range(NUM_ORDER_ITEMS)],
        }
    )
    discount_chance = np.random.rand(NUM_ORDER_ITEMS)
    order_items["discount_applied"] = np.where(
        discount_chance < 0.7,  # 70% chance to have a discount
        np.round(np.random.uniform(0.00, 0.50, NUM_ORDER_ITEMS), 2),
        None,
    )
    order_items = order_items.merge(
        products_df[["product_id", "price"]], on="product_id", how="left"
    )
    order_items["total_price"] = np.round(
        order_items["quantity"]
        * order_items["price"]
        * (1 - order_items["discount_applied"] / 100),
        2,
    )
    order_items.loc[random.sample(range(NUM_ORDER_ITEMS), 10), "quantity"] = 0
    order_items.loc[random.sample(range(NUM_ORDER_ITEMS), 15), "total_price"] = np.nan
    order_items = pd.concat([order_items, order_items.sample(10)])  # Duplicates
    return order_items


# === Generate Returns ===
def returns_gen(order_items_df, order_df):
    REFUND_POLICY = {
        "Defective": 1.0,
        "Wrong Item": 0.5,
        "Not Satisfied": 0.2,
        "Other": 0.1,
    }
    returns = pd.DataFrame(
        {
            "return_id": [f"RET_{today_str}_{i}" for i in range(1, NUM_RETURNS + 1)],
            "order_item_id": random.sample(
                order_items_df["order_item_id"].tolist(), NUM_RETURNS
            ),
            "reason": random.choices(list(REFUND_POLICY.keys()), k=NUM_RETURNS),
            "return_status": [
                random.choice(["Approved", "Rejected", "Pending"])
                for _ in range(NUM_RETURNS)
            ],
        }
    )

    returns = returns.merge(
        order_items_df[["order_item_id", "price", "order_id"]],
        on="order_item_id",
        how="left",
    )

    returns = returns.merge(
        order_df[["order_id", "order_date"]], on="order_id", how="left"
    )
    returns["return_date"] = [
        fake.date_between(start_date=odate, end_date=odate + pd.Timedelta(days=30))
        for odate in pd.to_datetime(returns["order_date"])
    ]

    def calculate_refund(row):
        if row["return_status"] in ["rejected", "initiated"]:
            return 0
        return round(row["price"] * REFUND_POLICY.get(row["reason"], 0), 2)

    returns["refund_amount"] = returns.apply(calculate_refund, axis=1)
    returns_df = returns[
        [
            "return_id",
            "order_item_id",
            "reason",
            "return_date",
            "refund_amount",
            "return_status",
        ]
    ]

    returns_df.loc[random.sample(range(NUM_RETURNS), 8), "refund_amount"] = -50
    returns_df.loc[random.sample(range(NUM_RETURNS), 5), "return_status"] = "Unknown"

    return returns_df


# Save to CSV
# Define base path
BASE_PATH = Path("data/dirty")

# Get current timestamp for filename (year_month)
timestamp = datetime.now().strftime("%Y_%m")


# Save each DataFrame to its own folder with timestamped filename
def save_monthly_csv(df, table_name):
    folder = BASE_PATH / table_name
    folder.mkdir(parents=True, exist_ok=True)
    file_path = folder / f"{timestamp}.csv"
    df.to_csv(file_path, index=False, quoting=1)
    print(f"✅ Saved {table_name} data to {file_path}")


customers_df = customers_gen()
order_df = order_gen(all_customer_ids)
order_item_df = order_item_gen(order_df["order_id"].tolist())
returns_df = returns_gen(order_item_df, order_df)

save_monthly_csv(customers_df, "customers")
save_monthly_csv(order_df, "orders")
save_monthly_csv(order_item_df, "order_items")
save_monthly_csv(returns_df, "returns")
