import random
import pandas as pd
import numpy as np
from utils.common import DATA_DIR
from uuid import uuid4, UUID
import hashlib


def generate_consistent_uuid(month_str, i):
    """Generate consistent UUIDv4 based on month_str and index"""
    seed = f"{month_str}_{i}".encode("utf-8")
    # Create SHA-1 hash of the seed
    hash_bytes = hashlib.sha1(seed).digest()
    # Convert to UUID (using bytes 0-16)
    return UUID(bytes=hash_bytes[:16], version=4)


def generate_order_items(timestamp, orders_df, products_df, today_str):
    NUM_ORDER_ITEMS = len(orders_df) + np.random.randint(1000, 3000)
    order_item_ids = [
        generate_consistent_uuid(today_str, i) for i in range(1, NUM_ORDER_ITEMS + 1)
    ]

    order_ids = orders_df["order_id"].tolist()
    product_ids = products_df["product_id"].tolist()

    order_items = pd.DataFrame(
        {
            "order_item_id": order_item_ids,
            "order_id": random.choices(order_ids, k=NUM_ORDER_ITEMS),
            "product_id": random.choices(product_ids, k=NUM_ORDER_ITEMS),
        }
    )

    # Merge with product prices
    order_items = order_items.merge(
        products_df[["product_id", "price"]], on="product_id", how="left"
    )

    # Generate nullable discount values
    discount_chance = np.random.rand(NUM_ORDER_ITEMS)
    order_items["discount_applied"] = np.where(
        discount_chance < 0.7,  # 70% chance to have a discount
        np.round(np.random.uniform(0.20, 0.50, NUM_ORDER_ITEMS), 2),
        0,
    )

    order_items["quantity"] = np.random.randint(1, 10, NUM_ORDER_ITEMS)

    # Calculate total_price
    order_items["total_price"] = np.round(
        order_items["price"]
        * order_items["quantity"]
        * (1 - order_items["discount_applied"]),
        2,
    )

    order_items_path = DATA_DIR / "order_items"
    order_items_path.mkdir(parents=True, exist_ok=True)
    order_items.to_csv(order_items_path / f"{timestamp}.csv", index=False)
    return order_items
