from datetime import datetime
import pandas as pd
import numpy as np
import random
from faker import Faker
from utils.common import DATA_DIR
from uuid import uuid4

fake = Faker()

REFUND_POLICY = {
    "Defective": 1.0,
    "Wrong Item": 0.5,
    "Not Satisfied": 0.2,
    "Other": 0.1,
}

RETURN_STATUSES = ["initiated", "approved", "rejected", "completed"]


def generate_returns(timestamp, order_items_df, month_str, start_date, end_date):
    # Calculate number of returns (5-15% of order items)
    NUM_RETURNS = int(len(order_items_df) * np.random.uniform(0.05, 0.15))

    # Sample order items for returns
    sampled_items = order_items_df.sample(
        n=NUM_RETURNS
    ).copy()  # Use copy to avoid warnings

    # Create returns DataFrame
    returns = pd.DataFrame(
        {
            "return_id": [str(uuid4()) for _ in range(1, NUM_RETURNS + 1)],
            "order_item_id": sampled_items["order_item_id"].values,
            "reason": random.choices(list(REFUND_POLICY.keys()), k=NUM_RETURNS),
            "return_status": random.choices(RETURN_STATUSES, k=NUM_RETURNS),
            "return_date": [
                fake.date_between(start_date=start_date, end_date=end_date)
                for _ in range(NUM_RETURNS)
            ],
            "created_at": [
                datetime.now() - pd.DateOffset(days=random.randint(1, 30))
                for _ in range(NUM_RETURNS)
            ],
            "price": sampled_items[
                "price"
            ].values,  # Bring price directly into the DataFrame
        }
    )

    # Calculate refund amount
    returns["refund_amount"] = returns.apply(
        lambda row: 0
        if row["return_status"] in ["rejected", "initiated"]
        else round(row["price"] * REFUND_POLICY.get(row["reason"], 0), 2),
        axis=1,
    )

    # Select final columns
    returns = returns[
        [
            "return_id",
            "order_item_id",
            "reason",
            "return_date",
            "refund_amount",
            "return_status",
            "created_at",
        ]
    ]

    # Save to CSV
    returns_path = DATA_DIR / "returns"
    returns_path.mkdir(parents=True, exist_ok=True)
    returns.to_csv(returns_path / f"{timestamp}.csv", index=False)

    return returns
