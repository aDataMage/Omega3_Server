import pandas as pd
import random
from uuid import uuid4
from faker import Faker
from datetime import datetime, timezone
from utils.common import DATA_DIR
from enumsC import RegionEnum

fake = Faker()


def generate_stores(num_stores=20):
    store_ids = [str(uuid4()) for _ in range(num_stores)]
    region_values = [region.value for region in RegionEnum]

    stores = pd.DataFrame(
        {
            "store_id": store_ids,
            "manager_name": [fake.name() for _ in range(num_stores)],
            "name": [f"{fake.company()} Store" for _ in range(num_stores)],
            "created_at": [datetime.now(timezone.utc) for _ in range(num_stores)],
            "updated_at": [datetime.now(timezone.utc) for _ in range(num_stores)],
            "is_active": True,
            "region": [random.choice(region_values) for _ in range(num_stores)],
        }
    )

    stores_path = DATA_DIR / "stores"
    stores_path.mkdir(parents=True, exist_ok=True)
    stores.to_csv(stores_path / "stores.csv", index=False)
    print("🏬 Stores generated and saved.")
    return stores
