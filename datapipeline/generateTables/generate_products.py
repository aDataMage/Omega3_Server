import pandas as pd
import numpy as np
import random
from uuid import uuid4
from datetime import datetime, timezone
from utils.common import DATA_DIR
from enumsC import (
    CategoryEnum,
    BrandEnum,
)


def generate_static_products(num_products=300, seed=42):
    categories = [cat.value for cat in CategoryEnum]
    brands = [brand.value for brand in BrandEnum]

    product_templates = {
        "electronics": ["Laptop", "Smartphone", "Tablet", "Smartwatch", "TV"],
        "fashion": ["Shirt", "Pants", "Shoes", "Jacket", "Hat"],
        "home_appliances": [
            "Blender",
            "Microwave",
            "Toaster",
            "Vacuum",
            "Air Conditioner",
        ],
        "beauty": ["Lipstick", "Foundation", "Perfume", "Moisturizer", "Shampoo"],
        "sports": [
            "Tennis Racket",
            "Basketball",
            "Yoga Mat",
            "Running Shoes",
            "Helmet",
        ],
        "books": ["Novel", "Biography", "Mystery", "Fantasy", "Self-Help"],
        "toys": ["Puzzle", "Action Figure", "Doll", "Lego Set", "Board Game"],
        "automotive": ["Tire", "Engine Oil", "Car Battery", "Air Filter", "Brake Pads"],
        "groceries": ["Milk", "Eggs", "Bread", "Chicken", "Rice"],
        "furniture": ["Chair", "Table", "Sofa", "Desk", "Bookshelf"],
        "health": [
            "Vitamins",
            "Pain Reliever",
            "Thermometer",
            "Bandages",
            "Hand Sanitizer",
        ],
    }

    random.seed(seed)
    np.random.seed(seed)

    # Flatten category-product pairs
    products = []
    for category in categories:
        for name in product_templates.get(category, [category.title() + " Item"]):
            products.append((name, category))

    repeat_factor = (num_products // len(products)) + 1
    products = (products * repeat_factor)[:num_products]

    now = datetime.now(timezone.utc)
    cost = np.round(np.random.uniform(2, 400, num_products), 2)
    price = np.round(cost + np.random.uniform(10, 200, num_products), 2)

    df = pd.DataFrame(
        {
            "product_id": [str(uuid4()) for _ in range(num_products)],
            "name": [name for name, _ in products],
            "price": price,
            "cost": cost,
            "brand": np.random.choice(brands, size=num_products),
            "category": [cat for _, cat in products],
            "stock_quantity": np.random.randint(10, 1000, num_products),
            "created_at": [now] * num_products,
            "updated_at": [now] * num_products,
        }
    )

    products_path = DATA_DIR / "products"
    products_path.mkdir(parents=True, exist_ok=True)
    df.to_csv(products_path / "products.csv", index=False)

    print(f"📦 {num_products} products with valid categories saved.")
    return df
