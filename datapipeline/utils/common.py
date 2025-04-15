from pathlib import Path
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()

DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_NAME = os.getenv("DATABASE_NAME")

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
TABLES = ["products", "customers", "orders", "order_items", "returns", "stores"]


def ensure_dirs():
    for table in TABLES:
        (DATA_DIR / table).mkdir(parents=True, exist_ok=True)


engine = create_engine(
    f"postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}",
    echo=True,
)
