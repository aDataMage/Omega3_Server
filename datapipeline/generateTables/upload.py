import pandas as pd
from pathlib import Path
from utils.common import DATA_DIR, engine
from sqlalchemy import text


def upload_all_tables_to_sql():
    TABLES = TABLES = [
        "stores",
        "customers",
        "products",
        "orders",
        "order_items",
        "returns",
    ]

    for table in TABLES:
        files = sorted((DATA_DIR / table).glob("*.csv"))
        if not files:
            print(f"❌ No files found for table '{table}'.")
            continue
        else:
            print(f"✅ Found file for table '{table}'.")
            print(f"📥 Loading table '{table}' into SQL...")

        df_all = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
        file_path = f"/tmp/{table}.csv"
        df_all.to_csv(file_path, index=False)

        with engine.begin() as conn:
            # Truncate existing data but keep schema and constraints
            print(f"🧹 Truncating table '{table}'...")
            conn.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))

            # Load fresh data
            raw_conn = conn.connection
            with raw_conn.cursor() as cur, open(file_path, "r") as f:
                copy_sql = (
                    f"COPY {table} FROM STDIN WITH CSV HEADER DELIMITER ',' NULL '';"
                )
                cur.copy_expert(copy_sql, f)
            raw_conn.commit()

        print(f"✅ Loaded table '{table}' to SQL.")


if __name__ == "__main__":
    upload_all_tables_to_sql()
