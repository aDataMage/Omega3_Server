from airflow.operators.python import get_current_context
import sys
from pathlib import Path
from airflow.decorators import dag, task
import pendulum
import pandas as pd
import psycopg2
from datetime import datetime
import logging

PG_CONN_STR = (
    "dbname=ecommdb user=adatamage password=adatamage2002 host=localhost port=5432"
)
# Set Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from utils.logger import DataQualityLogger
    from plugins.operators.table_cleaner import TableCleanOperator
except ImportError as e:
    raise ImportError(f"Failed to import modules: {e}")

DAG_DIR = Path(__file__).parent
dq_logger = DataQualityLogger(
    log_method="file", log_path=str(PROJECT_ROOT / "logs/table"), db_uri=None
)


@dag(
    dag_id="dynamic_etl",
    start_date=pendulum.datetime(2023, 1, 1),
    catchup=False,
    tags=["etl"],
)
def dynamic_etl():
    @task
    def get_timestamp():
        context = get_current_context()
        ts = context["execution_date"]
        timestamp_str = str(ts.strftime("%Y_%m"))
        return {"timestamp": timestamp_str}  # explicitly ensure string

    @task
    def get_files():
        return ["customers", "orders", "returns", "order_items"]

    @task(multiple_outputs=True)
    def extract(file: str, timestamp: dict) -> dict:
        logging.info(
            f"Extracting data from {file} for timestamp {timestamp['timestamp']}"
        )
        file_path = (
            DAG_DIR.parent / "data" / "dirty" / file / f"{timestamp['timestamp']}.csv"
        )
        df = pd.read_csv(file_path)
        dq_logger.log_stats(df, file)
        return {"dataframe": df.to_dict(), "filename": file}

    def clean_with_operator(file_info: dict, timestamp):
        cleaned_data_path = (
            DAG_DIR.parent
            / "data"
            / "clean"
            / file_info["filename"]
            / f"{timestamp['timestamp']}_clean.csv"
        )
        cleaned_df = TableCleanOperator(
            task_id=f"clean_{file_info['filename']}",
            table_name=file_info["filename"],
            input_data=file_info["dataframe"],
            cleaning_rules={},
        ).execute(context=get_current_context())

        cleaned_df.to_csv(cleaned_data_path, index=False)
        return {"filename": file_info["filename"], "file_path": str(cleaned_data_path)}

    # @task
    # def load_to_sql_task(file_info: dict, timestamp):
    #     log_filename = f"data_load_log_{timestamp}.txt"

    #     with open(log_filename, "a") as log_file:
    #         conn = psycopg2.connect(PG_CONN_STR)
    #         cursor = conn.cursor()

    #         cleaned_file_path = file_info["file_path"]

    #         try:
    #             # Load to PostgreSQL using COPY for efficient bulk insert
    #             with open(cleaned_file_path, "r") as f:
    #                 cursor.copy_expert(
    #                     f"COPY {file_info['filename']} FROM STDIN WITH CSV HEADER", f
    #                 )

    #             timestamp_iso = datetime.now().isoformat()
    #             log_file.write(
    #                 f"[{timestamp_iso}] ✅ Successfully loaded {file_info['filename']} into PostgreSQL.\n"
    #             )
    #             print(
    #                 f"✅ Successfully loaded {file_info['filename']} into PostgreSQL."
    #             )

    #         except Exception as e:
    #             timestamp_iso = datetime.now().isoformat()
    #             log_file.write(
    #                 f"[{timestamp_iso}] ⚠️ Failed to load {file_info['filename']}: {str(e)}\n"
    #             )
    #             print(f"⚠️ Failed to load {file_info['filename']}: {str(e)}")

    #         conn.commit()
    #         cursor.close()
    #         conn.close()

    ts_info = get_timestamp()
    files = get_files()
    extracted = extract.expand(file=files, timestamp=[ts_info])
    cleaned = task(clean_with_operator).expand(
        file_info=extracted, timestamp=[ts_info]
    )
    # load_to_sql_task.expand(file_info=cleaned, timestamp=[ts_info["timestamp"]])


dynamic_etl()
