from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pendulum
import subprocess


@dag(
    schedule="59 23 L * *",  # Last day of the month at 11:59 PM
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data_generation"],
)
def generate_monthly_data():
    @task
    def generate():
        subprocess.run(
            [
                "python3",
                "/home/enx/Omega_3/server/datapipeline/generateTables/generate.py",
            ],
            check=True,
        )

    trigger_etl = TriggerDagRunOperator(
        task_id="trigger_dynamic_etl",
        trigger_dag_id="dynamic_etl",  # this must match the DAG ID in your ETL file
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=60,
    )

    generate() >> trigger_etl


generate_monthly_data()
