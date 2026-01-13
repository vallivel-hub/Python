# Runs dbt_base_loader using BashOperatot due to the use of Polars.
# Commented out the running of a second program.
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum

# Define EDT timezone
local_tz = pendulum.timezone("America/New_York")

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'start_date': pendulum.datetime(2025, 11, 1, tz=local_tz),
}

with DAG(
    dag_id='dbt_pscs_loader_prd_prod_dag',
    default_args=default_args,
    start_date=pendulum.datetime(2025, 11, 1, tz=local_tz),
    schedule_interval='30 3 * * *', # Daily at 3:30 AM
    catchup=False,
    description='Runs the dbt_pscs_loader script daily for PROD',
    tags=['dbt', 'pscs'],
) as dag:

    # pythonpath = "/opt/elt-monorepo/pscsSourceLoader:/opt/elt-monorepo/data-movers:/opt/elt-monorepo/shared_libs"
    pythonpath = "/opt/elt-monorepo/pscsSourceLoader:/opt/elt-monorepo/shared_libs"

    run_loader = BashOperator(
        task_id='dbt_pscs_loader',
        bash_command='python /opt/elt-monorepo/pscsSourceLoader/dbt_pscs_loader.py PROD',
        env={
            "RUN_ENV": "PROD",
            "PYTHONPATH": pythonpath
        }
    )
