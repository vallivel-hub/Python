# dbt_projects/

This folder holds all DBT projects used in Airflow DAGs.  This is place holder and any code here should NOT be pushed back to git, use the original working directory!

Each project (e.g., `UMS_STAGING/`, `finance_dw/`) should be added as a subfolder here. 
These projects can either be cloned manually or synced via automation/scripts.

For example:

- `dbt_projects/UMS_STAGING/`
- `dbt_projects/finance_dw/`

This folder is mounted into the Airflow container at `/opt/airflow/UMS_STAGING`.