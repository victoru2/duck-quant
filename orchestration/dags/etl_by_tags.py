import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig
)
from cosmos.profiles import DuckDBUserPasswordProfileMapping

# ----------------------
# Configuración DAG
# ----------------------
default_args = {
    "owner": "victor",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "pool": "duckdb_pool"
}


def get_dbt_root_path():
    default_dbt_root_path = Path(__file__).parent / "dbt"
    return Path(os.getenv("DBT_ROOT_PATH", default_dbt_root_path))


# ----------------------
# Crear ProfileConfig usando conexión Airflow + Mapping
# ----------------------
def create_profile_config(profile_name: str, target_name: str):
    return ProfileConfig(
        profile_name=profile_name,
        target_name=target_name,
        profile_mapping=DuckDBUserPasswordProfileMapping(
            conn_id="duckdb_conn",
            profile_args={
                "path": "/opt/airflow/dags/data/database.duckdb"
            }
        )
    )


# ----------------------
# Crear Task Group por tag
# ----------------------
def create_dbt_task_group(group_id: str,
                          profile_name: str,
                          target_name: str,
                          project_config: ProjectConfig,
                          tag: str):
    return DbtTaskGroup(
        group_id=group_id,
        project_config=project_config,
        profile_config=create_profile_config(profile_name, target_name),
        execution_config=ExecutionConfig(
            dbt_executable_path="dbt"
        ),
        render_config=RenderConfig(
            select=[f"tag:{tag}"],
        )
    )


# ----------------------
# DAG principal
# ----------------------
@dag(
    dag_id="dbt_airflow_by_tags",
    schedule=timedelta(minutes=5),
    start_date=datetime(2024, 2, 23),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ETL", "DBT", "Cosmos"]
)
def dbt_iceberg_dag():

    start_task = EmptyOperator(task_id="start")

    task_groups = ["expense", "crypto_invest"]
    tasks = {}
    environment = os.getenv("ENVIRONMENT", "dev")

    project_config = ProjectConfig(
        dbt_project_path=get_dbt_root_path(),
    )

    for group in task_groups:
        tasks[group] = create_dbt_task_group(
            group_id=f"{group}_models",
            profile_name="duck_quant",
            target_name=environment,
            project_config=project_config,
            tag=group
        )

    start_task >> list(tasks.values())


dbt_iceberg_dag = dbt_iceberg_dag()
