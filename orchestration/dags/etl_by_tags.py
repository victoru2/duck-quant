import os
from resources import RESOURCE_LIST
from pathlib import Path
from datetime import datetime, timedelta

from airflow.decorators import dag
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig
)
from cosmos.profiles import DuckDBUserPasswordProfileMapping

default_args = {
    "owner": "victor",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "pool": "duckdb_pool"
}


def get_dbt_root_path():
    default_path = Path(__file__).parent / "dbt"
    return Path(os.getenv("DBT_ROOT_PATH", default_path))


def create_profile_config():
    return ProfileConfig(
        profile_name="duck_quant",
        target_name=os.getenv("ENVIRONMENT", "dev"),
        profile_mapping=DuckDBUserPasswordProfileMapping(
            conn_id="duckdb_conn",
            profile_args={"path": "/opt/airflow/dags/data/database.duckdb"}
        )
    )


def build_dbt_dag(tag: str):
    @dag(
        dag_id=f"dbt_by_tag__{tag}",
        schedule=None,
        start_date=datetime(2024, 2, 23),
        catchup=False,
        default_args=default_args,
        tags=["ETL", "dbt", tag]
    )
    def _dag():
        DbtTaskGroup(
            group_id=f"{tag}_models",
            project_config=ProjectConfig(dbt_project_path=get_dbt_root_path()),
            profile_config=create_profile_config(),
            execution_config=ExecutionConfig(dbt_executable_path="dbt"),
            render_config=RenderConfig(select=[f"tag:{tag}"])
        )
    return _dag()


# Dynamically generate DAGs for each Tag
for _tag in RESOURCE_LIST:
    globals()[f"dbt_by_tag__{_tag}"] = build_dbt_dag(_tag)
