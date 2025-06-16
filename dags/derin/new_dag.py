from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, LoadMode
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Using Airflow connection with username/password authentication
profile_config = ProfileConfig(
    profile_name="snowflake_demo",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "schema": "ANALYTICS",
            "threads": 4,
        },
    )
)

# Configure to use manifest
render_config = RenderConfig(
    load_method=LoadMode.DBT_MANIFEST,
    select=["tag:example_models"]  # Select only models with example_models tag
)

# Configure to use the virtual environment
execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)

# Project config with manifest path
project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/dbt/snowflake_demo",
    manifest_path="/usr/local/airflow/dbt/snowflake_demo/target/manifest.json",
    install_dbt_deps=False,
)

something_else = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    dag_id="something_else_dag",
)
