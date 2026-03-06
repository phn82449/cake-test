"""DAG for syncing files from source SFTP to target SFTP."""

from datetime import datetime, timedelta

from airflow import DAG

from src.operators.extensible_transfer import ExtensibleTransferOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "sftp_to_sftp_sync",
    default_args=default_args,
    description="Sync files from source SFTP to target SFTP unidirectionally.",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 3, 1),
    catchup=False,
    tags=["sftp", "sync", "data_engineering"],
) as dag:
    sync_task = ExtensibleTransferOperator(
        task_id="sync_sftp_directories",
        source_conn_id="sftp_source_conn",
        target_conn_id="sftp_target_conn",
        source_prefix="/upload",
        target_prefix="/upload",
    )

    sync_task
