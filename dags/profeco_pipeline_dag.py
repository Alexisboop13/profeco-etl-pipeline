"""
DAG del pipeline PROFECO: Extract -> Transform -> Load
"""

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/src")


def run_extract():
    from extract import extract
    extract()


def run_transform():
    from extract import extract
    from transform import transform, save_processed

    raw_df = extract()
    clean_df = transform(raw_df)
    save_processed(clean_df)


def run_load():
    from extract import extract
    from transform import transform
    from load import build_star_schema, save_and_upload

    raw_df = extract()
    clean_df = transform(raw_df)
    tables = build_star_schema(clean_df)
    save_and_upload(tables)


with DAG(
    dag_id="profeco_etl_pipeline",
    description="Pipeline ETL de quejas PROFECO: Extract -> Transform -> Load a S3/Athena",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["profeco", "etl", "portfolio"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )

    extract_task >> transform_task >> load_task
