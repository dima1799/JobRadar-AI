from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

QDRANT_URL =  Variable.get("QDRANT_URL")
QDRANT_COLLECTION =  Variable.get("QDRANT_COLLECTION")
NO_PROXY =  Variable.get("NO_PROXY")
APP_AIRFLOW_PATH = Variable.get("APP_AIRFLOW_PATH")
SAVE_VACANCIES_AIRFLOW_PATH = Variable.get("SAVE_VACANCIES_AIRFLOW_PATH")
EMBED_MODEL = Variable.get("EMBED_MODEL")
SCHEDULE = "0 6 * * *"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="daily_job_ingest",
    default_args=default_args,
    schedule_interval=SCHEDULE,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["jobs", "qdrant", "rag"],
    description="Ежедневно парсим hh и обновляем коллекцию в Qdrant",
)

with dag:
    # парсинг вакансий с hh.ru
    run_parser = BashOperator(
        task_id="run_parser",
        bash_command=f"python {APP_AIRFLOW_PATH}hh_parser.py",
        env={
            "SAVE_VACANCIES_AIRFLOW_PATH" : SAVE_VACANCIES_AIRFLOW_PATH,
        },
    )

    # загрузка вакансий в qdrant
    upload_qdrant = BashOperator(
        task_id="upload_qdrant",
        bash_command=f"python {APP_AIRFLOW_PATH}upload_to_qdrant.py",
        env={
            "SAVE_VACANCIES_AIRFLOW_PATH" : SAVE_VACANCIES_AIRFLOW_PATH,
            "QDRANT_URL": QDRANT_URL,
            "QDRANT_COLLECTION": QDRANT_COLLECTION,
            "EMBED_MODEL" : EMBED_MODEL,
        },
    )

    run_parser >> upload_qdrant
