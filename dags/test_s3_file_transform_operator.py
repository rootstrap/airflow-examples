from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime, timedelta
from airflow.models import Variable


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 7),
    "email": ["mikaela.pisani@rootstrap.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

source_s3_path = Variable.get("source_s3_path")
dest_s3_path = Variable.get("dest_s3_path")


with DAG("s3_transformer", default_args=default_args, schedule_interval= '@once') as dag:


    start = DummyOperator(task_id='start')


    transformer = S3FileTransformOperator(
        task_id='transform_s3_data',
        description='transform_s3_data',
        source_s3_key=source_s3_path,
        dest_s3_key=dest_s3_path,
        replace=False,
        transform_script='/opt/airflow/dags/scripts/transform.py',
        source_aws_conn_id='s3_connection',
        dest_aws_conn_id='s3_connection'
    )

    transformer.set_upstream(start)

