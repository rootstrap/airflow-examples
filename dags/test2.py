from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator

from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["mikaela.pisani@rootstrap.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG("s3_example", default_args=default_args, schedule_interval=timedelta(1))


t1 = BashOperator(
    task_id='bash_test',
    bash_command='echo "hello, it should work" > s3_conn_test.txt',
    dag=dag)

transformer = S3FileTransformOperator(
    task_id='etl_medical_records',
    description='cleans medical etl_medical_records',
    source_s3_key='s3://rs-champz-test/original_data/*',
    dest_s3_key='s3://rs-champz-test/cleaned_data/',
    replace=False,
    transform_script='scripts/clean_medical_records.py',
    source_aws_conn_id='s3_connection',
    dest_aws_conn_id='s3_connection',
    dag=dag
)

t1.set_upstream(transformer)

