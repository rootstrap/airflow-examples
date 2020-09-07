from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator

from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 07, 09),
    "email": ["mikaela.pisani@rootstrap.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("transformer", default_args=default_args, schedule_interval=timedelta(1)) as dag:

    t1 = BashOperator(
        task_id='bash_test',
        bash_command='echo "hello, it should work" > s3_conn_test.txt'
    )


    transformer = S3FileTransformOperator(
        task_id='ETL_medical_records',
        description='cleans ETL_medical_records',
        source_s3_key='s3://rs-champz-test/champz/original_data/100.xml',
        dest_s3_key='s3://rs-champz-test/champz/cleaned_data/100.xml',
        replace=False,
        transform_script='../scripts/clean_medical_records.py',
        source_aws_conn_id='s3_connection',
        dest_aws_conn_id='s3_connection'
    )

    t1.set_upstream(transformer)

