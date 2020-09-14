from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator

from datetime import datetime, timedelta


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

with DAG("query_s3", default_args=default_args, schedule_interval= '@once') as dag:

    t1 = BashOperator(
        task_id='bash_test',
        bash_command='echo "Starting AWSAthenaOperator TEST"'
    )

    run_query = AWSAthenaOperator(
        task_id='run_query',
        database='s3://medical-records/csv',
        query='SHOW PARTITIONS UNNEST(SEQUENCE(0, 100))',
        output_location='s3://rs-champz-test/result-test',
        aws_conn_id='s3_connection'
    )

    #run_query = AWSAthenaOperator(
    #    task_id='run_query',
    #    database='s3://medical-records/csv',
    #    query='SELECT text FROM UNNEST(SEQUENCE(0, 100))',
    #    output_location='s3://rs-champz-test/result-test',
    #    aws_conn_id='s3_connection'
    #)


    t1.set_upstream(run_query)

