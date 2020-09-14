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
        database='s3://medical-records/',
        query='DESCRIBE TABLE csv',
        output_location='s3://rs-champz-test/result-test',
        aws_conn_id='s3_connection'
    )

    run_query2 = AWSAthenaOperator(
        task_id='run_query2',
        database='s3://medical-records/',
        query='SHOW PARTITIONS FRPM csv',
        output_location='s3://rs-champz-test/result-test',
        aws_conn_id='s3_connection'
    )

    run_query3 = AWSAthenaOperator(
        task_id='run_query3',
        database='s3://medical-records/',
        query='SELECT text FROM csv',
        output_location='s3://rs-champz-test/result-test',
        aws_conn_id='s3_connection'
    )

    run_query4 = AWSAthenaOperator(
        task_id='run_query4',
        database='s3://rs-champz-test/',
        query='SELECT text FROM csv',
        output_location='s3://rs-champz-test/',
        aws_conn_id='s3_connection'
    )


    t1.set_upstream(run_query)
    t1.set_upstream(run_query2)
    t1.set_upstream(run_query3)
    t1.set_upstream(run_query4)

