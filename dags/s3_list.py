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

def list_files(**kwargs):
    s3 = S3Hook(aws_conn_id='s3_connection')
    s3.get_conn()
    files = s3.list_prefixes(self, bucket_name='s3://rs-champz-test/champz/original_data/')
    print("BUCKET:  {}".format(files))


with DAG("list_files", default_args=default_args, schedule_interval= '@once') as dag:

    t1 = BashOperator(
        task_id='bash_test',
        bash_command='echo "Starting AWSAthenaOperator TEST"'
    )

    check_for_files = BranchPythonOperator(
        task_id='list_files',
        provide_context=True,
        python_callable=GetFiles,
        dag=dag
    )

    t1.set_upstream(check_for_files)