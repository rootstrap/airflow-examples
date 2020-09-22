from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator

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
    return len(files)!=0


with DAG("list_files", default_args=default_args, schedule_interval= '@once') as dag:

start_op = BashOperator(
    task_id='bash_test',
    bash_command='echo "Starting AWSAthenaOperator TEST"'
)

branch_op = BranchPythonOperator(
    task_id='list_files',
    provide_context=True,
    python_callable=getfiles,
    dag=dag
)

continue_op = DummyOperator(task_id='continue_task', dag=dag)

stop_op = DummyOperator(task_id='stop_task', dag=dag)

start_op >> branch_op >> [continue_op, stop_op]
