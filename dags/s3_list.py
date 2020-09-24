from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

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
    files = s3.list_keys(bucket_name='rs-champz-test', prefix='champz/original_data/', delimiter='/')
    if files is None: 
        return False
    else:
        print('Files: ' , files)
        print(type(files))
        for file in files:
            print(file)
        return len(files)


dag = DAG("query-s3", default_args=default_args, schedule_interval= '@once')

start_op = BashOperator(
    task_id='bash_test',
    bash_command='echo "Starting TEST"',
    dag=dag )

task = PythonOperator(
    task_id='list_files',
    provide_context=True,
    python_callable=list_files,
    dag=dag
)

start_op >> task
