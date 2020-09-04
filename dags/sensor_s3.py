"""
S3 Sensor Connection Test
"""

from airflow import DAG
from airflow.operators import SimpleHttpOperator, HttpSensor,   BashOperator, EmailOperator, S3KeySensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 11, 1),
    'email': ['something@here.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('s3_dag_test', default_args=default_args, schedule_interval= '@once')

t1 = BashOperator(
    task_id='bash_test',
    bash_command='echo "hello, it should work" > s3_conn_test.txt',
    dag=dag)

sensor = S3KeySensor(
    task_id='check_s3_for_file_in_s3',
    bucket_key='champz/original_data/100.xml',
    wildcard_match=True,
    bucket_name='rs-champz-test',
    s3_conn_id='s3_connection',
    timeout=18*60*60,
    poke_interval=120,
    dag=dag)

t1.set_upstream(sensor)