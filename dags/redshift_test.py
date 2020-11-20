
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
#from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.s3_to_redshift import S3ToRedshiftOperator
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


with DAG("redshift_transformer", default_args=default_args, schedule_interval= '@once') as dag:


    start = DummyOperator(task_id='start')

    t1 = BashOperator(
        task_id='bash_test',
        bash_command='echo "Testing File transform" > s3_conn_test.txt'
    )

    s3_to_redshift_transformer = S3ToRedshiftOperator(
        task_id = 's3_to_redshift_transformer',
        schema = 'PUBLIC',
        table = 'patient',
        s3_bucket = 'patients-records',
        s3_key = '',
        redshift_conn_id = 'redshift_connection',
        aws_conn_id = 's3_connection',
        copy_options = ('csv'),
        truncate_table  = False
    )


    t1.set_upstream(start)
    s3_to_redshift_transformer.set_upstream(t1)