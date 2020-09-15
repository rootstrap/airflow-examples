from airflow.models import DAG
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from datetime import datetime

class XComEnabledAWSAthenaOperator(AWSAthenaOperator):
    def execute(self, context):
        super(XComEnabledAWSAthenaOperator, self).execute(context)
        # just so that this gets `xcom_push`(ed)
        return self.query_execution_id

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

with DAG("query_s3_athena", default_args=default_args, schedule_interval= '@once') as dag:

    run_query = XComEnabledAWSAthenaOperator(
        task_id='run_query',
        database='prototype',
        query='SELECT * FROM patient',
        output_location='s3://rs-champz-test/result-test',
        aws_conn_id='s3_connection'
    )
    
    move_results = S3FileTransformOperator(
        task_id='move_results',
        source_s3_key='s3://mybucket/mypath/{{ task_instance.xcom_pull(task_ids="run_query") }}.csv',
        dest_s3_key='s3://rs-champz-test/parquet-test/{{ task_instance.xcom_pull(task_ids="run_query") }}.parquet',
        transform_script='/opt/airflow/dags/scripts/parquet_test.py',
        source_aws_conn_id='s3_connection',
        dest_aws_conn_id='s3_connection'
        replace=False
    )
    
move_results.set_upstream(run_query)