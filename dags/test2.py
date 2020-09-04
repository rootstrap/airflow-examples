from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
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


transformer = S3FileTransformOperator(
    task_id='etl_medical_records',
    description='cleans medical etl_medical_records',
    source_s3_key='s3://rs-champz-test/original_data/*',
    dest_s3_key='s3://rs-champz-test/cleaned_data/',
    replace=False,
    transform_script='scripts/clean_medical_records.py'
)



# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

t2.set_upstream(t1)
t3.set_upstream(t1)