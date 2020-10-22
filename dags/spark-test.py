"""
SparkSubmit prototype
Launch a spark application to a spark cluster 
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["mikaela.pisani@rootstrap.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("spark-test", default_args=default_args,schedule_interval= '@once')

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_job',
    conn_id='spark_connection',
    java_class='org.apache.spark.examples.SparkPi',
    application='local:///opt/spark/examples/src/main/python/pi.py',
    name='airflowspark-test',
    verbose=True,
    conf={
        'spark.kubernetes.container.image': 'mikaelapisani/spark-py:1.0',
        'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark'
        },
    application_args=["1000"], 
    dag=dag,
)

spark_submit_task.set_upstream(t1)
