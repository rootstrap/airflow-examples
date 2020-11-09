 
"""
LivyOperator prototype
Launch a spark application to a spark cluster with Apache Livy
Example taken from https://github.com/apache/airflow/blob/master/airflow/providers/apache/livy/example_dags/example_livy.py 
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operator.python import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor 

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

dag = DAG("livy-test2", default_args=default_args,schedule_interval= '@once')

t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

"""
Statement State
Value   Description
waiting Statement is enqueued but execution hasn't started
running Statement is currently running
available   Statement has a response ready
error   Statement failed
cancelling  Statement is being cancelling
cancelled   Statement is cancelled
"""
def check_state(response):
    value = response.json()['state']
    print('state=:' + value)
    return (value == 'available' or value == 'error' or value == 'cancelled')



spark_task  = SimpleHttpOperator(
    task_id='spark-test-livy',
    method='POST',
    endpoint='/batches',
    data={
        "name": "SparkPi-01",
        "className": "org.apache.spark.examples.SparkPi",
        "numExecutors": 2,
        "file": "local:///opt/spark/examples/src/main/python/pi.py",
        "args": ["10"],
        "conf": {
            "spark.kubernetes.container.image": "mikaelapisani/spark-py:1.0",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark"
        }
      },
    headers={'Content-Type': 'application/json'},
    xcom_push=True,
    response_check=check_state,
    http_conn_id='livy_conn_id',
    dag=dag
)

def print_response(response):
    print("######################################")
    print(response)
    print("######################################")


print_response = task_archive_s3_file = PythonOperator(
    task_id='archive_s3_file',
    dag=dag,
    python_callable=obj.print_response,
    provide_context=True,
    dag)

"""spark_sensor = HttpSensor(
    task_id='spark-sensor-livy',
    method='GET',
    endpoint='/batches',
    http_conn_id='livy_conn_id',
    )"""

spark_task.set_upstream(t1)
print_response.set_upstream(spark_task)
