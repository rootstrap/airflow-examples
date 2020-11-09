 
"""
LivyOperator prototype
Launch a spark application to a spark cluster with Apache Livy
Example taken from https://github.com/apache/airflow/blob/master/airflow/providers/apache/livy/example_dags/example_livy.py 
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor 

from datetime import datetime, timedelta
import json 
from enum import Enum

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

dag = DAG("livy-test2", default_args=default_args, schedule_interval= '@once')

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
    success
    dead
"""

class State(Enum):
    waiting = 'waiting'
    running = 'running'
    available = 'available'
    error = 'available'
    cancelling = 'cancelling'
    cancelled = 'cancelled'
    dead = 'dead'
    success = 'success'

def check_state(response):
    value = response.json()['state']
    print('state=:' + value)
    state = str(State(value))
    return (state == 'success' or state=='error' or state=='cancelling' or state =='cancelled')

def get_id(**context):
    response = context['ti'].xcom_pull(task_ids='spark-test-livy')
    print('Respose:', response)
    response = json.loads(response)
    return response['id']

spark_task  = SimpleHttpOperator(
    task_id='spark-test-livy',
    method='POST',
    endpoint='/batches',
    data=json.dumps({
        "name": "SparkPi-06", #TODO: generate id 
        "className": "org.apache.spark.examples.SparkPi",
        "numExecutors": 2,
        "file": "local:///opt/spark/examples/src/main/python/pi.py",
        "args": ["10"],
        "conf": {
            "spark.kubernetes.container.image": "mikaelapisani/spark-py:1.0",
            "spark.kubernetes.driver.pod.name" : "spark-pi-driver",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark"
        }
      }),
    headers={'Content-Type': 'application/json'},
    xcom_push=True,
    http_conn_id='livy_conn_id',
    dag=dag
)



get_id  = PythonOperator(
    task_id='get_id',
    python_callable=get_id,
    provide_context=True,
    xcom_push=True,
    dag=dag
    )

spark_sensor = HttpSensor(
    task_id='spark-sensor-livy',
    method='GET',
    endpoint="/batches/{{ti.xcom_pull(task_ids='get_id')}}/state",
    http_conn_id='livy_conn_id',
    dag=dag,
    response_check=check_state
    )

spark_task.set_upstream(t1)
get_id.set_upstream(spark_task)
spark_sensor.set_upstream(get_id)
