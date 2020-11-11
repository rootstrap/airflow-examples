 
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
import uuid


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

"""
Statement State
Value   Description
waiting Statement is enqueued but execution hasn't started
running Statement is currently running
available   Statement has a response ready
error   Statement failed
cancelling  Statement is being cancelling
cancelled   Statement is cancelled

Session State
    Value  Description
    not_started Session has not been started
    starting    Session is starting
    idle    Session is waiting for input
    busy    Session is executing a statement
    shutting_down   Session is shutting down
    error   Session errored out
    dead    Session has exited
    killed  Session has been killed
    success Session is successfully stopped
"""

class SessionState(Enum):
    waiting = 'waiting'
    running = 'running'
    available = 'available'
    cancelling = 'cancelling'
    cancelled = 'cancelled'
    not_started = 'not_started'
    starting = 'starting'
    idle = 'idle'
    busy = 'busy'
    shutting_down = 'shutting_down'
    error = 'error'
    dead = 'dead'
    killed = 'killed'
    success = 'success'

def check_state(response):
    value = response.json()['state']
    print('state=:' + value)
    session_state = SessionState(value)
    return session_state in [SessionState.success, 
                            SessionState.killed, 
                            SessionState.dead , 
                            SessionState.error, 
                            SessionState.cancelled]

def get_id(**context):
    response = context['ti'].xcom_pull(task_ids='spark-task')
    print('Respose:', response)
    response = json.loads(response)
    return response['id']


dag = DAG("livy-test2", default_args=default_args, schedule_interval= '@once')

generate_uuid = PythonOperator(
        task_id='generate_uuid',
        python_callable=lambda: str(uuid.uuid4()),
        xcom_push=True,
        dag = dag
    )

print_id = BashOperator(task_id="print_id", bash_command='echo {{ti.xcom_pull(task_ids="generate_uuid")}}', dag=dag)


spark_task  = SimpleHttpOperator(
    task_id='spark-task',
    method='POST',
    endpoint='/batches',
    data=json.dumps({
        'name': "spark-task-{{ti.xcom_pull(task_ids='generate_uuid')}}", 
        'className': 'org.apache.spark.examples.SparkPi',
        'numExecutors': 2,
        'file': 'local:///opt/spark/examples/src/main/python/pi.py',
        'args': ['10'],
        'conf': {
            'spark.kubernetes.container.image': 'mikaelapisani/spark-py:1.0',
            'spark.kubernetes.driver.pod.name' : "driver-{{ti.xcom_pull(task_ids='generate_uuid')}}",
            'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark'
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
    endpoint='/batches/{{ti.xcom_pull(task_ids="get_id")}}/state',
    http_conn_id='livy_conn_id',
    dag=dag,
    response_check=check_state
    )
print_id.set_upstream(generate_uuid)
spark_task.set_upstream(print_id)
get_id.set_upstream(spark_task)
spark_sensor.set_upstream(get_id)
