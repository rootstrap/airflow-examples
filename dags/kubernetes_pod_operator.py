from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator


today = datetime.today()
default_args = {
     'description': 'Champz Pipeline',
     'depend_on_past': False,
    'start_date': datetime(today.year, today.month, today.day),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'kubernetes_sample', default_args=default_args, schedule_interval="0 12 * * *")


start = DummyOperator(task_id='run_this_first', dag=dag)

passing = KubernetesPodOperator(namespace='airflow',
                          image="python:3",
                          name="kubernetes_pod_operator_example",
                          task_id="kubernetes_pod_operator_example",
                          cmds=["python", "-c", "print 'Hello'"],
                          get_logs=True,
                          in_cluster=True,
                          log_events_on_failure=True,
                          is_delete_operator_pod=False,
                          dag=dag
                          )

start >> passing