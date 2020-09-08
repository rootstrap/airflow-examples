from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
     'description': 'Test Pipeline',
     'depend_on_past': False,
    'start_date':  datetime(2016, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'kubernetes_sample', default_args=default_args, schedule_interval= '@once')


start = DummyOperator(task_id='run_this_first', dag=dag)


passing = KubernetesPodOperator(namespace='kubernetes_sample', image="python:3.6", cmds=["python","-c"], 
  arguments=["print('hello world')"], name="passing-test", task_id="passing-task", get_logs=True, dag=dag )



start >> passing