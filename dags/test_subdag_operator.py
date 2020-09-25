from airflow import DAG

from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta


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

def hello(file):
    print('Hello!!!! ', file)


def loop_files(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@once",
    )
    
    tasks = []
    for i in range (5):
        tasks = tasks + [PythonOperator(
            task_id='hello_world' + str(i),
            op_kwargs={'file': str(i)},
            python_callable=hello,
            dag=dag_subdag)]


    return dag_subdag
            


dag = DAG("subdagtest2", default_args=default_args, schedule_interval= '@once')

start_op = BashOperator(
    task_id='bash_test',
    bash_command='echo "Starting TEST"',
    dag=dag )

loop_files = SubDagOperator(
    task_id='loop_files',
    subdag=loop_files('subdagtest2', 'loop_files', default_args),dag=dag
)


start_op >> loop_files