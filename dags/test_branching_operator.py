'''
Copyright [2020] [Matthew Mutiso]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

import random

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='sample_branch_operator',
    default_args=args,
    catchup=False,
    schedule_interval=None # This means we trigger the DAG manually from airflow web ui
)

start_here_task = BashOperator(task_id='start_task', bash_command='echo starting at $(date) ', dag=dag)

def is_random_integer_odd_or_even():
    choices = [i for i in range(1,11)] # generate a list of integers from  1 to 10
    choice = random.choice(choices)

    if choice % 2 == 0:
        return 'even_task'
    else:
        return 'odd_task'

branching_task = BranchPythonOperator(
    task_id='branching',
    python_callable=is_random_integer_odd_or_even,
    trigger_rule="all_done",
    dag=dag,
)

even_task = BashOperator(task_id='even_task', bash_command='echo friend, we are now even at $(date) ', dag=dag)

odd_task = BashOperator(task_id='odd_task', bash_command='echo wow, isnt this a bit odd at $(date) ', dag=dag)

end_here_task = BashOperator(task_id='end_task', bash_command='echo finishing at $(date) ', dag=dag)

start_here_task >> branching_task

branching_task >> even_task >> end_here_task

branching_task >> odd_task >> end_here_task