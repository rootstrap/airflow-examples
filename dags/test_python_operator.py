"""
Simple DAG example for using Python Operator
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# (1) Define the DAG

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 10, 24),
    "email": ["something@here.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("python-test", default_args=default_args, schedule_interval="@once")

# (2) Define the Python functions to be executed
# Note: Objects returned will show up in the logs


def mock_etl(job_success_percentage):
    """
    Mocks a function that performs data ETL, and returns job status
    """
    if job_success_percentage >= 0.5:
        return "mock_etl: Job completed successfully with status code 0"
    else:
        return "mock_etl: Job failed to complete with status code 1"


def mock_predict(job_success_percentage=0.5):
    """
    Mocks a function that produces predictions, and returns job status
    """
    if job_success_percentage >= 0.5:
        return "mock_predict: Job completed successfully with status code 0"
    else:
        return "mock_predict: Job completed successfully with status code 1"


# (3) Define the Python Operators
"""
Required keywords:
+ task_id: Name of the task defined
+ python_callable: Python function to call
+ dag: dag object to attach task to

Optional keywords:
+ op_args: list, arguments to pass to the Python function
+ op_kwargs: dict, keyword arguments to pass to the Python function
"""

t1 = PythonOperator(
    task_id="mock_etl", python_callable=mock_etl, dag=dag, op_args=[0.6]
)
t2 = PythonOperator(
    task_id="mock_predict",
    python_callable=mock_predict,
    dag=dag,
    op_kwargs={"job_success_percentage": 0.4},
)

# (4) Configure sequence of tasks in the DAG

t2.set_upstream(t1)
