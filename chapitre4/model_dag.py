#Import required librairies
import datetime
from datetime import timedelta
import airflow 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


#Defining default arguments
#  
args = {
    'owner': 'Momo',
    'start_date': airflow.utils.dates.days_ago(2),
    'end_date': datetime.datetime(2022, 11, 6),
    'depends_on_past': False,
    'email': ['momo@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Instantiating the DAG object

dag = DAG(
    'test_AI_model_dag',
    default_args=args,
    description='Test an AI model with Airflow',
    # Continue to run DAG once per day
    schedule_interval=timedelta(days=1)
)

#Declaring Tasks

#First task

# python callable function
def print_hello():
		return "Hello World! Let's test our trained AI model"

# Creating first task
hello_world_task = PythonOperator(task_id='hello_world_task', python_callable=print_hello, dag=dag)

#Running pytho script
t1 = BashOperator(
    task_id='print_date',
    bash_command='python test_onnx_mzr.py',
    dag=dag,
)

#Second task 

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag,
)

#Task order 

hello_world_task >> t1 >> t2

