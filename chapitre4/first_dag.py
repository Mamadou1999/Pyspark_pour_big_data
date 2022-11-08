#Import required librairies
import datetime
from datetime import timedelta
import airflow 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


#Defining default arguments
#  
args = {
    'owner': 'Momo',
    'start_date': airflow.utils.dates.days_ago(2),
    'end_date': datetime.datetime(2022, 11, 6),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Instantiating the DAG object

dag = DAG(
    'pramod_airflow_dag',
    default_args=args,
    description='A simple DAG',
    # Continue to run DAG once per day
    schedule_interval=timedelta(days=1)
)

#Declaring Tasks

#First task
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
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

t1 >> t2

