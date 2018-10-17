#Import inbuilt module
import os

#Import MySQL module for mysql connectivity
import MySQLdb
import pandas as pd

#Import Airflow module and Operator
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta,datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}



dag = DAG(
    dag_id='Dynamic_DAG_Example',
    default_args=default_args,
    description='A Dynamic DAG from Metadata Database',
    schedule_interval=timedelta(days=1))

db = MySQLdb.connect(host="localhost", user="airflow", passwd="airflow", db="dag_metadata")

df = pd.read_sql("select dag_id,task_id,task_name,script_path,script_name,task_type,dependents from dag_metadata.tasks where dag_id='dynamic_dag_from_metadata_example' order by dag_id,task_id", con=db)

#first_task_id = 1      # To point to First Task in DAG
#last_task_id = df['task_id'].max()    # To point to last task in DAG
    
for idx,row in df.iterrows():
    dag_id = row['dag_id']
    task_id = row['task_id']
    task_name = row['task_name']
    script_path = row['script_path']
    script_name = row['script_name']
    task_type = row['task_type']
    dependents = row['dependents'].split("|")  #Holds TASK_ID's separated by pipe

    
    t1 = BashOperator(
            task_id=task_name,
            bash_command=script_path+script_name+' ',
            dag=dag)
    
#    if task_id == first_task_id:
#        start = DummyOperator(
#                task_id = 'START_of_ETL',
#                dag = dag)  
#        t1.set_upstream(start)
    
#    if task_id == last_task_id:
#        end = DummyOperator(
#                task_id = 'END_of_ETL',
#                dag = dag)   
#        end.set_upstream(t1)
    
	#Based on Dependent TASK_ID upstream tasks will be tagged here 
    for d in dependents:
        if  d == '':
            continue
        for idx,row in df[df['task_id'] == int(d)].iterrows():
            t2 = BashOperator(
                    task_id=row['task_name'],
                    bash_command=row['script_path']+row['task_name']+' ',
                    dag=dag)
            t1.set_upstream(t2)
