# Airflow
Dynamic Dependency Generation with Apache Airflow.


When you have hundreds of tasks to be scheduled in Airflow, it becomes very difficult to provide dependency between tasks manually and you need to find out a way to generate it dynamically.

Attached code generates dependencies between tasks available in MySQL metadata. For this following things must be available in metadata database.

- All required tasks with TASK_ID, to uniquely identify each task.
- Dependency column in MySQL table will be having TASK_ID's of those tasks on which respective tasks is dependent.
