from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_databricks_connection3',
    default_args=default_args,
    description='A simple test DAG for Databricks connection',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 1),
    catchup=False,
)

notebook_task = DatabricksSubmitRunOperator(
    task_id='run_databricks_notebook',
    databricks_conn_id='databricks_default',
    existing_cluster_id='0708-172335-rypwxm52',
    notebook_task={
        'notebook_path': '/Workspace/Users/ayushi_7ebfd7d3-20f9-44d2-8b54-949f14eb2471@mentorskoolgcp.nuvelabs.com/airflowcheck',
    },
    dag=dag,
)

notebook_task
