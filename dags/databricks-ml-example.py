from datetime import datetime
import logging

from airflow.decorators import task, dag
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
# from astronomer.providers.databricks.operators.databricks import DatabricksSubmitRunOperatorAsync

from include.databricks_helper import get_notebook_output

import mlflow

docs = """
Demonstrates orchestrating ML pipelines executed on Databricks with Airflow
"""

DATABRICKS_USER='<your username>'
DATABRICKS_NOTEBOOK_PATH=f'/Users/{DATABRICKS_USER}'
DATABRICKS_CLUSTER_ID='<databricks cluster id to attach notebooks to>'

@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    doc_md=docs
)
def databricks_ml_example():

    ingest_notebook = {
        'notebook_path': f'{DATABRICKS_NOTEBOOK_PATH}/BigQuery-to-Databricks',
    }

    ingest = DatabricksSubmitRunOperator(
        task_id='ingest_notebook_task',
        existing_cluster_id=DATABRICKS_CLUSTER_ID,
        notebook_task=ingest_notebook
        )
 
    feauture_engineering_notebook = {
        'notebook_path': '{DATABRICKS_NOTEBOOK_PATH}/feauture-eng_census-pred',
    }

    feauture_engineering = DatabricksSubmitRunOperator(
        task_id='feature_engineering_notebook_task',
        existing_cluster_id=DATABRICKS_CLUSTER_ID,
        notebook_task=feauture_engineering_notebook
        )


    train_notebook = {
        'notebook_path': '{DATABRICKS_NOTEBOOK_PATH}/LightGBM-Census-Classifier'
    }

    train = DatabricksSubmitRunOperator(
        task_id='train_notebook_task',
        existing_cluster_id=DATABRICKS_CLUSTER_ID,
        notebook_task=train_notebook,
        do_xcom_push=True
        )

    
    @task
    def register_model(ti=None):

        databricks_run_id = ti.xcom_pull(task_ids='train_notebook_task', key='run_id')
        logging.info(f'Training notebook run_id: {databricks_run_id}')

        model_uri = get_notebook_output(databricks_run_id)
        
        model_version = mlflow.register_model(model_uri, 'census_pred')

        logging.info(f'Name: {model_version.name}')
        logging.info(f'Version: {model_version.version}')

        client = mlflow.tracking.MlflowClient()

        client.transition_model_version_stage(
            name=model_version.name,
            version=model_version.version,
            stage="Staging"
            )

    ingest >> feauture_engineering >> train >> register_model() 



dag = databricks_ml_example()