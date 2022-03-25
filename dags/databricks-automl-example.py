from datetime import datetime
import logging

from airflow.decorators import task, dag
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

import mlflow

docs = """
Demonstrates orchestrating ML pipelines executed on Databricks with Airflow
"""

@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    doc_md=docs
)
def databricks_automl_example():

    ingest_notebook = {
        'notebook_path': '/Users/faisal@astronomer.io/BigQuery_to_Databricks',
    }

    ingest = DatabricksSubmitRunOperator(
        task_id='ingest_notebook_task',
        existing_cluster_id='0224-221140-suj0ngd4',
        notebook_task=ingest_notebook
        )
 
    feauture_engineering_notebook = {
        'notebook_path': '/Users/faisal@astronomer.io/feauture-eng_census-pred',
    }

    feauture_engineering = DatabricksSubmitRunOperator(
        task_id='feature_engineering_notebook_task',
        existing_cluster_id='0224-221140-suj0ngd4',
        notebook_task=feauture_engineering_notebook
        )


    train_notebook = {
        'notebook_path': '/Users/faisal@astronomer.io/databricks_automl/22-03-03-21:24-never_married_census_adult_income_features-2022_03_03-15_24-nb-10e0f57e/trials/22-03-03-21:24-DecisionTree-978c97aa0e15974358a9b1a0f4086e83'
    }

    train = DatabricksSubmitRunOperator(
        task_id='train_notebook_task',
        existing_cluster_id='0224-221140-suj0ngd4',
        notebook_task=train_notebook,
        do_xcom_push=True
        )

    
    @task
    def register_model(ti=None):

        databricks_run_id = ti.xcom_pull(task_ids='train_notebook_task', key='run_id')
        logging.info(databricks_run_id)

        databricks_hook = DatabricksHook()

        response = databricks_hook._do_api_call(("GET", f'api/2.0/jobs/runs/get-output?run_id={databricks_run_id}'), {})
        
        model_uri = response['notebook_output']['result']
        
        mv = mlflow.register_model(model_uri, 'census_pred')

        logging.info(f'Name: {mv.name}')
        logging.info(f'Version: {mv.version}')

        client = mlflow.tracking.MlflowClient()

        client.transition_model_version_stage(
            name=mv.name,
            version=mv.version,
            stage="Staging"
            )

    # ingest >> feauture_engineering >> 
    train >> register_model() 



dag = databricks_automl_example()