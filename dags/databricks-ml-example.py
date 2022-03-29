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
def databricks_ml_example():

    ingest_notebook = {
        'notebook_path': "/Users/{{ var.value.databricks_user }}/BigQuery_to_Databricks",
    }

    ingest = DatabricksSubmitRunOperator(
        task_id='ingest_notebook_task',
        existing_cluster_id="{{ var.value.databricks_cluster_id }}",
        notebook_task=ingest_notebook
    )
 
    feature_engineering_notebook = {
        'notebook_path': "/Users/{{ var.value.databricks_user }}/feature-eng_census-pred",
    }

    feauture_engineering = DatabricksSubmitRunOperator(
        task_id='feature_engineering_notebook_task',
        existing_cluster_id="{{ var.value.databricks_cluster_id }}",
        notebook_task=feature_engineering_notebook
    )

    train_notebook = {
        'notebook_path': "/Users/{{ var.value.databricks_user }}/LightGBM-Census-Classifier"
    }

    train = DatabricksSubmitRunOperator(
        task_id='train_notebook_task',
        existing_cluster_id="{{ var.value.databricks_cluster_id }}",
        notebook_task=train_notebook,
        do_xcom_push=True
    )

    
    @task
    def register_model(databricks_run_id: str):

        logging.info(f'Training notebook run_id: {databricks_run_id}')

        databricks_hook = DatabricksHook()
        model_uri = databricks_hook.get_run_output(databricks_run_id)['notebook_output']['result']
        logging.info(f'Model URI: {model_uri}')
        
        model_version = mlflow.register_model(model_uri, 'census_pred')

        logging.info(f'Name: {model_version.name}')
        logging.info(f'Version: {model_version.version}')

        client = mlflow.tracking.MlflowClient()

        client.transition_model_version_stage(
            name=model_version.name,
            version=model_version.version,
            stage="Staging"
        )


    ingest >> feauture_engineering >> train
    register_model(train.output['run_id'])


dag = databricks_ml_example()