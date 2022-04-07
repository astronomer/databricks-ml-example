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

    train_automl_notebook = {
        'notebook_path': "/Users/{{ var.value.databricks_user }}/census_automl",
        'base_parameters': {
            'target_variable': 'never_married',
            'features_database': 'census_data',
            'features_table': 'census_adult_income_features',
            'data_dir': 'dbfs:/automl/adult'

        }         
    }

    train_automl = DatabricksSubmitRunOperator(
        task_id='train_notebook_task',
        existing_cluster_id="{{ var.value.databricks_cluster_id }}",
        notebook_task=train_automl_notebook,
        do_xcom_push=True
    )

    ingest >> feauture_engineering >> train_automl


dag = databricks_automl_example()