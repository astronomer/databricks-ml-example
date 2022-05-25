from pendulum import datetime

from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

docs = """
Demonstrates orchestrating ML pipelines executed on Databricks with Airflow
"""


@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    doc_md=docs,
    default_args={
        'existing_cluster_id': "{{ var.value.databricks_cluster_id }}"
    }
)
def databricks_automl_example():

    # Executes Databricks Notebook that performs data ingestion from BigQuery.
    # Connection to Bigquery is predifined in the Cluster settings the notebook is attached to.
    ingest = DatabricksSubmitRunOperator(
        task_id='ingest_notebook_task',
        notebook_task={
            'notebook_path': "/Users/{{ var.value.databricks_user }}/BigQuery_to_Databricks"
        }
    )

    # Executes Databricks notebook that performs feature engineering and stores model in the Databricks Feature Store.
    feauture_engineering = DatabricksSubmitRunOperator(
        task_id='feature_engineering_notebook_task',
        notebook_task={
            'notebook_path': "/Users/{{ var.value.databricks_user }}/feature-eng_census-pred",
        }
    )

    # Executes Databricks notebook that uses AutoML to train a model by pulling in data from the Feature Store and
    # tracking with MLFlow.
    train_automl = DatabricksSubmitRunOperator(
        task_id='train_notebook_task',
        notebook_task={
            'notebook_path': "/Users/{{ var.value.databricks_user }}/census_automl",
            'base_parameters': {
                'target_variable': 'never_married',
                'features_database': 'census_data',
                'features_table': 'census_adult_income_features',
                'data_dir': 'dbfs:/automl/adult'
            }
        },
        do_xcom_push=True
    )

    ingest >> feauture_engineering >> train_automl


dag = databricks_automl_example()