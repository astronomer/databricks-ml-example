## Databricks Data Science and Machine Learning Example with Airflow

These DAGs give basic examples on how to use Airflow to orchestarte your ML tasks in Databricks. The Databricks code is in a Databricks notebook for which you can find descriptions of below.

1. **databricks-ml-example.py** - Runs an end to end data ingest to model publishing pipeline with the following tasks:
    - **ingest:** Pulls data from BigQuery and does some basic cleaning and transformations then saves it to Delta Lake.
    - **feature engineering:**  Extract features for model and save output to the Feature Store.
    - **train:** Train model with a Databricks notebook
    - **register:** Register model to mlflow

2. **databricks-automl-example.py** - Runs an experimental pipeline from ingest to model training with Databricks AutoML with the following tasks:
    - **ingest:** Pulls data from BigQuery and does some basic cleaning and transformations then saves it to Delta Lake.
    - **feature engineering:**  Extract features for model and save output to the Feature Store.
    - **train:** Train models using AutoML with a notebook

3. **databricks-ml-retrain-exmaple.py** - Runs a pipeine that retrains, registers, and submits a transition to Stage request for a model, then submits a Slack notification with the following tasks: 
    - **retrain** - Retrain model with a notebook
    - **register** - Register in MLflow
    - **submit transition request** - Submit an approval request in MLflow to transition the model to Stage
    - **notify** - Send a Slack notification with relevant details about the model

## Requirements

### Bigquery
 - [Service account with correct permsissions](https://docs.databricks.com/data/data-sources/google/bigquery.html#step-1-set-up-google-cloud) to use with Databricks cluster

### Databricks
  - [Authentication token](https://docs.databricks.com/dev-tools/api/latest/authentication.html) (if you don't want to use a username and password to authenticate from Airflow)
  - Existing cluster [setup with GCP credentials](https://docs.databricks.com/data/data-sources/google/bigquery.html#create-a-google-service-account-for-databricks) (you can use an on demand cluster but you will need to supply it the GCP credentials accordingly)
  - Notebooks for each task.
     - You can use the notebooks in the `example_notebooks` folder which have been provided in this repo to get started.

### Airflow
 - [Databricks connection](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html)
 - Airflow Variables
    - databricks_user
    - databricks_cluster_id 
    - databricks_instance
 - MLflow environment variables in your .env
    - MLFLOW_TRACKING_URI=databricks
    - DATABRICKS_HOST=your_databricks_host
    - DATABRICKS_TOKEN=your_PAT