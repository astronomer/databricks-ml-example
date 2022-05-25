# Databricks Data Science and Machine Learning Examples with Airflow

## DAGs
These DAGs give basic examples on how to use Airflow to orchestrate your ML tasks in Databricks. The Databricks code is in a Databricks notebook for which you can find descriptions of below.

1. **databricks-ml-example.py** - Runs an end to end data ingest to model publishing pipeline with the following tasks:
    - **ingest:** Pulls data from BigQuery and does some basic cleaning and transformations then saves it to Delta Lake.
    - **feature engineering:**  Extract features for model and save output to the Feature Store.
    - **train:** Train model with a Databricks notebook
    - **register:** Register model to mlflow

2. **databricks-automl-example.py** - Runs an experimental pipeline from ingest to model training with Databricks AutoML with the following tasks:
    - **ingest:** Pulls data from BigQuery and does some basic cleaning and transformations then saves it to Delta Lake.
    - **feature engineering:**  Extract features for model and save output to the Feature Store.
    - **train:** Train models using AutoML with a notebook

3. **databricks-ml-retrain-example.py** - Runs a pipeline that retrains, registers, and submits a transition to Stage request for a model, then submits a Slack notification with the following tasks: 
    - **retrain:** Retrain model with a notebook
    - **register:** Register in MLflow
    - **submit transition request:** Submit an approval request in MLflow to transition the model to Stage
    - **notify:** Send a Slack notification with relevant details about the model

    Note: For this DAG we used the Databricks REST API in many places for requests to MLFlow due to there not being a Python API available for those endpionts yet.

4. **databricks-model-serve-sagemaker-example.py** - Deploys MLflow model to Sagemaker
   - **check model info for Staging:** Checks if there is a model marked for Staging and gets it information.
   - **new model version confirmation:** Shortcircuit Operator that determines if the model has been deployed already and whether to proceed ot not.
   - **deploy model:** Use mlflow.sagemaker API to deploy model and endpoint in AWS Sagemaker
   - **test model endpoint:** Use Sagemaker API to send a request with sample data to get predictions
   - **mark as deployed:** Tag model version in MLflow Registry as deployed
   
   Note: For this DAG we place AWS credentials as environment variables and not as an Airflow connections. This is to
   simply avoid putting them in two places, since the API calls to Sagemaker or MLflow that don't use an Airflow operator cannot access those credentials from connections.

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
 - AWS environment variables in your .env
   - AWS_ACCESS_KEY_ID
   - AWS_SECRET_ACCESS_KEY
   - AWS_SESSION_TOKEN