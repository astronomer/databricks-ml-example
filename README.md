## Databricks Data Science and Machine Learning Example with Airflow

These DAGs give basic examples on how to use Airflow to orchestarte your ML tasks in Databricks. The Databricks code is in a Databricks notebook for which you can find descriptions of below.

1. **databricks-automl-example.py** - Runs an end to end data ingest to model publishing pipeline.
    - **ingest:** Pulls data from BigQuery and does some basic cleaning and transformations then saves it to Delta Lake.
    - **feature engineering:**  Extract features for model and save output to the Feature Store.
    - **train:** Train model
    - **register:** register model to mlflow

 ## Requirements

 ### Bigquery
 - [Service account with correct permsissions](https://docs.databricks.com/data/data-sources/google/bigquery.html#step-1-set-up-google-cloud) to use with Databricks cluster

 ### Databricks
 - [Authentication token](https://docs.databricks.com/dev-tools/api/latest/authentication.html) (if you don't want to use a username and password to authenticate from Airflow)
 - Existing cluster [setup with GCP credentials](https://docs.databricks.com/dev-tools/api/latest/authentication.html) (you can use an on demand cluster but you will need to supply it the GCP credentials accordingly)

 ### Airflow
 - [Databricks connection](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html)
 - MLflow environment variables in your .env
     - MLFLOW_TRACKING_URI=databricks
     - DATABRICKS_HOST=your_databricks_host
     - DATABRICKS_TOKEN=your_PAT