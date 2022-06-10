import json
import logging
import os

import requests
from airflow.decorators import task, dag
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.slack.hooks.slack import SlackHook
from pendulum import datetime

docs = """
Demonstrates orchestrating ML retrain pipelines executed on Databricks with Airflow.

Triggers the execution of a designated retrain notebook, then registers the new model and submits a "transition to 
Staging" approval request. Lastly, it sends a Slack notification to a designated channel informing stakeholders on the 
model's validation scores, link to the approval request, and other relevant information.
"""

model_name = 'census_pred'


@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    doc_md=docs
)
def databricks_ml_retrain_example():

    # Executes Databricks notebook that does model training by pulling in data from the Feature Store tracking with
    # MLFlow.
    retrain = DatabricksSubmitRunOperator(
        task_id='retrain_notebook_task',
        existing_cluster_id="{{ var.value.databricks_cluster_id }}",
        notebook_task={
            'notebook_path': "/Users/{{ var.value.databricks_user }}/LightGBM-Census-Classifier"
        },
        do_xcom_push=True
    )
    
    @task()
    def register_model(databricks_run_id: str):
        """Register model in MLflow

        Uses the run_id to get the notebook output which contains the model URI needed to register the model.

        Returns the new model version.

        Keyword arguments:
        databricks_run_id -- run_id of the training notebook used in the "train" task
        """
        import mlflow

        logging.info(f'Training notebook run_id: {databricks_run_id}')

        # Get model URI from notebook output
        databricks_hook = DatabricksHook()
        model_uri = databricks_hook.get_run_output(databricks_run_id)['notebook_output']['result']
        logging.info(f'Model URI: {model_uri}')

        # Register the model get new version number.
        registered_model = mlflow.register_model(model_uri, model_name)

        return registered_model.version

    @task()
    def submit_for_approval_to_stage(model_version: str, databricks_instance: str):
        """Submit transition to Staging request

        After registration a transition to Staging request is submitted which can be reviewed by users.

        Returns response from MLflow API for the request.

        Keyword arguments:
        model_version -- Version of the model that should be submitted with transition request
        databricks_instance -- The Databricks instance id
        """
        response = requests.post(
            url=f'https://{databricks_instance}.cloud.databricks.com/api/2.0/mlflow/transition-requests/create',
            headers={'Authorization': 'Bearer %s' % os.environ['DATABRICKS_TOKEN']},
            json={
                'comment': 'Orchestrated by Airflow',
                'name': model_name,
                'stage': 'Staging',
                'version': model_version
            }
        )
        
        if response.status_code != 200:
            raise Exception(response.text)

        return response.text

    @task()
    def notify(model_version, transition_req: str, databricks_instance: str):
        """Send notification

        Send a Slack notification with new model's metrics and that it's ready for approval to transition to Staging.

        Keyword Arguments:
        model_version -- Version of model that notification is for
        transition_req -- URL for transition request
        databricks_instance -- The Databricks instance id
        """

        # Get all model version that are registered with the specified model name.
        response = requests.get(
            url=f'https://{databricks_instance}.cloud.databricks.com/api/2.0/mlflow/databricks/registered-models/get',
            headers={'Authorization': 'Bearer %s' % os.environ['DATABRICKS_TOKEN']},
            json={
                'name': model_name,
            }
        )

        if response.status_code != 200:
            raise Exception(response.text)

        registered_models = json.loads(response.text)

        model_run_id = ''

        # Get the MLflow run_id for the new model version from the original API request.
        for version in registered_models['registered_model_databricks']['latest_versions']:
            if version['version'] == model_version:
                model_run_id = version['run_id']
                logging.info(f'Model run_id: {model_run_id}')

        # Get the MLflow run information for the new model.
        response = requests.get(
            url=f'https://{databricks_instance}.cloud.databricks.com/api/2.0/mlflow/runs/get',
            headers={'Authorization': 'Bearer %s' % os.environ['DATABRICKS_TOKEN']},
            json={
                'run_id': model_run_id,
            }
        )
        
        if response.status_code != 200:
            raise Exception(response.text)

        # Collect validation scores from the model run information.
        run_data = json.loads(response.text)
        logging.info(run_data)
        metrics = run_data['run']['data']['metrics']

        validation_scores = list()
        for metric in metrics:
            if 'val_' in metric['key']:
                key = metric['key']
                value = metric['value']
                validation_scores.append({key: value})

        # Create Slack message.
        message = f"""
*Model Transition Request:* ```{transition_req}```
*Awaiting approval:* https://{databricks_instance}.cloud.databricks.com/#mlflow/models/census_pred/versions/{model_version}
*Name:* `{model_name}`
*run_id:* `{model_run_id}`
Validation Scores: ```{validation_scores}```
        """

        # Send Slack message to specific channel.
        slack = SlackHook(slack_conn_id='slack_default')
        slack.call(
            api_method='chat.postMessage',
            json={
                'text': message,
                'channel': "#integrations"
            }
        )

    model_ver = register_model(retrain.output['run_id'])
    transition_request = submit_for_approval_to_stage(model_ver, "{{ var.value.databricks_instance}}")
    notify(model_ver, transition_request, "{{ var.value.databricks_instance}}")
    

dag = databricks_ml_retrain_example()
