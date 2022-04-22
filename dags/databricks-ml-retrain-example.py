from ensurepip import version
import json
from pendulum import datetime
import logging
import requests
import os

from airflow.decorators import task, dag
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.slack.hooks.slack import SlackHook

import mlflow

docs = """
Demonstrates orchestrating ML pipelines executed on Databricks with Airflow
"""

model_name = 'census_pred'

@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    doc_md=docs
)
def databricks_ml_retrain_example():

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
    
        logging.info(f'Training notebook run_id: {databricks_run_id}')

        databricks_hook = DatabricksHook()
        model_uri = databricks_hook.get_run_output(databricks_run_id)['notebook_output']['result']
        logging.info(f'Model URI: {model_uri}')
        
        model_version = mlflow.register_model(model_uri, model_name)

        logging.info(f'Name: {model_version.name}')
        logging.info(f'Version: {model_version.version}')

        return model_version.version
    

    @task()
    def submit_for_approval_to_stage(model_version, databricks_instance):

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
    def notify(model_version, transistion_request, databricks_instance):

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

        for version in registered_models['registered_model_databricks']['latest_versions']:
            if version['version'] == model_version:
                model_run_id = version['run_id']
                logging.info(f'Model run_id: {model_run_id}')


        response = requests.get(
            url=f'https://{databricks_instance}.cloud.databricks.com/api/2.0/mlflow/runs/get',
            headers={'Authorization': 'Bearer %s' % os.environ['DATABRICKS_TOKEN']},
            json={
                'run_id': model_run_id,
            }
        )
        
        if response.status_code != 200:
            raise Exception(response.text)

        run_data = json.loads(response.text)
        logging.info(run_data)
        metrics = run_data['run']['data']['metrics']

        validation_scores = list()
        for metric in metrics:
            if 'val_' in metric['key']:
                key = metric['key']
                value = metric['value']
                validation_scores.append({key:value})

        messege = f"""
*Model Transition Request:* ```{transistion_request}```
*Awating approval:* https://{databricks_instance}.cloud.databricks.com/#mlflow/models/census_pred/versions/{model_version}
*Name:* `{model_name}`
*run_id:* `{model_run_id}`
Validation Scores: ```{validation_scores}```
        """

        slack = SlackHook(slack_conn_id='slack_default')
        slack.call(
            api_method='chat.postMessage',
            json={
                'text': messege,
                'channel':"#integrations"
            }
        )


    retrain
    model_version = register_model(retrain.output['run_id'])
    transition_request = submit_for_approval_to_stage(model_version, "{{ var.value.databricks_instance}}")
    notify(model_version, transition_request, "{{ var.value.databricks_instance}}")
    

dag = databricks_ml_retrain_example()