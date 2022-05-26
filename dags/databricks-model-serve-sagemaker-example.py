import boto3
from pendulum import datetime
import logging
from pprint import pformat

from airflow.decorators import task, dag
from airflow.operators.python import ShortCircuitOperator

from mlflow.tracking import MlflowClient
from mlflow.sagemaker import SageMakerDeploymentClient

import pandas as pd

from include.sample_data import test_sample

docs = """
Demonstrates orchestrating ML model serving pipelines executed on Databricks with Airflow.

Checks if there is a model marked for `Staging` that does not have a `deployed: True` tag in the registry. If that
condition is met, it proceeds to deploy the new model version to Sagemaker and create an end point. Once the end point
is created a test on the endpoint with sample data is performed and if it gets a successful response then the model
in the registry is tagged as `deployed: True`.
"""

model_name = 'census_pred'
region = 'us-east-2'
model_uri = f'models:/{model_name}/Staging'
deployment_name = 'census-pred-deployment'
target_uri = f'sagemaker:/{region}'


@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    doc_md=docs
)
def databricks_model_serve_sagemaker_example():

    @task(multiple_outputs=True)
    def check_model_version_status():
        """Check if a new model version is available

        Collect info if there is a new model version that has been transitioned to Staging and determine its deployment
        status.

        Returns JSON with deployed status, version number, and model description
        """
        client = MlflowClient()

        # Iterate through all model versions and get only the one market for Staging.
        # Then gather info on its version number, and deployment tag info, and description.
        for mv in client.search_model_versions(f"name='{model_name}'"):
            model_version = dict(mv)
            if model_version['current_stage'] == 'Staging':
                logging.info(pformat(mv, indent=4))

                version_number = model_version['version']
                description = model_version['description']

                if 'deployed' not in model_version['tags']:
                    deployed = False
                elif 'deployed' in model_version['tags'] and model_version['tags']['deployed'] == 'False':
                    deployed = False
                else:
                    deployed = True

                return {'deployed': deployed, 'version': version_number, 'description': description}

        # If no model is marked for Staging, then return "Not Available"
        return {'deployed': 'Not Available', 'version': None, 'description': None}

    model_status_result = check_model_version_status()

    # Based on the output of the check_model_version_status() task determine if a new model needs to deployed and
    # proceed accordingly.
    new_version_confirmation = ShortCircuitOperator(
        task_id="new_version_confirmation",
        python_callable=lambda deployed: not deployed,
        op_kwargs=dict(deployed=model_status_result['deployed'])
    )

    @task
    def deploy_model(image_url: str, sagemaker_execution_arn: str):
        """Deploy new model to AWS Sagemaker

        Uses mlflow-pyfunc image and Sagemaker Execution Role to deploy new model and endpoint.
        """

        sagemaker = SageMakerDeploymentClient(target_uri)

        sagemaker.create_deployment(
            name=deployment_name,
            model_uri=model_uri,
            config=dict(
                image_url=f'{image_url}',
                execution_role_arn=f'{sagemaker_execution_arn}'
            )
        )

    @task()
    def test_model_endpoint():
        """Test sending requests to new endpoint

        Uses Sagemaker Runtime API to send a request with sample data to get a prediction.
        """

        # Read in JSON string of sample data to test with.
        df = pd.read_json(test_sample, orient='split')

        # Get prediction from Endpoint with sample data.
        sagemaker_runtime = boto3.client('sagemaker-runtime', region_name=region)

        predictions = sagemaker_runtime.invoke_endpoint(
            EndpointName=deployment_name,
            Body=df.to_json(orient="split"),
            ContentType='application/json; format=pandas-split'
        )

        logging.info('Predictions: ' + predictions['Body'].read().decode("ascii"))

    @task
    def mark_as_deployed(model_version: str):
        """Tag model as deployed in MLflow Registry

        If the previous tasks are successful this will add a "deployed: True" tag to the model version which was
        deployed as part of his pipeline.

        Keyword Arguments:
        model_version -- model version that was deployed as part of this pipeline.
        """

        client = MlflowClient()

        client.set_model_version_tag(
            name=model_name,
            version=model_version,
            key='deployed',
            value="True"
        )

    new_version_confirmation >> deploy_model(image_url="{{ var.value.mlflow_pyfunc_image_url }}", sagemaker_execution_arn="{{ var.value.sagemaker_execution_arn }}") >> test_model_endpoint() >> mark_as_deployed(
        model_status_result['version'])


dag = databricks_model_serve_sagemaker_example()
