from airflow.providers.databricks.hooks.databricks import DatabricksHook

def get_notebook_output(databricks_run_id: str):
    databricks_hook = DatabricksHook()

    response = databricks_hook._do_api_call(("GET", f'api/2.0/jobs/runs/get-output?run_id={databricks_run_id}'), {})
    
    return response['notebook_output']['result']