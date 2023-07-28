
from azureml.core.authentication import ServicePrincipalAuthentication
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azureml.core import Workspace, Experiment
from azureml.pipeline.core import PipelineEndpoint
import datetime

SCORING_MODEL_NAME = vj_MODEL_NAME
PIPELINE_NAME = f"{SCORING_MODEL_NAME}"
EXPERIMENT_NAME = f'{SCORING_MODEL_NAME}'

VAULT_NAME = 'satsprdmdpkeys'
VAULT_URL = f"https://{VAULT_NAME}.vault.azure.net/"
Service_principal_key='azureml-principal-key'


def get_secret(secret_name):
  credential = DefaultAzureCredential()
  secret_client = SecretClient(vault_url=VAULT_URL, credential=credential)
  return secret_client.get_secret(secret_name).value


# Set scoring date. If it's passed into the job at runtime, try to use it (if it's parseable)
# Otherwise, default to most recent monday.
try:
    scoring_date = datetime.datetime.strptime(vj_SCORING_DATE, '%Y-%m-%d').date().strftime('%Y-%m-%d')
except (ValueError, TypeError):
    today = datetime.date.today()
    scoring_date = (today - datetime.timedelta(days=today.weekday())).strftime('%Y-%m-%d')
    print(f"Could not parse \'vj_SCORING_DATE\' as a date, setting scoring date to most recent Monday: {scoring_date}")

svc_pr = ServicePrincipalAuthentication(
    tenant_id="b15a587d-acc9-4644-aa51-b56dee85c304",
    service_principal_id="0600df20-70a0-4da8-ac60-124427817405",
    service_principal_password=get_secret(Service_principal_key))

    
# connect to workspace and experiment
ws = Workspace(
    subscription_id=ve_AML_SUBSCRIPTION_ID,
    resource_group=ve_AML_RESOURCE_GROUP,
    workspace_name=ve_AML_WORKSPACE_NAME,
       auth=svc_pr
)

exp = Experiment(workspace=ws, name=EXPERIMENT_NAME)

run_parameters = {'scoring_date': scoring_date, 'mdp_env': ve_Environment}
endpoint_to_run = PipelineEndpoint.get(workspace=ws, name=f"{PIPELINE_NAME}")
run = exp.submit(config=endpoint_to_run, pipeline_parameters=run_parameters, tags={'pipeline_type': 'scoring', 'launched_from': 'Matillion', **run_parameters})

run.wait_for_completion(show_output=True)

