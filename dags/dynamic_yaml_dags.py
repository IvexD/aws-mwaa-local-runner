"""
Before you use this DAG, you need to add 2 airflow variables:
- databricks_dispatcher_job_id = 12158
- databricks_dependencies_job_id = 12181

Your databricks_default connection has to use access token. To setup databricks connection go to Admin -> Connections -> databricks_default edit
Setup:
- hostname: dbc-4948616e-287a.cloud.databricks.com
- {"token": "YOUR DATABRICKS API TOKEN"}

Databricks :

1. POC databricks jobs can be found here:
https://dbc-4948616e-287a.cloud.databricks.com/#joblist/POC

2. POC Notebooks are available on: /Users/marcin.bakowski@intive.com/airflow/poc

"""
import logging
import os

from airflow.models import Variable

from aetn.services.task_builder import *
from aetn.services.yaml_reader import load_dags_from_yaml

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("POC DYNAMIC DAG")
logger.info("Hello world! It Works!")

RUN_ENV = Variable.get("run_env")
YAML_INPUT_DIR = os.path.join(os.environ.get("AIRFLOW_HOME", ""), "dags/datapipline/**/")


# prepare task builder service
task_builder_service = TasksBuilderService(RUN_ENV)
task_builder_service.register_builder("databricks", DatabricksTaskBuilder)
task_builder_service.register_builder("s3_sensor", S3SensorTaskBuilder)
task_builder_service.register_builder("external_task_sensor", ExternalTaskSensorTaskBuilder)
task_builder_service.register_builder("s3_copy", S3CopyTaskBuilder)
task_builder_service.register_builder("s3_to_s3_copy", S3StreamCopyTaskBuilder)
task_builder_service.register_builder("sftp_to_s3_copy", SFTPToS3TaskBuilder)
task_builder_service.register_builder("s3_to_sftp_copy", S3ToSFTPTaskBuilder)
task_builder_service.register_builder("http_to_s3_copy", HttpToS3TaskBuilder)
task_builder_service.register_builder("facebook_to_s3_copy", FacebookToS3TaskBuilder)
task_builder_service.register_builder("mongo_to_s3_copy", MongoToS3CopyTaskBuilder)
task_builder_service.register_builder("s3_to_mongo_copy", S3ToMongoCopyTaskBuilder)
task_builder_service.register_builder("mongo_delete", MongoDeleteTaskBuilder)
task_builder_service.register_builder("gigya_to_s3_copy", GigyaToS3TaskBuilder)
task_builder_service.register_builder("mailchimp_to_s3_copy", MailChimpToS3TaskBuilder)
task_builder_service.register_builder("trends_transformer", TrendsTransformTaskBuilder)
task_builder_service.register_builder("python_script", PythonScriptTaskBuilder)
task_builder_service.register_builder("postgres_with_alert", PostgresOperatorTaskBuilder)
task_builder_service.register_builder("dummy", DummyTaskBuilder)

# load and register das
for dag in load_dags_from_yaml(YAML_INPUT_DIR, task_builder_service):
    globals()[dag.dag_id] = dag
