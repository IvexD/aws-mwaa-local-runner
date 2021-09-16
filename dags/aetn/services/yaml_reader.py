import glob
import logging

import yaml
from airflow import DAG

logger = logging.getLogger(__name__)


def load_dags_from_yaml(yaml_config_dir, task_builder_service):
    logger.info("Looking for DAG yaml configs with pattern: '%s*.yaml'", yaml_config_dir)
    for file_name_yaml in glob.glob('%s*.yaml' % yaml_config_dir, recursive=True):
        logger.info("Found yaml config: %s", file_name_yaml)
        with open(file_name_yaml) as f:
            try:
                dag_meta = yaml.safe_load(f)
            except yaml.YAMLError as exc:
                logger.exception(exc)
                continue

        try:
            logger.info("Found dag_id: %s", dag_meta['dag_id'])
            logger.debug("start_date: %s", str(dag_meta['start_date']))
            dag = DAG(dag_id=dag_meta['dag_id'],
                      schedule_interval=dag_meta['schedule'],
                      default_args={'owner': 'airflow', 'start_date': dag_meta['start_date']},
                      max_active_runs=int(dag_meta.get("max_active_runs", 1)))

            for name, activities in dag_meta['pipeline'].items():
                logger.debug("Found %s pipeline", name)
                for activity in activities:
                    task_builder_service.build_task(activity, dag)
            yield dag
        except:
            logger.exception("DAG error in %s", file_name_yaml)
