import json
import logging
import pendulum

from collections import defaultdict
from functools import partial
from sqlalchemy import desc

from airflow.models import Variable, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import S3KeySensor, ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.db import provide_session

from aetn.operators.dwh_databricks_operators import DatabricksRunNowHook, DatabricksRunNowOperator
from aetn.operators.dwh_s3_operators import TimeAwareS3KeySensor, S3CopyOperator, S3StreamCopyOperator, HttpToS3Operator
from aetn.operators.dwh_custom_operators import *
from aetn.operators.dwh_mongo_operators import MongoToS3Operator, S3ToMongoOperator, MongoDeleteOperator
from aetn.operators.dwh_sftp_operators import SFTPToS3Operator, S3ToSFTPOperator
from aetn.operators.dwh_postgres_operators import PostgresOperatorWithAlert
from aetn.services.sns_alarm import construct_sns_callback

logger = logging.getLogger(__name__)


class AbstractTaskBuilder:
    def __init__(self, tasks_builder_service):
        self._task_builder_service = tasks_builder_service

    def _get_job_params(self, param_name, activity, transform_list=[]):
        """ Inject run_env into params in transform_list of param_name. """
        job_params = activity.get(param_name)
        for param in transform_list:
            if job_params.get(param):
                job_params[param] = job_params[param].format(run_env=self._task_builder_service.run_env)
        return job_params

    @staticmethod
    def _get_task_kwargs(activity):
        return {
            "retries": int(activity.get("retries", 0)),
            "trigger_rule": activity.get("trigger_rule") or TriggerRule.ALL_SUCCESS,
            "depends_on_past": activity.get("depends_on_past", False),
            "retry_delay": int(activity.get("retry_delay", 300))
        }

    def _add_task_callbacks(self, task, activity):
        """ Parse the task description and add desired callbacks. """
        date_lag = activity.get("date_lag", 0)
        task_name = activity["name"]
        task_type = activity.get("job_type") or activity["type"]
        sns_topic_map = Variable.get("job_sns_topic_map", deserialize_json=True)['map']
        sns_default_topic_arn = sns_topic_map['default']
        sns_topic_arn = None
        if task_name in sns_topic_map:
            sns_topic_arn = sns_topic_map[task_name]
        for on_fail_activity in activity.get("on_fail") or []:
            if "sns_alarm" in on_fail_activity:
                task.on_failure_callback = partial(construct_sns_callback, sns_topic_arn, sns_default_topic_arn,
                                                   task_name, task_type, date_lag, False,
                                                   self._task_builder_service.run_env)
        for on_success_activity in activity.get("on_success") or []:
            if "sns_alarm" in on_success_activity:
                task.on_success_callback = partial(construct_sns_callback, sns_topic_arn, sns_default_topic_arn,
                                                   task_name, task_type, date_lag, True,
                                                   self._task_builder_service.run_env)

    def _set_upstream(self, task, activity):
        depends_on = activity.get("depends_on")
        if depends_on:
            for task_id in depends_on:
                dependency_task = self._task_builder_service.get_task_from_bag(task.dag.dag_id, task_id)
                if not dependency_task:
                    raise Exception("Task id not defined, you must defined it in yaml config "
                                    "before first usage: %s" % task_id)
                else:
                    task.set_upstream(dependency_task)

    def build_task(self, task_id, activity, dag):
        raise NotImplementedError


class DatabricksTaskBuilder(AbstractTaskBuilder):

    def __init__(self, tasks_builder_service):
        super(DatabricksTaskBuilder, self).__init__(tasks_builder_service)
        self._hook = DatabricksRunNowHook()
        self._databricks_dispatcher_job_id = Variable.get("databricks_dispatcher_job_id")

    def build_task(self, task_id, activity, dag):
        job_name = activity.get("job_name") or activity["name"]
        date_lag = activity.get("date_lag", 0)
        task = DatabricksRunNowOperator(
            task_id=task_id,
            json=self._databricks_dispatcher_params(job_name=job_name,
                                                    job_type=activity['job_type'],
                                                    job_params=activity.get("job_params"),
                                                    run_missing_dates=activity.get('run_missing_dates', 'yes'),
                                                    only_available_data=activity.get('only_available_data', 'no'),
                                                    only_available_hours=activity.get('only_available_hours', 'no')
                                                    ),
            dag=dag,
            date_lag=date_lag,
            **self._get_task_kwargs(activity)
        )

        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task

    def _databricks_dispatcher_params(self, job_name, job_type, job_params=None, run_missing_dates='yes',
                                      only_available_data='no', only_available_hours='no'):
        job_params = job_params or {}
        return {
            'job_id': self._databricks_dispatcher_job_id,
            'notebook_params': [
                {"key": "env", "value": self._task_builder_service.run_env},
                {"key": "job_name", "value": job_name},
                {"key": "job_type", "value": job_type},
                {"key": "job_params_json", "value": json.dumps(job_params)},
                {"key": "run_missing_dates", "value": run_missing_dates},
                {"key": "only_available_data", "value": only_available_data},
                {"key": "only_available_hours", "value": only_available_hours}
            ]
        }


class S3SensorTaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("s3_key", activity, ["bucket_name"])
        kwargs.update(self._get_task_kwargs(activity))
        date_lag = kwargs.pop("date_lag", 0)
        data_range = kwargs.pop("data_range", [0, 1])
        if kwargs.pop("run_env_aware_bucket_key", False):
            # get bucket_key from bucket_keys[run_env]
            kwargs["bucket_key"] = kwargs["bucket_keys"][self._task_builder_service.run_env]
            kwargs.pop("bucket_keys")

        if kwargs.pop("time_aware_bucket_key", False):
            task = TimeAwareS3KeySensor(
                task_id=task_id,
                dag=dag,
                date_lag=date_lag,
                data_range=data_range,
                **kwargs
            )
        else:
            task = S3KeySensor(
                task_id=task_id,
                dag=dag,
                **kwargs
            )

        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class MongoToS3CopyTaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, ["s3_bucket"])
        kwargs.update(self._get_task_kwargs(activity))
        task = MongoToS3Operator(
            task_id=task_id,
            dag=dag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class S3ToMongoCopyTaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, ["s3_bucket"])
        kwargs.update(self._get_task_kwargs(activity))
        task = S3ToMongoOperator(
            task_id=task_id,
            dag=dag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class MongoDeleteTaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, [])
        kwargs.update(self._get_task_kwargs(activity))
        task = MongoDeleteOperator(
            task_id=task_id,
            dag=dag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class SFTPToS3TaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, ["dest_bucket"])
        kwargs.update(self._get_task_kwargs(activity))
        date_lag = kwargs.pop("date_lag", 0)
        task = SFTPToS3Operator(
            task_id=task_id,
            dag=dag,
            date_lag=date_lag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class S3ToSFTPTaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, ["source_bucket"])
        kwargs.update(self._get_task_kwargs(activity))
        date_lag = kwargs.pop("date_lag", 0)
        task = S3ToSFTPOperator(
            task_id=task_id,
            dag=dag,
            date_lag=date_lag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class HttpToS3TaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, ["dest_bucket"])
        kwargs.update(self._get_task_kwargs(activity))
        task = HttpToS3Operator(
            task_id=task_id,
            dag=dag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class FacebookToS3TaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, ["dest_bucket"])
        kwargs.update(self._get_task_kwargs(activity))
        task = FacebookToS3Operator(
            task_id=task_id,
            dag=dag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class TrendsTransformTaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, [])
        kwargs.update(self._get_task_kwargs(activity))
        task = TrendsTransformOperator(
            task_id=task_id,
            dag=dag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class PythonScriptTaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, [])
        kwargs.update(self._get_task_kwargs(activity))
        task = PythonScriptOperator(
            task_id=task_id,
            dag=dag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class S3CopyTaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, ["source_bucket", "dest_bucket"])
        kwargs.update(self._get_task_kwargs(activity))
        date_lag = kwargs.pop("date_lag", 0)
        task = S3CopyOperator(
            task_id=task_id,
            dag=dag,
            date_lag=date_lag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class S3StreamCopyTaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, ["source_bucket", "dest_bucket"])
        kwargs.update(self._get_task_kwargs(activity))
        date_lag = kwargs.pop("date_lag", 0)
        task = S3StreamCopyOperator(
            task_id=task_id,
            dag=dag,
            date_lag=date_lag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class GigyaToS3TaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, ["dest_bucket"])
        kwargs.update(self._get_task_kwargs(activity))
        task = GigyaToS3Operator(
            task_id=task_id,
            dag=dag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class MailChimpToS3TaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, ["dest_bucket"])
        kwargs.update(self._get_task_kwargs(activity))
        task = MailChimpToS3Operator(
            task_id=task_id,
            dag=dag,
            **kwargs
        )
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class ExternalTaskSensorTaskBuilder(AbstractTaskBuilder):
    """
    Waits for a task to complete in a different DAG
    """

    @provide_session
    def build_task(self, task_id, activity, dag, session=None):

        def execution_date(*args):
            """
            Returns external task execution date and time.
            Date is taken from current DAG execution date and time is read
            from last external DAG run date
            """
            TI = TaskInstance
            _ti = session.query(TI).filter(
                TI.dag_id == activity['external_dag_id'],
                TI.task_id == activity['external_task_id']
            ).order_by(desc(TI.execution_date)).first()

            dttm = '{} {}'.format(
                args[0].to_date_string(),
                _ti.execution_date.strftime('%H:%M:%S')
            )

            return pendulum.parse(dttm)

        task = ExternalTaskSensor(
            task_id=task_id,
            dag=dag,
            external_dag_id=activity['external_dag_id'],
            external_task_id=activity['external_task_id'],
            execution_date_fn=execution_date
        )

        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class PostgresOperatorTaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        kwargs = self._get_job_params("job_params", activity, ['sql', 'message_format', 'subject_format', 'postgres_conn_id'])
        kwargs.update(self._get_task_kwargs(activity))
        kwargs['run_env'] = run_env=self._task_builder_service.run_env
        task = PostgresOperatorWithAlert(
            task_id=task_id,
            dag=dag,
            **kwargs)
        self._add_task_callbacks(task, activity)
        self._set_upstream(task, activity)
        return task


class DummyTaskBuilder(AbstractTaskBuilder):
    def build_task(self, task_id, activity, dag):
        task = DummyOperator(
            task_id=task_id,
            dag=dag,
            **self._get_task_kwargs(activity)
        )
        self._set_upstream(task, activity)
        return task


class TasksBuilderService:
    BUILDERS = {}
    TASKS_BAG = defaultdict(dict)

    def __init__(self, run_env):
        self.run_env = run_env

    def register_builder(self, activity_type, builder_cls):
        self.BUILDERS[activity_type] = builder_cls(self)

    def build_task(self, activity, dag):
        task_id = self._task_id(activity['name'])
        task = self.TASKS_BAG[dag.dag_id].get(task_id)
        if task:
            return task

        if not activity['type'] in self.BUILDERS:
            raise Exception("Task builder for type '%s' were not registered" % activity['type'])
        task = self.BUILDERS[activity["type"]].build_task(task_id, activity, dag)
        self.add_task_to_bag(dag.dag_id, task_id, task)
        return task

    def get_task_from_bag(self, dag_id, task_id):
        return self.TASKS_BAG[dag_id].get(task_id)

    def add_task_to_bag(self, dag_id, task_id, task):
        self.TASKS_BAG[dag_id][task_id] = task

    @staticmethod
    def _task_id(_id):
        return str(_id)
