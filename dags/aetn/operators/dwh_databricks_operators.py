from datetime import timedelta

import six
import time

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.databricks_hook import DatabricksHook
from airflow.models import BaseOperator, Variable

RUN_NOW_ENDPOINT = ('POST', 'api/2.0/jobs/run-now')
GET_RUN_OUTPUT_ENDPOINT = ('GET', 'api/2.0/jobs/runs/get-output')


class DatabricksRunNowHook(DatabricksHook):

    def run_now(self, json):
        """
        Utility function to call the ``api/2.0/jobs/run-now`` endpoint.

        :param json: The data used in the body of the request to the ``run-now`` endpoint.
        :type json: dict
        :return: the run_id as a string
        :rtype: str
        """
        response = self._do_api_call(RUN_NOW_ENDPOINT, json)
        return response['run_id']

    def get_run_output(self, run_id, sleep=5):
        import json

        running, response = True, None
        while running:
            response = self._do_api_call(GET_RUN_OUTPUT_ENDPOINT, {"run_id": run_id})
            running = (response['metadata']['state']['life_cycle_state']
                       not in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"])
            if running:
                time.sleep(sleep)

        if response and 'notebook_output' in response:
            notebook_output = response['notebook_output'].get('result', "")
            return json.loads(notebook_output) if notebook_output else None


class DatabricksRunNowOperator(BaseOperator):
    """
    Submits an Spark job run to Databricks using the
    `api/2.0/jobs/runs/submit
    <https://docs.databricks.com/api/latest/jobs.html#run-now`_
    API endpoint.

    There are two ways to instantiate this operator.

    In the first way, you can take the JSON payload that you typically use
    to call the ``api/2.0/jobs/runs/run-now`` endpoint and pass it directly
    to our ``DatabricksSubmitRunOperator`` through the ``json`` parameter.
    For example ::
        {
          'job_id': 1,
          'notebook_params': {
            'dry-run': 'true',
            'oldest-time-to-consider': '1457570074236'
          }
        }

        notebook_run = DatabricksRunNowOperator(task_id='notebook_run', json=json)

    In the case where both the json parameter **AND** the named parameters
    are provided, they will be merged together. If there are conflicts during the merge,
    the named parameters will take precedence and override the top level ``json`` keys.

    Currently the named parameters that ``DatabricksRunNowOperator`` supports are
        - ``job_id``
        - ``jar_params``
        - ``notebook_params``
        - ``python_params``
        - ``spark_submit_params``
    """
    # Used in airflow.models.BaseOperator
    template_fields = ('json',)
    # Databricks brand color (blue) under white text
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'

    def __init__(
            self,
            json=None,
            job_id=None,
            notebook_task=None,
            jar_params=None,
            notebook_params=None,
            python_params=None,
            spark_submit_params=None,
            databricks_conn_id='databricks_default',
            polling_period_seconds=30,
            databricks_retry_limit=1,
            date_lag=0,
            **kwargs):
        """
        Creates a new ``DatabricksRunNowOperator``.
        """
        super(DatabricksRunNowOperator, self).__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit

        if job_id is not None:
            self.json['job_id'] = job_id
        if notebook_task is not None:
            self.json['notebook_task'] = notebook_task
        if jar_params is not None:
            self.json['jar_params'] = jar_params
        if notebook_params is not None:
            self.json['notebook_params'] = notebook_params
        if spark_submit_params is not None:
            self.json['spark_submit_params'] = spark_submit_params
        if python_params is not None:
            self.json['python_params'] = python_params

        self.json = self._deep_string_coerce(self.json)
        # This variable will be used in case our task gets killed.
        self.run_id = None
        self.date_lag = date_lag

    def _deep_string_coerce(self, content, json_path='json'):
        """
        Coerces content or all values of content if it is a dict to a string. The
        function will throw if content contains non-string or non-numeric types.

        The reason why we have this function is because the ``self.json`` field must be a dict
        with only string values. This is because ``render_template`` will fail for numerical values.
        """
        c = self._deep_string_coerce
        if isinstance(content, six.string_types):
            return content
        elif isinstance(content, six.integer_types+(float,)):
            return str(content)
        elif isinstance(content, (list, tuple)):
            return [c(e, '{0}[{1}]'.format(json_path, i)) for i, e in enumerate(content)]
        elif isinstance(content, dict):
            return {k: c(v, '{0}[{1}]'.format(json_path, k))
                    for k, v in list(content.items())}
        else:
            param_type = type(content)
            msg = 'Type {0} used for parameter {1} is not a number or a string' \
                    .format(param_type, json_path)
            raise AirflowException(msg)

    def _log_run_page_url(self, url):
        self.log.info('View run status, Spark UI, and logs at %s', url)

    def get_hook(self):
        return DatabricksRunNowHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit)

    def execute(self, context):
        self._add_dates_to_notebook_params(context)
        self._update_notebook_params_from_variable()
        hook = self.get_hook()
        self.run_id = self._run_task(hook)
        run_page_url = hook.get_run_page_url(self.run_id)
        self.log.info('Run submitted with run_id: %s', self.json['job_id'])
        self.log.info("Run submitted with params: %s", self.json)
        self._log_run_page_url(run_page_url)
        while True:
            run_state = hook.get_run_state(self.run_id)
            if run_state.is_terminal:
                if run_state.is_successful:
                    self.log.info('%s completed successfully.', self.task_id)
                    self._log_run_page_url(run_page_url)
                    return
                else:
                    error_message = '{t} failed with terminal state: {s}'.format(
                        t=self.task_id,
                        s=run_state)
                    raise AirflowException(error_message)
            else:
                self.log.info('%s in run state: %s', self.task_id, run_state)
                self._log_run_page_url(run_page_url)
                self.log.info('Sleeping for %s seconds.', self.polling_period_seconds)
                time.sleep(self.polling_period_seconds)

    def _run_task(self, hook):
        return hook.run_now(self.json) if 'job_id' in self.json else hook.submit_run(self.json)

    def on_kill(self):
        hook = self.get_hook()
        hook.cancel_run(self.run_id)
        self.log.info(
            'Task: %s with run_id: %s was requested to be cancelled.',
            self.task_id, self.run_id
        )

    def _add_dates_to_notebook_params(self, context):
        run_date = (context['execution_date'] + timedelta(days=self.date_lag)).strftime("%Y-%m-%d")
        self.json['notebook_params'].append({"key": "run_date", "value": run_date})
        self.json['notebook_params'].append({"key": "start_date",
                                             "value": self.start_date.strftime('%Y-%m-%d %H:%M:%S')})

    def _update_notebook_params_from_variable(self):
        try:
            data = Variable.get("%s.%s" % (self.dag_id, self.task_id), deserialize_json=True)
        except KeyError:
            return

        self.log.info("Found variable %s.%s data: %s", self.dag_id, self.task_id, data)
        for key in ["run_missing_dates", "only_available_data", "only_available_hours"]:
            if key in data:
                for param in self.json['notebook_params']:
                    if param["key"] == key:
                        self.log.info("Overwriting %s=%s", key, data[key])
                        param["value"] = data[key]
                        self.log.info(param)
