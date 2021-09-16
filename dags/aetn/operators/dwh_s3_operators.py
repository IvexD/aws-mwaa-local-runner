import subprocess
import json
import os
import string
import random
import s3fs

from datetime import timedelta

from airflow.operators.sensors import S3KeySensor
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from aetn.hooks.dwh_custom_hooks import HttpsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator


class TimeAwareS3KeySensor(S3KeySensor):

    @apply_defaults
    def __init__(self,
                 date_lag=0,
                 data_range=None,
                 *args,
                 **kwargs):
        super(TimeAwareS3KeySensor, self).__init__(*args, **kwargs)
        self.date_lag = date_lag
        self.data_range = data_range

    def poke(self, context):
        from airflow.hooks.S3_hook import S3Hook
        hook = S3Hook(aws_conn_id=self.aws_conn_id)
        # inject execution_date into bucket_key
        execution_date = context["execution_date"] + timedelta(self.date_lag)
        bucket_key = execution_date.strftime(self.bucket_key)
        full_url = "s3://" + self.bucket_name + "/" + bucket_key

        range_start = self.data_range[0]
        # Add one so it's inclusive of the range end
        range_end = self.data_range[1] + 1
        hook_pokes = [False for i in range(range_start, range_end)]
        for idx, _ in enumerate(hook_pokes):
            range_aware_key = bucket_key.format(idx=idx)
            full_url = "s3://" + self.bucket_name + "/" + range_aware_key
            self.log.info('Poking for key : {full_url}'.format(**locals()))
            if self.wildcard_match:
                hook_pokes[idx] = hook.check_for_wildcard_key(range_aware_key,
                                                   self.bucket_name)
            else:
                hook_pokes[idx] = hook.check_for_key(range_aware_key, self.bucket_name)
        return all(hook_pokes)


class S3CopyOperator(BaseOperator):
    """
    Copies data from a source S3 location to destination S3 location.
    """

    ui_color = '#f9c915'
    cmd_sync = 'aws s3 sync {source_uri} {dest_uri} --acl bucket-owner-full-control --exclude="*" --include="{source_key}"'
    cmd_cp = 'aws s3 cp {source_uri} {dest_uri} --acl bucket-owner-full-control'
    cmd_ls_list = "aws s3 ls {source_uri} | awk '{{print $4}}'"
    cmd_ls = "aws s3 ls {source_uri}"
    cmd_rm = 'rm -rf {source_uri}'

    @apply_defaults
    def __init__(
            self,
            source_bucket,
            dest_bucket,
            source_key=None,
            dest_key=None,
            s3_conn_id=None,
            date_lag=0,
            local=False,
            *args, **kwargs):
        super(S3CopyOperator, self).__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.source_bucket = source_bucket
        self.source_key = source_key
        self.dest_bucket = dest_bucket
        self.dest_key = dest_key
        self.temp_path = '/tmp/s3_copy/'
        self.cmd = self.cmd_cp
        self.date_lag = date_lag
        self.local = local

    def run_cmd(self, cmd, error_message, env=None):
        self.log.info("Running cmd: {}".format(cmd))
        try:
            cmd_output = subprocess.check_output(cmd, shell=True, env=env)
        except subprocess.CalledProcessError:
            raise AirflowException(error_message)
        return cmd_output

    def execute(self, context):
        today = context["execution_date"] + timedelta(days=self.date_lag)
        yesterday = context["execution_date"] + timedelta(days=self.date_lag) + timedelta(-1)
        # Generate unique tmp path
        self.temp_path = self.temp_path + ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        self.source_key = self.source_key.format(today=today.strftime("%Y-%m-%d"), yesterday=yesterday.strftime("%Y-%m-%d"))
        self.dest_key = self.dest_key.format(today=today.strftime("%Y-%m-%d"), yesterday=yesterday.strftime("%Y-%m-%d"))
        self.source_key = today.strftime(self.source_key)
        self.dest_key = today.strftime(self.dest_key)
        self.log.info("Copying file {} to S3 path {}".format(self.source_key, self.dest_key))
        source_uri = 's3://{}/{}'.format(self.source_bucket, self.source_key)
        dest_uri = 's3://{}/{}'.format(self.dest_bucket, self.dest_key)
        source_key = self.source_key
        if '*' in self.source_key:
            sufix = '*'
            split_data = self.source_key.split('*', 1)
            prefix = split_data[0]
            if len(split_data) == 2:
                sufix += split_data[1]
            source_uri = 's3://{}/{}'.format(self.source_bucket, prefix)
            source_key = sufix
            self.cmd = self.cmd_sync
        # If the access keys are set, set the env variables. Since we cannot use two pairs of credentials in a single
        # AWS request, need to copy to local and then back to destination bucket.
        if self.s3_conn_id:
            s3_source_hook = S3Hook(aws_conn_id=self.s3_conn_id)
            s3_destination_hook = S3Hook(aws_conn_id=None)
            conn_data = s3_source_hook.get_connection(self.s3_conn_id).extra_dejson
            connection_env = os.environ.copy()
            connection_env['AWS_ACCESS_KEY_ID'] = conn_data.get('aws_access_key_id', None)
            connection_env['AWS_SECRET_ACCESS_KEY'] = conn_data.get('aws_secret_access_key', None)
            # First check if any files match the challenge in the custom bucket
            self.log.info('Listing folder: {}'.format(source_uri))
            cmd_output = self.run_cmd(self.cmd_ls_list.format(source_uri=source_uri), "Listing source folder failed.", env=connection_env)
            server_files = cmd_output.decode('utf-8').split('\n')
            self.log.info('Found files: {}'.format(server_files))
            if len(server_files) == 0:
                raise AirflowException('Listing source folder failed with an empty list.')
            for server_file in server_files:
                self.run_cmd(self.cmd.format(source_uri=source_uri, dest_uri=self.temp_path, source_key=server_file),
                             "Copy sync from S3 custom connection failed.", env=krux_env)
                self.run_cmd(self.cmd.format(source_uri=self.temp_path, dest_uri=dest_uri, source_key=server_file),
                             "Copy sync to S3 failed.")
                self.run_cmd(self.cmd_rm.format(source_uri=self.temp_path), "Removal of temp file failed.")
        else:
            if self.local:
                # Copy files to localhost location
                if len(self.source_bucket) > 0:
                    dest_uri = self.dest_key
                else:
                    source_uri = source_uri.replace("s3://", "", 1)
            # First check if any files match the challenge
            self.run_cmd(self.cmd_ls.format(source_uri=source_uri), "Bucket key does not exist in S3.")
            self.run_cmd(self.cmd.format(source_uri=source_uri, dest_uri=dest_uri, source_key=source_key), "Copy sync to S3 failed.")
        self.log.info("Copy successful")


class S3StreamCopyOperator(BaseOperator):
    """
    Copies data from a source S3 location directly to destination S3 location, without using temporary files.
    """

    @apply_defaults
    def __init__(
            self,
            source_bucket,
            dest_bucket,
            source_key=None,
            dest_key=None,
            s3_conn_id=None,
            date_lag=0,
            local=False,
            *args, **kwargs):
        super(S3StreamCopyOperator, self).__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.source_bucket = source_bucket
        self.source_key = source_key
        self.dest_bucket = dest_bucket
        self.dest_key = dest_key
        self.date_lag = date_lag
        self.local = local

    def execute(self, context):
        range_end = context["execution_date"] + timedelta(days=self.date_lag)
        range_start = context["execution_date"] + timedelta(days=self.date_lag) + timedelta(-1)

        self.source_key = self.source_key.format(range_end=range_end.strftime("%Y-%m-%d"), range_start=range_start.strftime("%Y-%m-%d"))
        self.dest_key = self.dest_key.format(range_end=range_end.strftime("%Y-%m-%d"), range_start=range_start.strftime("%Y-%m-%d"))
        self.source_key = range_end.strftime(self.source_key)
        self.dest_key = range_end.strftime(self.dest_key)
        self.log.info("Copying file {} to S3 path {}".format(self.source_key, self.dest_key))
        source_uri = 's3://{}/{}'.format(self.source_bucket, self.source_key)
        dest_uri = 's3://{}/{}'.format(self.dest_bucket, self.dest_key)
        source_key = self.source_key
        if '*' in self.source_key:
            suffix = '*'
            split_data = self.source_key.split('*', 1)
            prefix = split_data[0]
            if len(split_data) == 2:
                suffix += split_data[1]
            source_uri = 's3://{}/{}'.format(self.source_bucket, prefix)
            source_key = suffix

        if self.s3_conn_id:
            s3_source_hook = S3Hook(aws_conn_id=self.s3_conn_id)
            s3_destination_hook = S3Hook(aws_conn_id=None)
            conn_data = s3_source_hook.get_connection(self.s3_conn_id).extra_dejson
            aws_access_key_id = conn_data['aws_access_key_id']
            aws_secret_access_key = conn_data['aws_secret_access_key']
            aws_session_token = conn_data.get('aws_session_token', None)
            s3 = s3fs.S3FileSystem(anon=False, key=aws_access_key_id, secret=aws_secret_access_key, token=aws_session_token)
            dest_s3 = s3fs.S3FileSystem(anon=False)

            # First check if any files match the challenge in the custom bucket
            self.log.info('Listing folder: {}'.format(source_uri))
            source_files = s3.ls(source_uri)
            self.log.info('Found files: {}'.format(source_files))
            nb_source_files = len(source_files)
            if nb_source_files == 0:
                raise AirflowException('Listing source folder failed with an empty list.')
            for source_file in source_files:
                filename = source_file.split('/')[-1]
                target_file = dest_uri + filename
                target_exists = dest_s3.exists(target_file)
                if target_exists:
                    self.log.info('Target file: {} already exists, skipping.'.format(target_file))
                else:
                    with s3.open(source_file, 'rb') as file_obj:
                        s3_destination_hook.load_file_obj(file_obj, self.dest_key + filename, bucket_name=self.dest_bucket)
        else:
            raise AirflowException('There is no s3_conn_id set, please use s3_copy instead of s3_to_s3_copy.')

        dest_files = s3_destination_hook.list_keys(bucket_name=self.dest_bucket, prefix=self.dest_key)
        if len(dest_files) != nb_source_files:
            s3_destination_hook.delete_objects(bucket=self.dest_bucket, keys=dest_files) 
            raise AirflowException('Number of files in destination folder was not equal to number of source files.')
        self.log.info("Copy successful")


class HttpToS3Operator(BaseOperator):
    """
    Operator for calling an endpoint on an HTTP system to execute an action and saves the response to S3.
    """

    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 endpoints,
                 dest_bucket,
                 dest_key,
                 method='GET',
                 data=None,
                 headers=None,
                 response_check=None,
                 extra_options=None,
                 http_conn_id=None,
                 *args, **kwargs):
        super(HttpToS3Operator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoints = endpoints
        self.headers = headers or {}
        self.data = data or {}
        self.response_check = response_check
        self.extra_options = extra_options or {}
        self.tmp_file_path = '/tmp/http_to_s3/'
        self.dest_bucket = dest_bucket
        self.dest_key = dest_key

    def query_json(self, endpoint, data):
        """ Query using provided http hook, check for errors and return as JSON. """
        http = HttpsHook(self.method, http_conn_id=self.http_conn_id)
        response = http.run(endpoint,
                            data,
                            self.headers,
                            self.extra_options)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
        else:
            if response.status_code != 200:
                raise AirflowException("Response returned %d" % response.status_code)
        json_response = json.loads(response.text)
        return json_response

    def execute(self, context):
        today = context["execution_date"]
        yesterday = context["execution_date"] + timedelta(-1)
        # Generate unique tmp path
        self.tmp_file_path = self.tmp_file_path + ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        self.data['until'] = today.strftime(self.data.get('until', '%Y-%m-%d'))
        self.data['since'] = yesterday.strftime(self.data.get('since', '%Y-%m-%d'))

        self.log.info("Calling HTTPS method")
        self.dest_key = today.strftime(self.dest_key)
        for idx, endpoint in enumerate(self.endpoints):
            endpoint = endpoint.format(today=today, yesterday=yesterday)
            self.dest_key = self.dest_key.format(num=str(idx))
            response_json = self.query_json(endpoint, json.dumps(self.data))
            with open(self.tmp_file_path, 'w+') as fh:
                fh.write(json.dumps(response_json))

            source_uri = '{}'.format(self.tmp_file_path)
            dest_uri = 's3://{}/{}'.format(self.dest_bucket, self.dest_key)
            self.log.info("HTTPS request successful, starting upload from {} to S3 {}".format(source_uri, dest_uri))
            try:
                subprocess.run('aws s3 cp {source_uri} {dest_uri} --acl bucket-owner-full-control'.format(source_uri=source_uri, dest_uri=dest_uri), shell=True, check=True)
            except subprocess.CalledProcessError:
                raise AirflowException("Upload to S3 failed.")
            self.log.info("Upload successful")
            try:
                subprocess.run('rm {source_uri}'.format(source_uri=self.tmp_file_path), shell=True, check=True)
            except subprocess.CalledProcessError:
                raise AirflowException("Removal of temp file failed.")
