import json
import subprocess
import string
import random
import time
import pandas as pd
import os
import runpy

from pytrends.request import TrendReq
from datetime import timedelta

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from aetn.hooks.dwh_custom_hooks import GigyaHook, MailChimpHook, HttpsHook
from airflow.exceptions import AirflowException


class GigyaToS3Operator(BaseOperator):
    """
    Calls a Gigya endpoint, downloads the data and saves it to S3.
    """

    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self, gigya_conn_id, dest_bucket, dest_key, *args, **kwargs):
        super(GigyaToS3Operator, self).__init__(*args, **kwargs)
        self.gigya_conn_id = gigya_conn_id
        self.dest_bucket = dest_bucket
        self.dest_key = dest_key
        self.limit = 1000
        self.tmp_file_path = '/tmp/gigya_s3/'

    def build_query(self, start, end):
        """ """
        query = "SELECT * FROM accounts"
        filters = []
        if start:
            filters.append("lastUpdatedTimestamp >= {start}".format(
                start=int(start.timestamp())))
        if end:
            filters.append("lastUpdatedTimestamp <= {end}".format(
                end=int(end.timestamp())))
        if len(filters) > 0:
            query += " WHERE " + " AND ".join(filters)
        query += " LIMIT {}".format(self.limit)
        self.log.info("Query: {query}".format(query=query))
        return query

    def process_response(self, data, cursor_pos, cur_date):
        """ """
        json_data = json.dumps(data)
        with open(self.tmp_file_path, 'w') as fh:
            fh.write(json_data)
        s3_save_path = self.dest_key.format(today=cur_date,
                                            cursor_pos=cursor_pos)
        source_uri = '{}'.format(self.tmp_file_path)
        dest_uri = 's3://{}/{}'.format(self.dest_bucket, s3_save_path)
        self.log.info(
            "Gigya request successful, starting upload to S3 {}".format(
                dest_uri))
        try:
            subprocess.run(
                'aws s3 cp {source_uri} {dest_uri} --acl bucket-owner-full-control'
                .format(source_uri=source_uri, dest_uri=dest_uri),
                shell=True,
                check=True)
        except subprocess.CalledProcessError:
            self.log.error("Upload to S3 failed.")
            return False

        self.log.info("Upload to S3 successful.")
        return True

    def execute(self, context):
        today = context["execution_date"]
        yesterday = context["execution_date"] + timedelta(-1)
        # Generate unique tmp path
        self.tmp_file_path = self.tmp_file_path + ''.join(
            random.choices(string.ascii_uppercase + string.digits, k=10))
        query = self.build_query(yesterday, today)
        gigya_hook = GigyaHook(gigya_conn_id=self.gigya_conn_id)
        cursor_pos = 0

        self.log.info("Calling Gigya search")
        response = gigya_hook.search(query)

        if not response:
            self.log.info("Gigya hook returned an empty response.")

        while response:
            if 'results' in response and len(response['results']) > 0:
                results = self.process_response(response['results'],
                                                cursor_pos,
                                                today.strftime('%Y-%m-%d'))
                self.log.info("Gigya search #{} results: {}".format(
                    cursor_pos, results))
                if 'cursorNextId' in response:
                    cursor_pos = response['cursorNextId']
                    response = gigya_hook.search_by_cursor(cursor_pos)
                    continue
            else:
                self.process_response('', cursor_pos + 1,
                                      today.strftime('%Y-%m-%d'))
                self.log.info("Gigya search returned no further entries.")
            break


class MailChimpToS3Operator(BaseOperator):
    """
    Calls a MailChimp endpoint, downloads the data and saves it to S3.
    """

    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 mc_dict,
                 dest_bucket,
                 dest_key,
                 limit=1000,
                 *args,
                 **kwargs):
        super(MailChimpToS3Operator, self).__init__(*args, **kwargs)
        self.dest_bucket = dest_bucket
        self.dest_key = dest_key
        self.mc_dict = mc_dict
        self.timeout = 300
        self.data = dict()
        self.data['lists'] = []
        self.data['members'] = []
        self.tmp_file_path = '/tmp/mailchimp_s3/'

    def execute(self, context):
        today = context["execution_date"]
        yesterday = context["execution_date"] + timedelta(-1)
        # Generate unique tmp path
        self.tmp_file_path = self.tmp_file_path + ''.join(
            random.choices(string.ascii_uppercase + string.digits, k=10))
        self.dest_key = today.strftime(self.dest_key)
        arg_dict = {
            'count': 1000,
            'since_last_changed': yesterday.strftime('%Y-%m-%d 00:00:00'),
            'before_last_changed': today.strftime('%Y-%m-%d 00:00:00'),
        }
        for mc_name, members in self.mc_dict.items():
            mc_client = MailChimpHook(mc_name=mc_name, timeout=self.timeout)
            self.data['lists'].extend(mc_client.get_all_lists())
            self.data['members'].extend(
                mc_client.get_members_by_save_list_id(members, arg_dict))

        for key, json_values in self.data.items():
            with open(self.tmp_file_path, 'w') as fh:
                for json_value in json_values:
                    values = json.dumps(json_value)
                    fh.write(values + '\n')
            s3_save_path = self.dest_key.format(key)
            source_uri = '{}'.format(temp_path)
            dest_uri = 's3://{}/{}'.format(self.dest_bucket, s3_save_path)
            self.log.info(
                "Mailchimp request successful, starting upload to S3 {}".
                format(dest_uri))
            try:
                subprocess.run(
                    'aws s3 cp {source_uri} {dest_uri} --acl bucket-owner-full-control'
                    .format(source_uri=source_uri, dest_uri=dest_uri),
                    shell=True,
                    check=True)
            except subprocess.CalledProcessError:
                self.log.error("Upload to S3 failed.")
                continue

            self.log.info("Upload to S3 successful.")


class FacebookToS3Operator(BaseOperator):
    """
    Operator for calling an endpoint on an Facebook system to execute an action and saves the response to S3.
    """

    ui_color = '#f4a460'
    FB_API_URL = 'v2.12/{}/adnetworkanalytics/'
    METRICS = [
        'fb_ad_network_revenue', 'fb_ad_network_imp', 'fb_ad_network_request',
        'fb_ad_network_cpm', 'fb_ad_network_filled_request',
        'fb_ad_network_bidding_request', 'fb_ad_network_bidding_response',
        'fb_ad_network_video_mrc', 'fb_ad_network_video_view',
        'fb_ad_network_click', 'fb_ad_network_request'
    ]
    BREAKDOWNS = ['app', 'placement', 'platform', 'property', 'deal']
    AGGREGATION_PERIOD = 'day'

    @apply_defaults
    def __init__(self,
                 product_ids,
                 dest_bucket,
                 dest_key,
                 data=None,
                 response_check=None,
                 extra_options=None,
                 http_conn_id=None,
                 *args,
                 **kwargs):
        super(FacebookToS3Operator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.product_ids = product_ids
        self.headers = {'Content-Type': 'application/json'}
        self.data = data or {}
        self.response_check = response_check
        self.extra_options = extra_options or {}
        self.tmp_file_path = '/tmp/http_s3/'
        self.dest_bucket = dest_bucket
        self.dest_key = dest_key

    def query_json(self, endpoint, data, method='POST'):
        """ Query using provided http hook, check for errors and return as JSON. """
        http = HttpsHook(method, http_conn_id=self.http_conn_id)
        response = http.run(endpoint, data, self.headers, self.extra_options)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
        else:
            if response.status_code != 200:
                raise AirflowException("Response returned %d" %
                                       response.status_code)
        json_response = json.loads(response.text)
        return json_response

    def execute(self, context):
        today = context["execution_date"]
        yesterday = context["execution_date"] + timedelta(-1)
        # Generate unique tmp path
        self.tmp_file_path = self.tmp_file_path + ''.join(
            random.choices(string.ascii_uppercase + string.digits, k=10))
        self.data['until'] = today.strftime(self.data.get('until', '%Y-%m-%d'))
        self.data['since'] = yesterday.strftime(
            self.data.get('since', '%Y-%m-%d'))
        self.log.info("Calling HTTPS method")
        query_responses = []

        for idx, product_id in enumerate(self.product_ids):
            product_id = product_id.format(today=today, yesterday=yesterday)
            endpoint = self.FB_API_URL.format(product_id)
            post_data = self.data
            post_data['metrics'] = self.METRICS
            post_data['breakdowns'] = self.BREAKDOWNS
            post_data['aggregation_period'] = self.AGGREGATION_PERIOD
            json_response = self.query_json(endpoint, json.dumps(post_data))
            query_responses.append(json_response)
            self.log.info("Search Response: {}".format(json_response))

        for idx, resp in enumerate(query_responses):
            result_link = resp['async_result_link'].split('/', 3)[3]
            get_data = self.data
            json_response = self.query_json(result_link, get_data, 'GET')
            self.log.info("Query Response: {}".format(json_response))
            while json_response['data'][0]['status'] != 'complete':
                time.sleep(5)
                json_response = self.query_json(result_link, get_data, 'GET')
            with open(self.tmp_file_path, 'w+') as fh:
                fh.write(json.dumps(json_response))

            curr_dest_key = today.strftime(self.dest_key.format(num=str(idx)))
            source_uri = '{}'.format(self.tmp_file_path)
            dest_uri = 's3://{}/{}'.format(self.dest_bucket, curr_dest_key)
            self.log.info(
                "HTTPS request successful, starting upload to S3 {}".format(
                    dest_uri))
            try:
                subprocess.run(
                    'aws s3 cp {source_uri} {dest_uri} --acl bucket-owner-full-control'
                    .format(source_uri=source_uri, dest_uri=dest_uri),
                    shell=True,
                    check=True)
            except subprocess.CalledProcessError:
                raise AirflowException("Upload to S3 failed.")
            self.log.info("Upload successful")
            try:
                subprocess.run('rm {source_uri}'.format(source_uri=source_uri),
                               shell=True,
                               check=True)
            except subprocess.CalledProcessError:
                raise AirflowException("Removal of temp file failed.")


class TrendsTransformOperator(BaseOperator):
    """
    Transform google trends data
    """

    ui_color = '#f4a460'

    TABLE_TYPE_MAPPING = dict(
        zip([
            "raw_plutotv_earnings_report", "raw_tubitv_adrise_content_report",
            "raw_amazon_earnings_report", "raw_aetn_watch_programs",
            "raw_amazon_minute_streamed_performance_report"
        ], ['series_name', 'series', 'series_name', 'title', 'series_name']))

    @apply_defaults
    def __init__(self, frequency, trend_type, input_path, output_path, *args,
                 **kwargs):
        super(TrendsTransformOperator, self).__init__(*args, **kwargs)
        self.frequency = frequency
        self.trend_type = trend_type
        self.input_path = input_path
        self.output_path = output_path
        self.sleep_time = 15

    def execute(self, context):
        today = context["execution_date"]
        self.input_path = today.strftime(self.input_path)
        self.output_path = today.strftime(self.output_path)
        for source_name, source_column in self.TABLE_TYPE_MAPPING.items():
            input_file = self.input_path.format(source_name)
            try:
                series_df = pd.read_csv(input_file,
                                        engine='python',
                                        usecols=[source_column],
                                        escapechar='\\',
                                        quotechar='"',
                                        error_bad_lines=False)
                series_df.fillna(0, inplace=True)
            except Exception as e:
                self.log.error(e)
                continue
            pytrend = TrendReq(retries=10,
                               backoff_factor=0.2,
                               timeout=(10, 60))
            series_list = list(set(series_df[source_column].values.tolist()))
            self.log.info("Running Trends on the following series: {}".format(
                series_list))

            for series in series_list:
                if not isinstance(series, str):
                    continue
                data = None
                for series_name in [
                        series, "{} {}".format(series, source_column)
                ]:
                    try:
                        self.log.info(
                            "Fetching data for {}".format(series_name))
                        if self.trend_type == 'historical':
                            pytrend.build_payload([series_name],
                                                  cat=0,
                                                  timeframe='today 5-y',
                                                  geo='US',
                                                  gprop='')
                            data = pytrend.interest_over_time()
                        else:
                            pytrend.build_payload(
                                [series_name],
                                cat=0,
                                timeframe='today {}-m'.format(
                                    int(self.frequency)),
                                geo='US',
                                gprop='')
                            data = pytrend.interest_by_region(
                                resolution='STATE')
                        if len(data) > 0:
                            break
                    except Exception as e:
                        self.log.info(e)
                        data = None
                        break
                if data is not None and len(list(data)) > 0:
                    self.log.info('Google Trends returned valid data')
                    data['asset_name'] = series
                    output_file = self.output_path.format(source_name)
                    data.to_csv(output_file, mode='a', header=False)
                # So Google doesnt complain of overflows
                time.sleep(self.sleep_time)
        self.log.info("Done.")


class PythonScriptOperator(BaseOperator):
    """
    Run python script
    """
    @apply_defaults
    def __init__(self, script_name, *args, **kwargs):
        super(PythonScriptOperator, self).__init__(*args, **kwargs)
        self.script_name = script_name

    def execute(self, context):
        script_path = os.path.join(os.environ.get('AIRFLOW_HOME', ''),
                                   'dags/python_scripts/' + self.script_name)
        self.log.info('Executing: ' + script_path)
        runpy.run_path(script_path)
        self.log.info('Script executed successfully')
