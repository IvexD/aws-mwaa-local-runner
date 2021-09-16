import json
import subprocess
import string
import random
import os

from datetime import datetime
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from bson import json_util
from aetn.hooks.dwh_custom_hooks import MongoHook
from pymongo import ReplaceOne
from datetime import timedelta
from airflow.exceptions import AirflowException


class MongoToS3Operator(BaseOperator):
    """
    Mongo -> S3
    :param mongo_conn_id:           The source mongo connection id.
    :type mongo_conn_id:            string
    :param mongo_collection:        The source mongo collection in a list form.
    :type mongo_collection:         string
    :param mongo_database:          The source mongo database.
    :type mongo_database:           string
    :param mongo_query:             The specified mongo query.
    :type mongo_query:              string
    :param s3_conn_id:              The destination s3 connnection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The destination s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The destination s3 key.
    :type s3_key:                   string
    """

    # TODO This currently sets job = queued and locks job
    template_fields = ['s3_key', 'mongo_query']

    def __init__(self,
                 mongo_conn_id,
                 mongo_collection,
                 mongo_database,
                 mongo_query,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 mode='insert',
                 *args, **kwargs):
        super(MongoToS3Operator, self).__init__(*args, **kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.s3_conn_id = s3_conn_id
        self.mongo_db = mongo_database
        self.mongo_collection = mongo_collection
        # Grab query and determine if we need to run an aggregate pipeline
        self.mongo_query = mongo_query
        self.is_pipeline = True if isinstance(self.mongo_query, list) else False
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.mode = mode
        self.tmp_file_path = '/tmp/mongo_s3/'

    def execute(self, context):
        """
        Executed by task_instance at runtime
        """
        today = context["execution_date"]
        yesterday = context["execution_date"] - timedelta(1)
        # Generate unique tmp path
        try:
            os.mkdir(self.tmp_file_path)
        except FileExistsError:
            pass
        self.tmp_file_path = self.tmp_file_path + ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        if self.mongo_query and 'updated_at' in self.mongo_query:
            self.mongo_query['updated_at'] = {key: today.timestamp() * 1000 if value == '{today}'
                                              else yesterday.timestamp() * 1000 if value == '{yesterday}' else value
                                              for key, value in self.mongo_query['updated_at'].items()}
        self.s3_key = today.strftime(self.s3_key)
        mongo_conn = MongoHook(self.mongo_conn_id)
        dest_uri = 's3://{}/{}'.format(self.s3_bucket, self.s3_key)

        # Grab collection and execute query according to whether or not it is a pipeline
        # Write to temp file
        with open(self.tmp_file_path, 'w+') as temp_file_handle:
            # Make sure the file exists, even if empty
            temp_file_handle.write('')
            for mongo_col in self.mongo_collection:
                self.log.info("Getting collection: {}, query: {}".format(mongo_col, self.mongo_query))
                collection = mongo_conn.get_collection(mongo_col, self.mongo_db)
                all_results = collection.aggregate(self.mongo_query) if self.is_pipeline else collection.find(self.mongo_query)
                self.log.info("Query results length: {}".format(all_results.count()))
                for query_res in all_results:
                    query_res['mongo_collection'] = mongo_col
                    temp_file_handle.write(json_util.dumps(query_res))
                    temp_file_handle.write('\n')

        self.log.info("Collections successfully downloaded to {}, uploading to S3 {}".format(self.tmp_file_path, dest_uri))
        try:
            subprocess.run('aws s3 cp {source_uri} {dest_uri} --acl bucket-owner-full-control'.format(
                source_uri=self.tmp_file_path, dest_uri=dest_uri), shell=True, check=True)
        except subprocess.CalledProcessError:
            raise AirflowException("Upload to S3 failed.")
        self.log.info("Upload finished.")
        try:
            subprocess.run('rm {source_uri}'.format(source_uri=self.tmp_file_path), shell=True, check=True)
        except subprocess.CalledProcessError:
            raise AirflowException("Removal of temp file failed.")


class S3ToMongoOperator(BaseOperator):
    """
    S3 -> Mongo
    :param s3_conn_id:              The source s3 connnection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The source s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The source s3 key.
    :type s3_key:                   string
    :param mongo_conn_id:           The destination mongo connection id.
    :type mongo_conn_id:            string
    :param mongo_collection:        The destination mongo collection.
    :type mongo_collection:         string
    :param mongo_db:                The destination mongo database.
    :type mongo_db:                 string
    :param mongo_method:            The method to push records into mongo. Possible
                                    values for this include:
                                        - insert
                                        - upsert
                                        - truncate
    :type mongo_method:             string
    :param mongo_replacement_filter: *(optional)* If choosing the replace
                                    method, this indicates the the filter to
                                    determine which records to replace. This
                                    may be set as either a string or dictionary.
                                    If set as a string, the operator will
                                    view that as a key and replace the record in
                                    Mongo where the value of the key in the
                                    existing collection matches the incoming
                                    value of the key in the incoming record.
                                    If set as a dictionary, the dictionary will
                                    be passed in normally as a filter. If using
                                    a dictionary and also attempting to filter
                                    based on the value of a certain key
                                    (as in when this is set as a string),
                                    set the key and value as the same.
                                    (e.g. {"customer_id": "customer_id"})
    :type mongo_replacement_filter: string/dictionary
    """

    template_fields = ('s3_key',
                       'mongo_collection',
                       'mongo_replacement_filter')

    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 mongo_conn_id,
                 mongo_collection,
                 mongo_method='insert',
                 mongo_db=None,
                 mongo_replacement_filter=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.mongo_conn_id = mongo_conn_id
        self.mongo_method = mongo_method
        self.mongo_collection = mongo_collection
        self.mongo_db = mongo_db
        self.mongo_replacement_filter = mongo_replacement_filter

        if self.mongo_method not in ('insert', 'upsert', 'truncate'):
            raise Exception('Please specify either "insert" or "upsert" or "truncate" for Mongo method.')

    def execute(self, context):
        s3 = S3Hook(self.s3_conn_id)
        mongo_hook = MongoHook(conn_id=self.mongo_conn_id)
        raw_data = s3.get_key(self.s3_key, bucket_name=self.s3_bucket).get_contents_as_string(encoding='utf-8')
        docs = [json.loads(doc) for doc in raw_data.split('\n')]

        self.method_mapper(mongo_hook, docs)

    def method_mapper(self, mongo_hook, docs):
        if self.mongo_method == 'insert':
            self.insert_records(mongo_hook, docs)
        elif self.mongo_method == 'upsert':
            self.replace_records(mongo_hook, docs)
        elif self.mongo_method == 'truncate':
            self.delete_records(mongo_hook)
            self.insert_records(mongo_hook, docs)

    def delete_records(self, mongo_hook):
        mongo_hook.drop_collection(self.mongo_collection, self.mongo_db)

    def insert_records(self, mongo_hook, docs):
        if len(docs) == 1:
            mongo_hook.insert_one(self.mongo_collection, docs[0], mongo_db=self.mongo_db)
        else:
            mongo_hook.insert_many(self.mongo_collection, [doc for doc in docs], mongo_db=self.mongo_db)

    def replace_records(self, mongo_hook, docs):
        operations = []
        for doc in docs:
            mongo_replacement_filter = dict()
            if isinstance(self.mongo_replacement_filter, str):
                mongo_replacement_filter = {self.mongo_replacement_filter: doc.get(self.mongo_replacement_filter, False)}
            elif isinstance(self.mongo_replacement_filter, dict):
                for k, v in self.mongo_replacement_filter.items():
                    if k == v:
                        mongo_replacement_filter[k] = doc.get(k, False)
                    else:
                        mongo_replacement_filter[k] = self.mongo_replacement_filter.get(k, False)
            operations.append(ReplaceOne(mongo_replacement_filter, doc, upsert=True))

            if len(operations) == 1000:
                self.log.info('Making Request....')
                mongo_hook.bulk_write(self.mongo_collection, operations, mongo_db=self.mongo_db, ordered=False)
                operations = []
                self.log.info('Request successfully finished....')

        if len(operations) > 0:
            self.log.info('Making Final Request....')
            mongo_hook.bulk_write(self.mongo_collection, operations, mongo_db=self.mongo_db, ordered=False)
            self.log.info('Final Request Finished.')


class MongoDeleteOperator(BaseOperator):
    """
    Mongo -> S3
    :param mongo_conn_id:           The source mongo connection id.
    :type mongo_conn_id:            string
    :param mongo_collection:        The source mongo collection in a list form.
    :type mongo_collection:         string
    :param mongo_database:          The source mongo database.
    :type mongo_database:           string
    :param mongo_filter:             The specified mongo filter.
    :type mongo_filter:              string
    """

    def __init__(self,
                 mongo_conn_id,
                 mongo_collection,
                 mongo_database,
                 mongo_filter,
                 *args, **kwargs):
        super(MongoDeleteOperator, self).__init__(*args, **kwargs)
        # Conn Ids
        self.mongo_conn_id = mongo_conn_id
        # Mongo Query Settings
        self.mongo_db = mongo_database
        self.mongo_collection = mongo_collection
        # Grab query and determine if we need to run an aggregate pipeline
        self.mongo_filter = mongo_filter

    def execute(self, context):
        """
        Executed by task_instance at runtime
        """
        mongo_conn = MongoHook(self.mongo_conn_id)
        for mongo_col in self.mongo_collection:
            collection = mongo_conn.get_collection(mongo_col, self.mongo_db)
            collection.remove(self.mongo_filter)
