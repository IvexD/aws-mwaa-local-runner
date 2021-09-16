import json
import requests
import os

from ssl import CERT_NONE
from pymongo import MongoClient
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from aetn.hooks.GSSDK import GSRequest
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.S3_hook import S3Hook
from airflow.exceptions import AirflowException

from mailchimp3 import MailChimp
from mailchimp3.mailchimpclient import MailChimpError


class HttpsHook(HttpHook):
    """
    Calls a HTTPS endpoint.
    """
    def get_conn(self, headers=None):
        """
        Returns http session for use with requests. Supports https.
        """
        conn = self.get_connection(self.http_conn_id)
        session = requests.Session()

        if "://" in conn.host:
            self.base_url = conn.host
        elif conn.schema:
            self.base_url = conn.schema + "://" + conn.host
        elif conn.conn_type:  # https support
            self.base_url = conn.conn_type + "://" + conn.host
        else:
            # schema defaults to HTTP
            self.base_url = "http://" + conn.host

        if conn.port:
            self.base_url = self.base_url + ":" + str(conn.port) + "/"
        if conn.login:
            session.auth = (conn.login, conn.password)
        if conn.extra:
            try:
                session.headers.update(conn.extra_dejson)
            except TypeError:
                self.log.warn('Connection to {} has invalid extra field.'.format(
                    conn.host))
        if headers:
            session.headers.update(headers)

        return session


class MailChimpHook(BaseHook):
    """
    Calls an endpoint on a MailChimp database to execute an action and returns the response.
    """

    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 mc_name,
                 timeout,
                 *args, **kwargs):
        super(MailChimpHook, self).__init__(source='mailchimp')
        self.mc_client = MailChimp(mc_name, timeout)

    def get_all_lists(self):
        try:
            response = self.mc_client.lists.all(get_all=True)
        except MailChimpError as err:
            self.log.error("ERROR: MailChimpError: {}".format(err))
            return None
        return response['lists']

    def get_members_by_save_list_id(self, list_id, args):
        offset = 0
        total = 1
        responses = []
        while offset < total:
            try:
                response = self.mc_client.lists.members.all('{}'.format(list_id), get_all=True, **args)
            except MailChimpError as err:
                self.log.info("ERROR: MailChimpError: {}".format(err))
                return
            offset += len(response['members'])
            args['offset'] = offset
            total = response['total_items']
            self.log.info("Total {}".format(total))
            self.log.info("Count {}".format(offset))
            responses.extend(response['members'])
        return responses


class GigyaHook(BaseHook):
    """
    Calls an endpoint on a Gigya database to execute an action and returns the response.
    """

    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 gigya_conn_id,
                 limit=1000,
                 *args, **kwargs):
        super(GigyaHook, self).__init__(source='gigya')
        self.limit = limit
        conn = self.get_connection(gigya_conn_id)
        extra_options = conn.extra_dejson if conn.extra else {}
        self.access_key = extra_options.get('access_key', None)
        self.secret_key = extra_options.get('secret_key', None)
        self.cert = '{}/{}'.format(os.getcwd(), extra_options.get('cert', None))

    def request(self, method, params):
        """ """
        request = GSRequest(self.access_key, self.secret_key, method, params, useHTTPS=True)
        request.setCACertsPath(self.cert)
        response = request.send()
        if response.getErrorCode() != 0:
            raise AirflowException("Gigya request error code {}, msg: {}".format(response.getErrorCode(), response.getErrorMessage()))
        return json.loads(response.getResponseText())

    def search(self, query):
        response = self.request('accounts.search', {'query': query, 'openCursor': True})
        return response

    def search_by_cursor(self, cursor_id):
        response = self.request('accounts.search', {'cursorId': cursor_id, 'timeout': 60000})
        return response


class MongoHook(BaseHook):
    """
    PyMongo Wrapper to Interact With Mongo Database
    Mongo Connection Documentation
    https://docs.mongodb.com/manual/reference/connection-string/index.html
    You can specify connection string options in extra field of your connection
    https://docs.mongodb.com/manual/reference/connection-string/index.html#connection-string-options
    ex.
        {replicaSet: test, ssl: True, connectTimeoutMS: 30000}
    """
    conn_type = 'MongoDb'

    def __init__(self, conn_id, *args, **kwargs):
        super().__init__(source='mongo')
        self.mongo_conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson

    def get_conn(self):
        """
        Fetches PyMongo Client
        """
        conn = self.connection
        uri = 'mongodb://{creds}{host}{port}/{database}'.format(
            creds='{}:{}@'.format(conn.login, conn.password) if conn.login is not None else '',
            host=conn.host,
            port='' if conn.port is None else ':{}'.format(conn.port),
            database='' if conn.schema is None else conn.schema
        )
        # Mongo Connection Options dict that is unpacked when passed to MongoClient
        options = self.extras
        # If we are using SSL disable requiring certs from specific hostname
        if options.get('ssl', False):
            options.update({'ssl_cert_reqs': CERT_NONE})
        return MongoClient(uri, **options)

    def get_collection(self, mongo_collection, mongo_db=None):
        """
        Fetches a mongo collection object for querying.
        Uses connection schema as DB unless specified
        """
        mongo_db = mongo_db if mongo_db is not None else self.connection.schema
        mongo_conn = self.get_conn()
        return mongo_conn.get_database(mongo_db).get_collection(mongo_collection)

    def aggregate(self, mongo_collection, aggregate_query, mongo_db=None, **kwargs):
        """
        Runs and aggregation pipeline and returns the results
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.aggregate
        http://api.mongodb.com/python/current/examples/aggregation.html
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)
        return collection.aggregate(aggregate_query, **kwargs)

    def find(self, mongo_collection, query, find_one=False, mongo_db=None, **kwargs):
        """
        Runs a mongo find query and returns the results
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)
        if find_one:
            return collection.find_one(query, **kwargs)
        else:
            return collection.find(query, **kwargs)

    def insert_one(self, mongo_collection, doc, mongo_db=None, **kwargs):
        """
        Inserts a single document into a mongo collection
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.insert_one
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)
        return collection.insert_one(doc, **kwargs)

    def drop_collection(self, mongo_collection, mongo_db=None, **kwargs):
        """
        Drops the entire mongo collection.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)
        return collection.drop(**kwargs)

    def insert_many(self, mongo_collection, docs, mongo_db=None, **kwargs):
        """
        Inserts many docs into a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.insert_many
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)
        return collection.insert_many(docs, **kwargs)

    def replace_one(self, mongo_collection, replacement_filter, doc, mongo_db=None, **kwargs):
        """
        Replaces a single document that matches a filter in a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.replace_one
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)
        return collection.replace_one(replacement_filter, doc, **kwargs)

    def update_one(self, mongo_collection, update_filter, update, mongo_db=None, **kwargs):
        """
        Updates a single document that matches a filter in a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.update_one
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)
        return collection.update_one(update_filter, update, **kwargs)

    def update_many(self, mongo_collection, update_filter, update, mongo_db=None, **kwargs):
        """
        Updates many docs that matches a filter in a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.update_many
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)
        return collection.update_many(update_filter, update, **kwargs)

    def bulk_write(self, mongo_collection, requests, mongo_db=None, **kwargs):
        """
        Submits a bulk write job mongo based on the specified requests.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.bulk_write
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.bulk_write(requests, **kwargs)


class S3StreamHook(S3Hook):
    def create_multipart_upload(self, **kwargs):
        return self.get_conn().create_multipart_upload(**kwargs)

    def complete_multipart_upload(self, **kwargs):
        return self.get_conn().complete_multipart_upload(**kwargs)

    def upload_part(self, **kwargs):
        return self.get_conn().upload_part(**kwargs)

    def abort_multipart_upload(self, **kwargs):
        return self.get_conn().abort_multipart_upload(**kwargs)
