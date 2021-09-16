import os
import subprocess

from datetime import timedelta
from math import ceil
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from aetn.hooks.dwh_custom_hooks import S3StreamHook


class SFTPToS3Operator(BaseOperator):
    """
    Operator for transferring files from remote SFTP host to local and then to S3.
    """
    TRANSFER_BUFFER_SIZE = 5242880  # 5MB

    @apply_defaults
    def __init__(self,
                 ssh_conn_id=None,
                 s3_conn_id=None,
                 remote_filepath=None,
                 local_filepath=None,
                 dest_key=None,
                 dest_bucket=None,
                 transform_script=None,
                 replace=True,
                 date_lag=0,
                 *args,
                 **kwargs):
        super(SFTPToS3Operator, self).__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.s3_conn_id = s3_conn_id
        self.remote_filepath = remote_filepath
        self.local_filepath = local_filepath
        self.dest_key = dest_key
        self.dest_bucket = dest_bucket
        self.transform_script = transform_script
        self.ssh_hook = None
        self.s3_hook = None
        self.replace = replace
        self.date_lag = date_lag

    def execute(self, context):
        today = context["execution_date"] + timedelta(days=self.date_lag)
        self.remote_filepath = today.strftime(self.remote_filepath)
        self.dest_key = today.strftime(self.dest_key)
        file_msg = None
        try:
            if self.ssh_conn_id:
                self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
            self.s3_hook = S3StreamHook(aws_conn_id=self.s3_conn_id)

            if not self.ssh_hook or not self.s3_hook:
                raise AirflowException("Cannot operate without ssh_conn_id or s3_conn_id.")
            file_msg = "from {0} to {1}/{2}".format(self.remote_filepath, self.dest_bucket, self.dest_key)

            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                if not self.transform_script:
                    file_size = sftp_client.stat(self.remote_filepath).st_size
                    with sftp_client.open(self.remote_filepath, 'rb') as file:
                        self.log.info("Starting streaming transfer to S3 %s", file_msg)
                        create_mpu_response = self.s3_hook.create_multipart_upload(
                            Bucket=self.dest_bucket,
                            Key=self.dest_key
                        )
                        upload_id = create_mpu_response['UploadId']
                        self.log.info("Multipart upload id: %s" % upload_id)
                        file_parts = {'Parts': []}
                        complete_mpu_response = {}
                        # calculate how many parts we'll have
                        num_parts = ceil(file_size / self.TRANSFER_BUFFER_SIZE)
                        try:
                            for index in range(0, num_parts):
                                part_num = index + 1
                                self.log.info("Uploading part {} of {}".format(part_num, num_parts))
                                file_piece = file.read(self.TRANSFER_BUFFER_SIZE)
                                upload_part_response = self.s3_hook.upload_part(
                                    Bucket=self.dest_bucket,
                                    Key=self.dest_key,
                                    UploadId=upload_id,
                                    PartNumber=part_num,
                                    Body=file_piece
                                )
                                file_parts['Parts'].append({
                                    'ETag': upload_part_response['ETag'],
                                    'PartNumber': part_num,
                                })
                            complete_mpu_response = self.s3_hook.complete_multipart_upload(
                                Bucket=self.dest_bucket,
                                Key=self.dest_key,
                                UploadId=upload_id,
                                MultipartUpload=file_parts
                            )
                        finally:
                            if 'ETag' not in complete_mpu_response:
                                self.s3_hook.abort_multipart_upload(
                                    Bucket=self.dest_bucket, Key=self.dest_key, UploadId=upload_id)
                                raise AirflowException(
                                    "Error while transferring {0}, multipart upload error".format(file_msg))
                        self.log.info("Transfer to S3 completed")
                    sftp_client.close()
                else:
                    self.local_filepath = today.strftime(self.local_filepath)
                    local_folder = os.path.dirname(self.local_filepath)
                    # Delete old items in the folder
                    try:
                        subprocess.run('rm -rf {local_folder}'.format(local_folder=local_folder), shell=True, check=True)
                    except subprocess.CalledProcessError:
                        raise AirflowException("Removal of temp file failed.")
                    try:
                        os.makedirs(local_folder)
                    except OSError:
                        if not os.path.isdir(local_folder):
                            raise
                    file_msg = "from {0} to {1}".format(self.remote_filepath, self.local_filepath)
                    self.log.info("Starting transfer %s", file_msg)
                    with sftp_client.open(self.remote_filepath, 'rb') as sftp_file:
                        with open(self.local_filepath, 'wb+') as local_file:
                            while local_file.write(sftp_file.read(self.TRANSFER_BUFFER_SIZE)):
                                pass
                    sftp_client.close()
                    self.log.info("Running transform script %s", self.transform_script)
                    try:
                        proc_out = subprocess.run([self.transform_script], shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.decode('utf-8')
                    except subprocess.CalledProcessError as err:
                        self.log.error(err.output)
                        raise AirflowException("Transform script failed.")
                    self.log.info(proc_out)
                    file_msg = "from {0} to {1}/{2}".format(self.local_filepath, self.dest_bucket, self.dest_key)
                    self.log.info("Starting transfer %s", file_msg)
                    source_uri = '{}'.format(self.local_filepath)
                    dest_uri = 's3://{}/{}'.format(self.dest_bucket, self.dest_key)
                    try:
                        if not self.replace:
                            ret = subprocess.run('aws s3 ls {dest_uri} | wc -l'.format(dest_uri=dest_uri), shell=True, check=True)
                            if len(ret.stdout) > 0:
                                self.log.info("File already exists, skipping")
                                return True
                        subprocess.run('aws s3 cp {source_uri} {dest_uri} --acl bucket-owner-full-control'.format(
                            source_uri=source_uri, dest_uri=dest_uri), shell=True, check=True)
                    except subprocess.CalledProcessError:
                        raise AirflowException("Upload to S3 failed.")
                    self.log.info("Transfer to S3 completed")
        except Exception as e:
            raise AirflowException("Error while transferring {0}, error: {1}".format(file_msg, str(e)))
        return True


class S3ToSFTPOperator(BaseOperator):
    """
    Operator for transferring files from S3 to local and then to remote SFTP host.
    """
    TRANSFER_BUFFER_SIZE = 5242880  # 5MB

    @apply_defaults
    def __init__(self,
                 ssh_conn_id=None,
                 s3_conn_id=None,
                 remote_filepath=None,
                 local_filepath=None,
                 source_key=None,
                 source_bucket=None,
                 transform_script=None,
                 date_lag=0,
                 *args,
                 **kwargs):
        super(S3ToSFTPOperator, self).__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.s3_conn_id = s3_conn_id
        self.remote_filepath = remote_filepath
        self.local_filepath = local_filepath
        self.source_key = source_key
        self.source_bucket = source_bucket
        self.transform_script = transform_script
        self.ssh_hook = None
        self.s3_hook = None
        self.date_lag = date_lag

    def execute(self, context):
        today = context["execution_date"] + timedelta(days=self.date_lag)
        self.remote_filepath = today.strftime(self.remote_filepath)
        self.source_key = today.strftime(self.source_key)
        file_msg = None
        try:
            if self.ssh_conn_id:
                self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
            self.s3_hook = S3StreamHook(aws_conn_id=self.s3_conn_id)
            if not self.ssh_hook or not self.s3_hook:
                raise AirflowException("Cannot operate without ssh_conn_id or s3_conn_id.")

            if not self.transform_script:
                with self.ssh_hook.get_conn() as ssh_client:
                    s3_obj = self.s3_hook.get_key(bucket_name=self.source_bucket, key=self.source_key)
                    sftp_client = ssh_client.open_sftp()
                    file_msg = "from {0}/{1} to {2}".format(self.source_bucket, self.source_key, self.remote_filepath)
                    self.log.info("Starting streaming transfer to SFTP %s", file_msg)
                    body = s3_obj.get()['Body']
                    with sftp_client.open(self.remote_filepath, 'w+') as file:
                        while file.write(body.read(amt=self.TRANSFER_BUFFER_SIZE)):
                            pass
                    sftp_client.close()
                    self.log.info("Transfer to SFTP completed")
            else:
                self.local_filepath = today.strftime(self.local_filepath)
                local_folder = os.path.dirname(self.local_filepath)
                # Delete old items in the folder
                try:
                    subprocess.run('rm -rf {local_folder}'.format(local_folder=local_folder), shell=True, check=True)
                except subprocess.CalledProcessError:
                    raise AirflowException("Removal of temp file failed.")
                try:
                    os.makedirs(local_folder)
                except OSError:
                    if not os.path.isdir(local_folder):
                        raise
                source_uri = 's3://{}/{}'.format(self.source_bucket, self.source_key)
                file_msg = "from {0} to {1}".format(source_uri, self.local_filepath)
                self.log.info("Starting transfer to %s", file_msg)
                try:
                    cmd_name = 'aws s3 cp {source_uri} {dest_uri} --acl bucket-owner-full-control'.format(
                        source_uri=source_uri, dest_uri=self.local_filepath)
                    subprocess.run(cmd_name, shell=True, check=True)
                except subprocess.CalledProcessError:
                    raise AirflowException("Copy from S3 failed.")
                self.log.info("Running transform script %s", self.transform_script)
                try:
                    proc_out = subprocess.run([self.transform_script], shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.decode('utf-8')
                except subprocess.CalledProcessError as err:
                    self.log.error(err.output)
                    raise AirflowException("Transform script failed.")
                self.log.info(proc_out)
                with self.ssh_hook.get_conn() as ssh_client:
                    sftp_client = ssh_client.open_sftp()
                    file_msg = "from {0} to {1}".format(self.local_filepath, self.remote_filepath)
                    self.log.info("Starting transfer to SFTP %s", file_msg)
                    sftp_client.put(localpath=self.local_filepath, remotepath=self.remote_filepath)
                    sftp_client.close()
                    self.log.info("Transfer to SFTP completed")
        except Exception as e:
            raise AirflowException("Error while transferring {0}, error: {1}".format(file_msg, str(e)))
        return True
