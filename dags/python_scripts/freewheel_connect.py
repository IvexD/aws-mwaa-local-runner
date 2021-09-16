import boto3
import os
from airflow.exceptions import AirflowException

sts_client = boto3.client('sts')
assumed_role_object = sts_client.assume_role(
    RoleArn="arn:aws:iam::265086463190:role/171213-V4LogAccess-role",
    RoleSessionName="AssumeRoleSession")
assumed_role_credentials = assumed_role_object['Credentials']
fw_v4_s3_client = boto3.client(
    's3',
    aws_access_key_id=assumed_role_credentials['AccessKeyId'],
    aws_secret_access_key=assumed_role_credentials['SecretAccessKey'],
    aws_session_token=assumed_role_credentials['SessionToken'],
)

conn_id = 's3_freewheel'
cmd = 'airflow connections --list | grep {} | wc -l'.format(conn_id)
conn_exists = int(os.popen(cmd).read().splitlines()[0])

if conn_exists:
    cmd = 'airflow connections --delete --conn_id {}'.format(conn_id)
    status = os.system(cmd)
    if status != 0:
        raise AirflowException(
            'Failed to delete connection {conn_id}'
            ' - command returned status {status}'
            .format(conn_id=conn_id, status=status))

access_key_id = assumed_role_credentials['AccessKeyId']
secret_access_key = assumed_role_credentials['SecretAccessKey']
session_token = assumed_role_credentials['SessionToken']
conn_extra = (
    '{{ "aws_access_key_id": "{id}", "aws_secret_access_key": "{key}",'
    ' "aws_session_token": "{token}" }}'
    .format(id=access_key_id, key=secret_access_key, token=session_token)
)
cmd = (
    'airflow connections --add --conn_id {conn_id} --conn_type S3'
    ' --conn_login airflow_admin --conn_extra \'{conn_extra}\''
    .format(conn_id=conn_id, conn_extra=conn_extra)
)
status = os.system(cmd)

if status != 0:
    raise AirflowException(
        'Failed to add connection {conn_id}'
        ' - command returned status {status}'
        .format(conn_id=conn_id, status=status))

