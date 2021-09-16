import boto3
import logging

from functools import partial
from datetime import timedelta

logger = logging.getLogger(__name__)


SNS_ERROR_SUBJECT = '{jobEnv} {jobName} {jobType} Dag ID: {dagID} ERROR {runDate}'
SNS_ERROR_MESSAGE = '{jobEnv} {jobName} Task ID: {taskID} has failed at {runDate}.'
SNS_SUCCESS_SUBJECT = '{jobEnv} {jobName} {jobType} Dag ID: {dagID} SUCCESS {runDate}'
SNS_SUCCES_MESSAGE = '{jobEnv} {jobName} Task ID: {taskID} has successfully run at {runDate}.'


def construct_sns_callback(topic_arn, default_topic_arn, job_name, job_type, date_lag, succeeded, run_env, context, **kwargs):
    """ Create a partial function for SNS Alarms. """
    execution_date = context["execution_date"]  + timedelta(date_lag)
    dag_id = context["task_instance"].dag_id
    task_id = context["task_instance"].task_id
    run_date = execution_date.strftime("%Y-%m-%d")
    if succeeded:
        subject = SNS_SUCCESS_SUBJECT.format(jobName=job_name, jobType=job_type, runDate=run_date, dagID=dag_id, jobEnv=run_env)
        message = SNS_SUCCES_MESSAGE.format(jobName=job_name, runDate=run_date, taskID=task_id, jobEnv=run_env)
    else:
        subject = SNS_ERROR_SUBJECT.format(jobName=job_name, jobType=job_type, runDate=run_date, dagID=dag_id, jobEnv=run_env)
        message = SNS_ERROR_MESSAGE.format(jobName=job_name, runDate=run_date, taskID=task_id, jobEnv=run_env)
    return sns_alarm_with_default(topic_arn, default_topic_arn, subject, message)


def sns_alarm_with_default(topic, default_topic, subject, message):
    """ Send a SNS message to both the default and desired topic. """
    sns_alarm(default_topic, subject, message)
    if topic:
        sns_alarm(topic, subject, message)


def sns_alarm(topic, subject, message):
    """ Send a custom SNS message. """
    logger.debug("Sending SNS Alarm to %s", topic)
    client = boto3.client(
        "sns",
        region_name="us-east-1"
    )
    return client.publish(
      TopicArn=topic,
      Subject=subject,
      Message=message
    )
