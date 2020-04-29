import logging
import textwrap

import boto3

from taro import ExecutionState, ExecutionStateObserver, JobInfo

log = logging.getLogger(__name__)

client = boto3.client("sns")


def notify(topics, subject, message):
    for topic in topics:
        client.publish(TopicArn=topic, Subject=subject, Message=message)
        log.debug("event=[sns_notified] topic=[{}] subject=[{}]".format(topic, subject))


def _generate(*sections):
    return "\n".join((textwrap.dedent(section) for section in sections))


def _header(header_text):
    return header_text + "\n" + "-" * len(header_text)


def _create_job_section(job):
    return """\
    {header}
      Job: {job.job_id}
      Instance: {job.instance_id}
      Executed: {executed}
      State: {job.state.name}
      Changed: {last_changed}
    """.format(header=_header("Job Detail"),
               job=job,
               executed=job.lifecycle.execution_started() or 'N/A',
               last_changed=job.lifecycle.last_changed())


def _create_error_section(exec_error):
    if exec_error.params:
        params = "\n    ".join("{}: {}".format(k, v) for k, v in exec_error.params.items())
    else:
        params = "none"
    return """\
    {header}
      Reason: {error.message}
      Params: {parameters}
    """.format(header=_header("Error Detail"), error=exec_error, parameters=params)


class SnsNotification(ExecutionStateObserver):

    def __init__(self, topics_provider):
        self.topics_provider = topics_provider

    def state_update(self, job: JobInfo):
        topics = self.topics_provider(job)
        if not topics:
            return

        states = job.lifecycle.states()
        prev_state = states[-2] if len(states) > 1 else ExecutionState.NONE
        cur_state = states[-1]

        subject = "Job {} changed state from {} to {}".format(job.job_id, prev_state.name, cur_state.name)
        job_section = _create_job_section(job)

        if cur_state.is_failure():
            exec_error = job.exec_error
            message = _generate(job_section, _create_error_section(exec_error))
            notify(topics, subject, message)
        else:
            notify(topics, subject, job_section)
