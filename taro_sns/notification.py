import logging
import textwrap

import boto3

from taro import ExecutionState, ExecutionStateObserver, JobInfo

log = logging.getLogger(__name__)

client = boto3.client("sns")


def notify(topic, subject, message):
    if not topic:
        return

    client.publish(TopicArn=topic, Subject=subject, Message=message)
    log.debug("event=[sns_notified] topic=[{}] subject=[{}]".format(topic, subject))


def _generate(*sections):
    return "\n".join((textwrap.dedent(section) for section in sections))


def _header(header_text):
    return (" " + header_text + " ").center(60, "*")


def _create_job_section(job):
    return """\
    {header}
    Job: {job.job_id}
    Instance: {job.instance_id}
    """.format(header=_header("Job Detail"), job=job)


def _create_error_section(exec_error):
    return """\
    {header}
    Reason: {error.message}
    Occurrence Time: TODO From job context?
    """.format(header=_header("Error Detail"), error=exec_error)


def _create_error_parameters(exec_error):
    header = _header("Error Parameters")
    if exec_error.params:
        return header + "\n" + "\n".join("{}: {}".format(k, v) for k, v in exec_error.params.items())
    else:
        return header + "none"


class SnsNotification(ExecutionStateObserver):

    def state_update(self, job: JobInfo):
        topic_arn = 'arn:aws:sns:eu-west-1:136604387399:my_topic'
        states = job.lifecycle.states()
        prev_state = states[-2] if len(states) > 1 else ExecutionState.NONE
        cur_state = states[-1]

        subject = "Job {} changed state from {} to {}".format(job.job_id, prev_state.name, cur_state.name)
        job_section = _create_job_section(job)

        if cur_state.is_failure():
            exec_error = job.exec_error
            message = _generate(job_section, _create_error_section(exec_error), _create_error_parameters(exec_error))
            notify(topic_arn, subject, message)
        else:
            notify(topic_arn, subject, job_section)
