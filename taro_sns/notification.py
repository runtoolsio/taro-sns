import logging
import textwrap

import boto3

import taro
from taro import ExecutionState, ExecutionStateObserver, JobInfo, ExecutionError, WarningObserver, Warn, WarnEventCtx

log = logging.getLogger(__name__)

client = boto3.client("sns")


def notify(topics, subject, message):
    for topic in topics:
        client.publish(TopicArn=topic, Subject=subject, Message=message)
        log.debug("event=[sns_notified] topic=[{}] subject=[{}]".format(topic, subject))


def _generate(*sections):
    return "\n\n".join((textwrap.dedent(section) for section in sections if section))


def _header(header_text):
    return header_text + "\n" + "-" * len(header_text)


def _create_job_section(job: JobInfo, *, always_exec_time: bool):
    s = _header("Job Detail")
    s += "\nJob: " + job.job_id
    s += "\nInstance: " + job.instance_id
    s += "\nExecuted: " + str(job.lifecycle.executed_at)
    s += "\nState: " + job.state.name
    s += "\nState changed: " + str(job.lifecycle.last_changed_at)
    if job.state.is_terminal() or always_exec_time:
        s += "\nExecution Time: " + taro.format_timedelta(job.lifecycle.execution_time)
    if job.warnings:
        s += "\nWarnings: " + ",".join((name + ": " + str(count) for name, count in job.warnings.items()))
    return s


def _create_hostinfo_section(host_info):
    if not host_info:
        return ''
    s = _header("Host Info")
    for name, value in host_info.items():
        s += f"\n{name}: {value}"
    return s


def _create_error_section(job: JobInfo, exec_error: ExecutionError):
    s = _header("Error Detail")
    s += "\nReason: " + str(exec_error)
    if job.status:
        s += "\nMessage: " + job.status
    if exec_error.params:
        s += "\nParams:" + "\n  ".join("{}: {}".format(k, v) for k, v in exec_error.params.items())
    else:
        s += "\nParams: (none)"

    return s


class SnsNotification(ExecutionStateObserver, WarningObserver):

    def __init__(self, topics_provider_states, topics_provider_warnings, hostinfo):
        self.topics_provider_states = topics_provider_states
        self.topics_provider_warnings = topics_provider_warnings
        self.hostinfo = hostinfo

    def state_update(self, job: JobInfo):
        topics = self.topics_provider_states(job)
        if not topics:
            return

        states = job.lifecycle.states
        prev_state = states[-2] if len(states) > 1 else ExecutionState.NONE
        cur_state = states[-1]

        subject = "Job {} changed state from {} to {}".format(job.job_id, prev_state.name, cur_state.name)
        sections = [_create_job_section(job, always_exec_time=False), _create_hostinfo_section(self.hostinfo)]

        if cur_state.is_failure():
            sections.append(_create_error_section(job, job.exec_error))
            subject += "!"
        if job.warnings:
            subject += " with warnings!"

        notify(topics, subject, _generate(*sections))

    def new_warning(self, job_info: JobInfo, warning: Warn, event_ctx: WarnEventCtx):
        topics = self.topics_provider_warnings(job_info, warning, event_ctx)
        if not topics:
            return

        subject = "!New warning {} for {}@{}!".format(warning.name, job_info.job_id, job_info.instance_id)
        sections = [_create_job_section(job_info, always_exec_time=True), _create_hostinfo_section(self.hostinfo)]

        notify(topics, subject, _generate(*sections))
