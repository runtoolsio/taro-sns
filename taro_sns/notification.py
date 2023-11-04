import boto3
import logging
import taro
import textwrap
from taro import ExecutionState, InstanceStateObserver, JobInst, ExecutionError, Warn, WarnEventCtx
from taro.jobs.execution import ExecutionPhase, Flag

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


def _create_job_section(job: JobInst, *, always_exec_time: bool):
    s = _header("Job Detail")
    s += "\nJob: " + job.job_id
    s += "\nInstance: " + job.run_id
    s += "\nExecuted: " + str(job.lifecycle.executed_at)
    s += "\nState: " + job.phase.name
    s += "\nState changed: " + str(job.lifecycle.last_transition_at)
    if job.phase.in_phase(ExecutionPhase.TERMINAL) or always_exec_time:
        s += "\nExecution Time: " + taro.format_timedelta(job.lifecycle.total_executing_time)
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


def _create_error_section(job: JobInst, exec_error: ExecutionError):
    s = _header("Error Detail")
    s += "\nReason: " + str(exec_error)
    if job.status:
        s += "\nMessage: " + job.status
    if exec_error.params:
        s += "\nParams:" + "\n  ".join("{}: {}".format(k, v) for k, v in exec_error.params.items())
    else:
        s += "\nParams: (none)"

    return s


class SnsNotification(InstanceStateObserver, InstanceWarningObserver):

    def __init__(self, topics_provider_states, topics_provider_warnings, hostinfo):
        self.topics_provider_states = topics_provider_states
        self.topics_provider_warnings = topics_provider_warnings
        self.hostinfo = hostinfo

    def instance_state_update(self, job_inst: JobInst, prev_state, new_state, changed):
        topics = self.topics_provider_states(job_inst)
        if not topics:
            return

        subject = "Job {} changed state from {} to {}".format(job_inst.job_id, prev_state.name, new_state.name)
        sections = [_create_job_section(job_inst, always_exec_time=False), _create_hostinfo_section(self.hostinfo)]

        if new_state.has_flag(Flag.FAILURE):
            sections.append(_create_error_section(job_inst, job_inst.run_error))
            subject += "!"
        if job_inst.warnings:
            subject += " with warnings!"

        notify(topics, subject, _generate(*sections))

    def new_warning(self, job_inst: JobInst, warn_ctx: WarnEventCtx):
        topics = self.topics_provider_warnings(job_inst, warn_ctx.warning, warn_ctx)
        if not topics:
            return

        subject = "!New warning {} for {}@{}!".format(warn_ctx.warning.name, job_inst.job_id, job_inst.run_id)
        sections = [_create_job_section(job_inst, always_exec_time=True), _create_hostinfo_section(self.hostinfo)]

        notify(topics, subject, _generate(*sections))
