import logging

import yaql
from yaql.language.exceptions import YaqlException

from taro import WarningEvent

log = logging.getLogger(__name__)

engine = yaql.factory.YaqlFactory().create()


def validate_rules(rules):
    for rule in rules:
        if hasattr(rule, 'when'):
            if not isinstance(rule.when, str):
                raise ValueError("When condition is not 'str'")
        if not isinstance(rule.notify, str):
            for topic in rule.notify:
                if not isinstance(topic, str):
                    raise ValueError("Notify array containing non-'str' value")


def create_topics_provider_states(rules):
    def create_topics(job):
        if not rules:
            return ()
        # from yaql import yaqlization
        # yaqlization.yaqlize(job)
        ctx = yaql.create_context()
        _add_job_context(ctx, job)
        return get_topics(rules, ctx)

    return create_topics


def create_topics_provider_warnings(rules):
    def create_topics(job, warning, event):
        if not rules:
            return ()
        ctx = yaql.create_context()
        _add_job_context(ctx, job)
        _add_warning_context(ctx, warning, event)
        return get_topics(rules, ctx)

    return create_topics


def _add_job_context(ctx, job):
    ctx['job_id'] = job.job_id
    ctx['state'] = job.state.name
    ctx['failure'] = job.state.is_failure()
    ctx['state_groups'] = [group.name for group in job.state.groups]


def _add_warning_context(ctx, warning, event: WarningEvent):
    ctx['warning_id'] = warning.id
    ctx['event'] = event.name
    ctx['new_warning'] = event == WarningEvent.NEW_WARNING  # TODO etc.


def get_topics(rules, ctx):
    topics = []
    for rule in rules:
        try:
            if not hasattr(rule, 'when') or engine(rule.when).evaluate(context=ctx) is True:
                if isinstance(rule.notify, str):
                    topics.append(rule.notify)
                else:
                    topics += rule.notify  # List expected
        except YaqlException as e:
            log.warning('event=[sns_rule_condition_invalid] condition=[%s] detail=[%s]', rule.when, e)
    return topics
