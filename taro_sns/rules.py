import logging

import yaql
from yaql import yaqlization
from yaql.language.exceptions import YaqlException

log = logging.getLogger(__name__)


def validate_rules(rules):
    if len(rules) == 0:
        raise ValueError('No notification rules defined')
    for rule in rules:
        if hasattr(rule, 'when'):
            if not isinstance(rule.when, str):
                raise ValueError("When condition is not 'str'")
        if not isinstance(rule.notify, str):
            for topic in rule.notify:
                if not isinstance(topic, str):
                    raise ValueError("Notify array containing non-'str' value")


def create_topics_provider(rules):
    engine = yaql.factory.YaqlFactory().create()

    def create_topics(job):
        topics = []

        yaqlization.yaqlize(job)
        ctx = yaql.create_context()
        ctx['job'] = job

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

    return create_topics
