"""
This top-level module represents a taro plugin for SNS notification.
Each plugin module name must start with 'taro_' prefix.
"""
import logging

import taro
from taro import PluginBase, PluginDisabledError, JobControl

RULES_FILE = 'taro_sns_rules.yaml'


def validate_rules(config):
    rules = config.rules
    if len(rules) == 0:
        raise ValueError('No notification rules defined in rules file ' + RULES_FILE)
    for rule in rules:
        if not isinstance(rule.when, str):
            raise ValueError("When condition is not 'str'")
        for topic in rule.notify:
            if not isinstance(topic, str):
                raise ValueError("Notify array containing non-'str' value")


def disable_boto3_logging():
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('nose').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)


class SnsPlugin(PluginBase):

    def __init__(self):
        try:
            rules_path = taro.lookup_config_file_path(RULES_FILE)
        except FileNotFoundError as e:
            raise PluginDisabledError('Rules file lookup failed ' + RULES_FILE) from e
        rules = taro.read_config(rules_path)
        try:
            validate_rules(rules)
        except Exception as e:
            raise PluginDisabledError('Invalid rules: ' + str(e)) from e

        disable_boto3_logging()
        # Import 'notification' package no sooner than boto3 logging is disabled to prevent boto3 init logs
        # Import 'notification' package only when validation is successful to prevent unnecessary boto3 import
        from taro_sns.notification import SnsNotification
        self.sns_notification = SnsNotification(rules.rules)

    def new_job_instance(self, job_instance: JobControl):
        job_instance.add_observer(self.sns_notification)
