"""
This top-level module represents a taro plugin for SNS notification.
Each plugin module name must start with 'taro_' prefix.
"""
import logging

import taro.util
from taro_sns import rules
import taro
from taro import PluginBase, PluginDisabledError, JobInstance, HostinfoError, NestedNamespace
from taro_sns import rules

RULES_FILE = 'taro_sns_rules.yaml'

log = logging.getLogger(__name__)


def read_validate_rules() -> NestedNamespace:
    try:
        rules_path = taro.lookup_file_in_config_path(RULES_FILE)
    except FileNotFoundError as e:
        raise PluginDisabledError('Rules file lookup failed -> ' + str(e)) from e
    rules_config = taro.util.read_yaml_file(rules_path)
    if not rules_config or (not hasattr(rules_config, 'states') or not hasattr(rules_config, 'warnings')):
        raise PluginDisabledError('Rules file must contain "states" and/or "warnings" sections')
    try:
        rules.validate_rules(rules_config.get('states', ()))
        rules.validate_rules(rules_config.get('warnings', ()))
    except Exception as e:
        raise PluginDisabledError('Invalid rules file -> ' + str(e)) from e

    return rules_config


def disable_boto3_logging():
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('nose').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)


class SnsPlugin(PluginBase):

    def __init__(self):
        disable_boto3_logging()
        try:
            host_info = taro.read_hostinfo()
        except HostinfoError:
            log.exception('event=[hostinfo_error]')
            host_info = {}
        # Import 'notification' package no sooner than boto3 logging is disabled to prevent boto3 init logs
        # Import 'notification' package only when validation is successful to prevent unnecessary boto3 import
        from taro_sns.notification import SnsNotification
        validated_rules = read_validate_rules()
        self.sns_notification =\
            SnsNotification(
                rules.create_topics_provider_states(validated_rules.get('states')),
                rules.create_topics_provider_warnings(validated_rules.get('warnings')),
                host_info)

    def new_job_instance(self, job_instance: JobInstance):
        # self.sns_notification.state_update(job_instance.create_info())  # Notify job created
        job_instance.add_state_observer(self.sns_notification)
        job_instance.add_warning_observer(self.sns_notification)
