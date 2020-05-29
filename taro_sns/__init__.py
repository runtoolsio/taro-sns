"""
This top-level module represents a taro plugin for SNS notification.
Each plugin module name must start with 'taro_' prefix.
"""
import logging

from taro_sns import rules
import taro
from taro import PluginBase, PluginDisabledError, JobControl, HostinfoError

RULES_FILE = 'taro_sns_rules.yaml'

log = logging.getLogger(__name__)


def read_validate_rules():
    try:
        rules_path = taro.lookup_config_file_path(RULES_FILE)
    except FileNotFoundError as e:
        raise PluginDisabledError('Rules file lookup failed -> ' + str(e)) from e
    rules_config = taro.read_config(rules_path)
    if not rules_config or not hasattr(rules_config, 'rules'):
        raise PluginDisabledError('Rules file is empty')
    try:
        rules.validate_rules(rules_config.rules)
    except Exception as e:
        raise PluginDisabledError('Invalid rules file -> ' + str(e)) from e

    return rules_config.rules


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
        self.sns_notification = SnsNotification(rules.create_topics_provider(read_validate_rules()), host_info)

    def new_job_instance(self, job_instance: JobControl):
        # self.sns_notification.state_update(job_instance.create_info())  # Notify job created
        job_instance.add_observer(self.sns_notification)
