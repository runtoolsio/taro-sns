"""
This top-level module represents a taro plugin for SNS notification.
Each plugin module name must start with 'taro_' prefix.
"""
from taro_sns.notification import SnsNotification


def create_execution_listener():
    """
    This method is called by the plugin framework

    :return: A listener sending SNS notification on execution state changes
    """
    return SnsNotification()
