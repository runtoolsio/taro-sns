"""
This top-level module represents a taro plugin for SNS notification.
Each plugin module name must start with 'taro_' prefix.
"""
from taro import PluginBase, JobControl
from taro_sns.notification import SnsNotification


class SnsPlugin(PluginBase):

    def new_job_instance(self, job_instance: JobControl):
        job_instance.add_observer(SnsNotification())
