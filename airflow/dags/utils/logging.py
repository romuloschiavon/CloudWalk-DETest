import logging
from airflow import configuration as conf
from airflow.utils.log.logging_mixin import LoggingMixin

def init_airflow_logging():
    logger = LoggingMixin().log
    return logger
