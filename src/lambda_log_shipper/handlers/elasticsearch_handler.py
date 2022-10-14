from typing import List
import random
import os
from datetime import date
import json
import requests

from lambda_log_shipper.handlers.base_handler import LogsHandler, LogRecord
from lambda_log_shipper.configuration import Configuration
from lambda_log_shipper.handlers.s3_handler import S3Handler
from lambda_log_shipper.utils import get_logger

class HTTPCodeException(Exception):
    """Raised when status code is over 400"""
    pass

class ElasticSearchHandler(LogsHandler):
    def handle_logs(self, records: List[LogRecord]) -> bool:
        if not records:
            return False
        get_logger().debug("ElasticSearchHandler started to run")
        index_name = ElasticSearchHandler.generate_key_name()
        log_stream_name = os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME") or "unknown_log_stream"

        data = ElasticSearchHandler.format_records(records=records, index_name=index_name, log_stream_name=log_stream_name)
        try:
            ElasticSearchHandler.send_logs(data)
            get_logger().info(f"ElasticSearchHandler put {len(records)} logs")
        except (requests.exceptions.RequestException, HTTPCodeException, Exception) as e:
            get_logger().error(f"Error sending logs to Elastic Search: {e}")
            # Try via S3
            S3Handler().handle_logs(records)
        return True

    @staticmethod
    def send_logs(data: str):
        url = os.environ.get('LOGS_ELASTICSEARCH_URL')
        if not url:
            raise Exception("Missing LOGS_ELASTICSEARCH_URL environment variable")
        headers = {
            'Content-Type': 'application/json',
        }
        user = os.environ.get('LOGS_ELASTICSEARCH_USER')
        password = os.environ.get('LOGS_ELASTICSEARCH_PASSWORD')
        # throw if we dont have user and password
        if not user or not password:
            raise Exception("Missing LOGS_ELASTICSEARCH_USER or LOGSELASTICSEARCH_PASSWORD environment variable")
        response = requests.post(
            url, 
            data=data, 
            headers=headers, 
            auth=requests.auth.HTTPBasicAuth(
                user,
                password
            )
        )
        if response.status_code >= 400:
            raise HTTPCodeException(f"HTTP code {response.status_code}")
        return response.text

    @staticmethod
    def generate_key_name():
        # get current date as string
        today = str(date.today())
        function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')
        return f"lambda-{function_name}-{today}"

    @staticmethod
    def format_records(records: List[LogRecord], index_name: str, log_stream_name: str) -> str:
        data = []
        for record in records:
            index_object = { "index": { "_index": index_name, } }
            data.append(index_object)
            log_object = { "@timestamp": record.log_time.isoformat(), "log_type": record.log_type.value, "log_stream_name": log_stream_name, "log": record.record }
            data.append(log_object)
        return json.dumps(data);
