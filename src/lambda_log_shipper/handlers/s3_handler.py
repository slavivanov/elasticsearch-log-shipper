from typing import List
import random

import boto3

from lambda_log_shipper.handlers.base_handler import LogsHandler, LogRecord
from lambda_log_shipper.configuration import Configuration
from lambda_log_shipper.utils import get_logger

TIME_PADDING = 30
TYPE_PADDING = 10


class S3Handler(LogsHandler):
    def handle_logs(self, records: List[LogRecord]) -> bool:
        if Configuration.s3_bucket_arn and records:
            get_logger().debug("S3Handler started to run")
            s3 = boto3.client("s3")
            key = S3Handler.generate_key_name(records)
            file_data = S3Handler.format_records(records)
            s3.put_object(Body=file_data, Bucket=Configuration.s3_bucket_arn, Key=key)
            get_logger().info(f"S3Handler put {len(records)} logs in {key}")
            return True
        return False

    @staticmethod
    def generate_key_name(records: List[LogRecord]):
        t = min(r.log_time for r in records)
        directory = f"logs/{t.year}/{t.month}/{t.day}/"
        return f"{directory}{t.hour}:{t.minute}:{t.second}:{t.microsecond}-{random.random()}"

    @staticmethod
    def format_records(records: List[LogRecord]) -> bytes:
        return "\n".join(map(S3Handler._format_record, records)).encode()

    @staticmethod
    def _format_record(r: LogRecord):
        return f"{r.log_time.isoformat():{TIME_PADDING}}{r.log_type.value:{TYPE_PADDING}}{r.record}"
