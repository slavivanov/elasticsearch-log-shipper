from dataclasses import asdict
import datetime
from re import A
import pytest
import json
import responses

from moto import mock_s3
import boto3

from lambda_log_shipper.handlers.base_handler import LogRecord, LogType
from lambda_log_shipper.handlers.elasticsearch_handler import ElasticSearchHandler
from lambda_log_shipper.handlers.elasticsearch_handler import S3Handler
from lambda_log_shipper.configuration import Configuration

ELASTICSEARCH_URL="https://example.com/"

@pytest.fixture(autouse=True)
def lambda_name(monkeypatch):
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "func")
    monkeypatch.setenv("LOGS_ELASTICSEARCH_URL", ELASTICSEARCH_URL)
    monkeypatch.setenv("LOGS_ELASTICSEARCH_USER", "user")
    monkeypatch.setenv("LOGS_ELASTICSEARCH_PASSWORD", "password")
    monkeypatch.setenv("AWS_LAMBDA_LOG_STREAM_NAME", "log_stream_name")

def test_generate_key_name(record):
    t1 = datetime.datetime(2020, 5, 22, 10, 20, 30)
    r1 = LogRecord(**{**asdict(record), "log_time": t1})
    t2 = datetime.datetime(2020, 5, 22, 10, 20, 35)
    r2 = LogRecord(**{**asdict(record), "log_time": t2})

    today = str(datetime.date.today())

    key = ElasticSearchHandler.generate_key_name()

    assert key.startswith(f"lambda-func-{today}")


def test_format_records(record):
    t1 = datetime.datetime(2020, 5, 22, 10, 20, 30, 123456)
    r1 = LogRecord(log_type=LogType.START, log_time=t1, record="Log Message A")
    r2 = LogRecord(log_type=LogType.FUNCTION, log_time=t1, record="Message B")

    log_stream_name = "log_stream_name"
    index_name = "index_name"

    data = ElasticSearchHandler.format_records(records=[r1, r2], log_stream_name=log_stream_name, index_name=index_name)

    expected = json.dumps([{"index": {"_index": "index_name"}}, {"@timestamp": "2020-05-22T10:20:30.123456", "log_type": "START", "log_stream_name": "log_stream_name", "log": "Log Message A"}, {"index": {"_index": "index_name"}}, {"@timestamp": "2020-05-22T10:20:30.123456", "log_type": "FUNCTION", "log_stream_name": "log_stream_name", "log": "Message B"}])
    assert data == expected

@responses.activate
def test_handle_logs_happy_flow(record):
    responses.add(responses.POST, ELASTICSEARCH_URL, json={"acknowledged": True}, status=200)

    t1 = datetime.datetime(2020, 5, 22, 10, 20, 30)
    record = LogRecord(log_type=LogType.START, log_time=t1, record="Log Message A")
    result = ElasticSearchHandler().handle_logs([record])
    assert result == True
    # assert that we called the URL properly
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == ELASTICSEARCH_URL
    assert responses.calls[0].request.method == "POST"
    assert responses.calls[0].response.status_code == 200
    assert responses.calls[0].request.body == json.dumps([{"index": {"_index": "lambda-func-2022-10-14"}}, {"@timestamp": "2020-05-22T10:20:30", "log_type": "START", "log_stream_name": "log_stream_name", "log": "Log Message A"}])

@mock_s3
def test_handle_logs_fallback_to_s3(record, monkeypatch):
    s3 = boto3.client("s3", region_name="us-west-2")
    monkeypatch.setattr(Configuration, "s3_bucket_arn", "123")
    s3.create_bucket(
        Bucket="123", CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )

    t1 = datetime.datetime(2020, 5, 22, 10, 20, 30, 123456)
    r1 = LogRecord(log_type=LogType.START, log_time=t1, record="a")

    responses.add(responses.POST, ELASTICSEARCH_URL, json={"acknowledged": True}, status=400)

    assert ElasticSearchHandler().handle_logs([r1])

    key_name = s3.list_objects(Bucket=Configuration.s3_bucket_arn)["Contents"][0]["Key"]
    obj = s3.get_object(Key=key_name, Bucket=Configuration.s3_bucket_arn)
    assert obj["Body"].read().decode() == "2020-05-22T10:20:30.123456    START     a"


# @mock_s3
# def test_handle_logs_no_config(monkeypatch, record):
#     assert not S3Handler().handle_logs([record])  # no exception
