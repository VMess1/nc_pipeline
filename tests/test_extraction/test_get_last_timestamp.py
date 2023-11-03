from moto import mock_cloudwatch, mock_logs, mock_ssm
import pytest
import boto3
import os
from unittest.mock import patch
from botocore.exceptions import ClientError
from src.extraction.get_last_timestamp import (
    get_last_timestamp
)
from datetime import datetime


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture(scope="function")
def mock_params(aws_credentials):
    with mock_ssm():
        yield boto3.client("ssm", region_name="eu-west-2")

class TestGetLastTimestamp:
    def test_returns_value_if_parameter_found(self, mock_params):
        test_name = 'Test-parameter'
        test_value = '2023-01-02'
        response = mock_params.put_parameter(
            Name=test_name,
            Value='2023-01-02',
            Overwrite=True,
        )
        assert get_last_timestamp(test_name) == test_value

    def test_returns_none_if_parameter_not_found(self, mock_params):
        test_name = 'Test-parameter'
   
        assert get_last_timestamp(test_name) == None


# @pytest.fixture(scope="function")
# def mock_logger(aws_credentials):
#     with mock_logs():
#         yield boto3.client("logs", region_name="eu-west-2")

        # response = mock_params.get_parameter(
        #     Name='Test-parameter'
        # )
        # print(response['Parameter']['Value'])
        # response = mock_params.put_parameter(
        #     Name='Test-parameter',
        #     Value='2024-01-02',
        #     Overwrite=True,
        # )
        # response = mock_params.get_parameter(
        #     Name='Test-parameter'
        # )
        # print(response['Parameter']['Value'])
        # response_time = response['Parameter']['Value']
        # print(datetime.now())
        # last_time = datetime.strptime(response_time, '%Y-%m-%d')
        # print(last_time)

# def test_returns_log(mock_logger):
    
#     mock_logger.create_log_group(logGroupName='test-group')
#     #print(response, '<<<< create log group')

#     mock_logger.create_log_stream(logGroupName='test-group',
#     logStreamName='test-stream-1'
#     )
#     mock_logger.create_log_stream(logGroupName='test-group',
#     logStreamName='test-stream-2'
#     )

#     mock_logger.put_retention_policy(
#         logGroupName='test-group',
#         retentionInDays=1
#     )

#     log_stream_list = mock_logger.describe_log_streams(
#         logGroupName='test-group'
#     )
#     print(log_stream_list['logStreams'][0])
#     log_stream_name_1 = log_stream_list['logStreams'][0]['logStreamName']
#     log_stream_name_2 = log_stream_list['logStreams'][1]['logStreamName']
#     print(log_stream_name_1)
#     print(log_stream_name_2)

#     # print(log_stream_name, '<<<<< stream name')
#     #print(response, '<<<< create log stream')
#     response = mock_logger.put_log_events(
#             logGroupName='test-group',
#             logStreamName=log_stream_name_1,
#             logEvents=[
#                 {
#                     "timestamp": 1698933550,
#                     "message": "Hello World"
#                 }
#             ],
#             sequenceToken='aaa'
#         )

#     response_2 = mock_logger.get_log_events(
#         logGroupName='test-group',
#         logStreamName=log_stream_name_1,
#         startTime=1698933540,
#         endTime=1798933540
#     )
#     print(response_2['events'], '<<< Response 2')
    # print(dir(response_2), '<<<<<< get log event')
    # print(response_2.values(), '<<<<<< get log event')
    # def switch_log(string):
    #     print('Little_Oysters')
    #     response = mock_log.put_log_events(logGroupName='test_group',
    #          logStreamName='stringNCrandom',
    #                            logEvents=[
    #     #                             {
    #                                 'timestamp': 123,
    #                                 'message': string
    #                             }
    #                             ],
    #     )
        

    # logger = logging.getLogger('test')
    # monkeypatch.setattr(logger, "info", switch_log)
    # logger.setLevel(logging.INFO)
    # logger.propagate = True
    # logger.info('This is a test event')