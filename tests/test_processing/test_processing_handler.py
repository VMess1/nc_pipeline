from src.processing.processing_handler import (
    main)  # ,  write_to_bucket)  # , dim_join_department
from moto import mock_s3
import boto3
import pytest
import pandas as pd
import os
from tests.test_processing.strings import currency_string
from io import BytesIO
from dataframes import currency_dataframe_transformed
import logging
from testfixtures import LogCapture
from botocore.exceptions import ClientError

logger = logging.getLogger('test')
logger.setLevel(logging.INFO)
logger.propagate = True

@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope='function')
def mock_buckets(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group3-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        conn.create_bucket(
            Bucket="nc-group3-transformation-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield conn


# @pytest.fixture(scope='function')
# def mock_logger():
#     #with LogCapture():
#     logger = logging.getLogger('test')
#     logger.setLevel(logging.INFO)
#     logger.propagate = True
#     return logger


class TestBasicTableFunctionality:
    def test_currency_table_update_is_processed(
            self, mock_buckets, monkeypatch):
        def mock_get():
            return mock_buckets
        monkeypatch.setattr(
            'src.processing.processing_handler.get_client',
            mock_get)
        mock_buckets.put_object(Bucket='nc-group3-ingestion-bucket',
                                Body=currency_string(),
                                Key='currency/currency20221103150000.csv')

        test_event = {'table_list': ['currency'], 'timestamp': 20221103150000}
        main(test_event, None)
        response = mock_buckets.get_object(
            Bucket='nc-group3-transformation-bucket',
            Key='currency/currency20221103150000.parquet')
        output = pd.read_parquet(BytesIO(response['Body'].read()))
        expected_output = currency_dataframe_transformed()
        assert output.equals(expected_output)

# LOGGER = logging.getLogger(__name__)


class TestErrorHandling:
    # def test_ingestion_bucket_not_found(
    #         self, mock_logger, monkeypatch, caplog):
    #     def mock_get():
    #         print('The mock logger is returned')
    #         return mock_logger
    #     monkeypatch.setattr(logging, 'getLogger', mock_get)
    #     test_event = {'table_list': ['currency'],
    #                'timestamp': 20221103150000}
    #     mock_get.side_effect=ClientError(
    #         error_response={"Error": {"Code": "NoSuchBucket"}},
    #         operation_name="ClientError"
    #         )
    #     main(test_event, None)
    #     print(caplog.text, '<<<< caplog.text')
    #     assert "Bucket not found." in caplog.text
    
    # def test_ingestion_bucket_not_found(self, caplog):
    #     test_event = {'table_list': ['currency'],
    #                'timestamp': 20221103150000}
    #     with caplog.at_level(logging.INFO):
    #         main(test_event, None)
    #         print(dir(caplog))
    #         print(caplog.records, '<<<< caplog.text')
    #         assert "Bucket not found." in caplog.records

    def test_transformation_bucket_not_found(self):
        pass
