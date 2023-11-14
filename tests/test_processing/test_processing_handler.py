from src.processing.processing_handler import (
    main)
from moto import mock_s3
import boto3
import pytest
import pandas as pd
import os
from tests.test_processing.strings import currency_string
from io import BytesIO
from dataframes import currency_dataframe_transformed
import logging
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


@pytest.fixture(scope='function')
def mock_missing_csv_bucket(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group3-transformation-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield conn


@pytest.fixture(scope='function')
def mock_missing_parquet_bucket(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group3-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield conn


class TestDateTable:
    def test_date_table_is_only_built_once(self, mock_buckets, monkeypatch):
        '''
        Creates mock buckets and puts an object in the ingestion bucket.
        Tests that, when the main function is called twice, the dim_date
        parquet table is only created once.
        '''
        def mock_get():
            return mock_buckets
        monkeypatch.setattr(
            'src.processing.processing_handler.get_client',
            mock_get)
        mock_buckets.put_object(Bucket='nc-group3-ingestion-bucket',
                                Body=currency_string(),
                                Key='currency/currency20221103150000.csv')
        test_event = {'Records': [{
            's3': {
                'bucket': {'name': 'nc-group3-ingestion-bucket'},
                'object': {'key': 'currency/currency20221103150000.csv'}
            }
        }]}

        main(test_event, None)
        main(test_event, None)
        from src.processing.processing_handler import (COUNT)
        assert COUNT == 1


class TestBasicTableFunctionality:
    def test_currency_table_update_is_processed(
            self, mock_buckets, monkeypatch):
        '''
        Tests that, for a currency table update, the main function processes
        the data and writes to the transformation bucket as required.
        '''
        def mock_get():
            return mock_buckets
        monkeypatch.setattr(
            'src.processing.processing_handler.get_client',
            mock_get)
        mock_buckets.put_object(Bucket='nc-group3-ingestion-bucket',
                                Body=currency_string(),
                                Key='currency/currency20221103150000.csv')
        test_event = {'Records': [{
            's3': {
                'bucket': {'name': 'nc-group3-ingestion-bucket'},
                'object': {'key': 'currency/currency20221103150000.csv'}
            }
        }]}
        main(test_event, None)
        response = mock_buckets.get_object(
            Bucket='nc-group3-transformation-bucket',
            Key='dim_currency/dim_currency20221103150000.parquet')
        output = pd.read_parquet(BytesIO(response['Body'].read()))
        expected_output = currency_dataframe_transformed()
        assert output.equals(expected_output)


class TestWarning:
    def test_invalid_currency_code_logs_warning(self, mock_buckets,
                                                monkeypatch, caplog):
        '''
        Tests that a warning is logged when an invalid currency code is used.
        '''
        def mock_get_csv_data(*args):
            return pd.DataFrame(data={
                'currency_id': [1, 2, 3],
                'currency_code': ['GBP', 'USD', 'ABC'],
                'created_at': ['2022-11-03 14:20:49.962000',
                               '2022-11-03 14:20:49.962000',
                               '2022-11-03 14:20:49.962000'],
                'last_updated': ['2022-11-03 14:20:49.962000',
                                 '2022-11-03 14:20:49.962000',
                                 '2022-11-03 14:20:49.962000']
            })
        monkeypatch.setattr(
            'src.processing.processing_handler.get_csv_data',
            mock_get_csv_data)

        def mock_get():
            return mock_buckets
        monkeypatch.setattr(
            'src.processing.processing_handler.get_client',
            mock_get)

        test_event = {'Records': [{
            's3': {
                'bucket': {'name': 'nc-group3-ingestion-bucket'},
                'object': {'key': 'currency/currency20221103150000.csv'}
            }
        }]}
        with caplog.at_level(logging.INFO):
            main(test_event, None)
            expected = ('Invalid currency code detected in file: ' +
                        'currency/currency20221103150000.csv')
            main(test_event, None)
            assert expected in caplog.text


class TestErrorHandling:
    def test_ingestion_bucket_not_found(
            self, mock_missing_csv_bucket,
            monkeypatch, caplog
    ):
        '''
        Tests that an error is logged if the ingestion bucket
        does not exist.
        '''
        def mock_get():
            return mock_missing_csv_bucket
        monkeypatch.setattr(
            'src.processing.processing_handler.get_client',
            mock_get)
        test_event = {'Records': [{
            's3': {
                'bucket': {'name': 'nc-group3-ingestion-bucket'},
                'object': {'key': 'currency/currency20221103150000.csv'}
            }
        }]}
        with caplog.at_level(logging.INFO):
            main(test_event, None)
            expected = "Bucket not found: nc-group3-ingestion-bucket"
            assert expected in caplog.text

    def test_transformation_bucket_not_found(
            self, mock_missing_parquet_bucket,
            monkeypatch, caplog
    ):
        '''
        Tests that an error is logged if the transformation bucket
        does not exist.
        '''
        def mock_get():
            mock_missing_parquet_bucket.put_object(
                Bucket='nc-group3-ingestion-bucket',
                Body=currency_string(),
                Key='currency/currency20221103150000.csv'
            )
            return mock_missing_parquet_bucket
        monkeypatch.setattr(
            'src.processing.processing_handler.get_client',
            mock_get
        )
        test_event = {'Records': [{
            's3': {
                'bucket': {'name': 'nc-group3-ingestion-bucket'},
                'object': {'key': 'currency/currency20221103150000.csv'}
            }
        }]}
        with caplog.at_level(logging.INFO):
            main(test_event, None)
            expected = "Bucket not found: nc-group3-transformation-bucket"
            assert expected in caplog.text

    def test_handler_logs_internal_service_errors(
            self, mock_buckets,
            monkeypatch, caplog
    ):
        '''
        Tests that errors are logged for service errors.
        '''
        def mock_get():
            response = {"Error": {"Code": "InternalServiceError"}}
            error = ClientError(response, 'test')
            raise error
        monkeypatch.setattr(
            'src.processing.processing_handler.get_client',
            mock_get
        )
        test_event = {'Records': [{
            's3': {
                'bucket': {'name': 'nc-group3-ingestion-bucket'},
                'object': {'key': 'currency/currency20221103150000.csv'}
            }
        }]}
        with caplog.at_level(logging.INFO):
            main(test_event, None)
            expected = "Internal service error detected."
            assert expected in caplog.text

    def test_handler_logs_error_for_incorrect_file_type(
            self, mock_buckets,
            monkeypatch, caplog
    ):
        '''
        Tests that errors are logged if the file type in the
        ingestion bucket is not CSV.
        '''
        def mock_get():
            return mock_buckets
        monkeypatch.setattr(
            'src.processing.processing_handler.get_client',
            mock_get
        )
        test_event = {'Records': [{
            's3': {
                'bucket': {'name': 'nc-group3-ingestion-bucket'},
                'object': {'key': 'currency/currency20221103150000.txt'}
            }
        }]}
        with caplog.at_level(logging.INFO):
            main(test_event, None)
            expected = "Incorrect parameter type: File type is not csv."
            assert expected in caplog.text
