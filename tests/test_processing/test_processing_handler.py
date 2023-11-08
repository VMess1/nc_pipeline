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


class TestErrorHandling:
    def test_ingestion_bucket_not_found(self):
        pass

    def test_transformation_bucket_not_found(self):
        pass
