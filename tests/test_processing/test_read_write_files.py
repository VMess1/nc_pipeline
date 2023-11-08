from moto import mock_s3
import boto3
import pandas as pd
import pytest
import os
from src.processing.read_write_files import (
    read_csv, write_to_bucket
)
from tests.test_processing.strings import new_string
from dataframes import currency_dataframe
from io import BytesIO


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope='function')
def mock_csv_bucket(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group3-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield conn


@pytest.fixture(scope='function')
def mock_parquet_bucket(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group3-transformation-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield conn


class TestReadCSV:
    def test_csv_is_read(self):
        output = read_csv('tests/test_processing/test_payment.csv')
        assert output.iloc[1]['payment_id'] == 2

    def test_csv_is_read_from_s3(self, mock_csv_bucket):
        mock_csv_bucket.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=new_string(),
            Key='payment/20221103150000.csv')
        response = mock_csv_bucket.get_object(
            Bucket="nc-group3-ingestion-bucket",
            Key="payment/20221103150000.csv")
        data = read_csv(response['Body'])
        assert data.iloc[0]['payment_id'] == 2


class TestWriteToBucket:
    def test_dataframe_is_saved_to_bucket(
            self, mock_parquet_bucket
    ):
        test_df = currency_dataframe()
        response = write_to_bucket(
            mock_parquet_bucket, 'currency',
            test_df, '2022-11-03 14:20:49.962000'
        )
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    def test_parquet_file_is_added_to_bucket_and_is_readable(
            self, mock_parquet_bucket
    ):
        test_df = currency_dataframe()
        write_to_bucket(
            mock_parquet_bucket, 'currency',
            test_df, '20221103142049962000'
        )
        response = mock_parquet_bucket.get_object(
            Bucket="nc-group3-transformation-bucket",
            Key="currency/currency20221103142049962000.parquet"
        )

        output = pd.read_parquet(BytesIO(response['Body'].read()))

        assert output.equals(currency_dataframe())
