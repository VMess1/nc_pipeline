from moto import mock_s3
import boto3
import pandas as pd
import pytest
import os
from src.processing.read_write_files import (
    get_csv_data, write_to_bucket, compile_full_csv_table
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
    # def test_csv_is_read(self):
    #     output = read_csv('tests/test_processing/test_payment.csv')
    #     assert output.iloc[1]['payment_id'] == 2

    def test_csv_is_read_from_s3(self, mock_csv_bucket):
        mock_csv_bucket.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=new_string(),
            Key='payment/payment20221103150000.csv')
        # response = mock_csv_bucket.get_object(
        #     Bucket="nc-group3-ingestion-bucket",
        #     Key="payment/20221103150000.csv")
        data = get_csv_data(mock_csv_bucket,
                            "nc-group3-ingestion-bucket",
                            'payment/payment20221103150000.csv')
        assert data.iloc[0]['payment_id'] == 2

class TestCompileFullCsvTable:
    def test_data_includes_all_csv_files_in_directory(self, mock_csv_bucket):
        test_data_1 = (
            'item_id, item_name, created_at, last_updated\n' +
            '1, name_1, 2022-12-12 15:15:15, 2022-12-12 15:15:15\n'
            '2, name_2, 2022-12-12 15:15:15, 2022-12-12 15:15:15'
        )
        test_data_2 = (
            'item_id, item_name, created_at, last_updated\n' +
            '3, name_3, 2023-12-12 15:15:15, 2023-12-12 15:15:15'
            )
        test_data_3 = (
            'item_id, item_name, created_at, last_updated\n' +
            '4, name_4, 2024-12-12 15:15:15, 2024-12-12 15:15:15'
            )
        mock_csv_bucket.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=test_data_1,
            Key='test/test20221212151515.csv')
        mock_csv_bucket.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=test_data_2,
            Key='test/test20231212151515.csv')
        mock_csv_bucket.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=test_data_3,
            Key='test/test20241212151515.csv')
        test_expected = pd.DataFrame(data={
            'item_id': [1, 2, 3, 4],
            'item_name': ['item_1', 'item_2', 'item_3', 'item_4'],
            'created_at': ['2022-12-12 15:15:15', '2022-12-12 15:15:15',
                            '2023-12-12 15:15:15', '2024-12-12 15:15:15'],
            'last_updated': ['2022-12-12 15:15:15', '2022-12-12 15:15:15',
                            '2023-12-12 15:15:15', '2024-12-12 15:15:15']
        })

        output = compile_full_csv_table(mock_csv_bucket,
                                        "nc-group3-ingestion-bucket",
                                        'test')
        assert output.equals(test_expected)
        

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
            mock_parquet_bucket, 'dim_currency',
            test_df, '20221103142049962000'
        )
        response = mock_parquet_bucket.get_object(
            Bucket="nc-group3-transformation-bucket",
            Key="dim_currency/dim_currency20221103142049962000.parquet"
        )

        output = pd.read_parquet(BytesIO(response['Body'].read()))

        assert output.equals(currency_dataframe())
