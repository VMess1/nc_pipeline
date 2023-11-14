from moto import mock_s3
import boto3
import pandas as pd
import pytest
import os
from src.processing.read_write_files import (
    get_csv_data,
    write_to_bucket,
    compile_full_csv_table,
    check_transformation_bucket)
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
    def test_csv_is_read_from_s3(self, mock_csv_bucket):
        '''
        Tests that csv files are read correctly and so
        can be interrogated.
        '''
        mock_csv_bucket.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=new_string(),
            Key='payment/payment20221103150000.csv')
        data = get_csv_data(mock_csv_bucket,
                            "nc-group3-ingestion-bucket",
                            'payment/payment20221103150000.csv')
        assert data.iloc[0]['payment_id'] == 2


class TestCompileFullCsvTable:
    def test_data_includes_all_csv_files_in_directory(self, mock_csv_bucket):
        '''
        Test that compile_full_csv_table returns a dataframe with
        all csv files in one dataframe.
        '''
        test_data_1 = (
            'item_id;item_name;created_at;last_updated\n' +
            '1;name_1;2022-12-12 15:15:15;2022-12-12 15:15:15\n'
            '2;name_2;2022-12-12 15:15:15;2022-12-12 15:15:15'
        )
        test_data_2 = (
            'item_id;item_name;created_at;last_updated\n' +
            '3;name_3;2023-12-12 15:15:15;2023-12-12 15:15:15'
        )
        test_data_3 = (
            'item_id;item_name;created_at;last_updated\n' +
            '4;name_4;2024-12-12 15:15:15;2024-12-12 15:15:15'
        )
        mock_csv_bucket.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=test_data_1,
            Key='item/item20221212151515.csv')
        mock_csv_bucket.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=test_data_2,
            Key='item/item20231212151515.csv')
        mock_csv_bucket.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=test_data_3,
            Key='item/item20241212151515.csv')
        test_expected = pd.DataFrame(data={
            'item_id': [1, 2, 3, 4],
            'item_name': ['name_1', 'name_2', 'name_3', 'name_4'],
            'created_at': ['2022-12-12 15:15:15', '2022-12-12 15:15:15',
                           '2023-12-12 15:15:15', '2024-12-12 15:15:15'],
            'last_updated': ['2022-12-12 15:15:15', '2022-12-12 15:15:15',
                             '2023-12-12 15:15:15', '2024-12-12 15:15:15']
        })

        output = compile_full_csv_table(mock_csv_bucket,
                                        "nc-group3-ingestion-bucket",
                                        'item')
        assert output.equals(test_expected)

    def test_data_removes_duplicate_csv_files_in_directory(
            self, mock_csv_bucket):
        '''
        test that compiling removes any duplicate rows of data 
        '''
        test_data_1 = (
            'item_id;item_name;created_at;last_updated\n' +
            '1;name_1;2022-12-12 15:15:15;2022-12-12 15:15:15\n'
            '2;name_2;2022-12-12 15:15:15;2022-12-12 15:15:15'
        )
        test_data_2 = (
            'item_id;item_name;created_at;last_updated\n' +
            '3;name_3;2023-12-12 15:15:15;2023-12-12 15:15:15'
        )
        test_data_3 = (
            'item_id;item_name;created_at;last_updated\n' +
            '2;name_5;2030-12-12 15:15:15;2030-12-12 15:15:15'
        )
        mock_csv_bucket.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=test_data_1,
            Key='item/item20221212151515.csv')
        mock_csv_bucket.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=test_data_2,
            Key='item/item20231212151515.csv')
        mock_csv_bucket.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=test_data_3,
            Key='item/item20301212151515.csv')
        test_expected = pd.DataFrame(data={
            'item_id': [1, 3, 2],
            'item_name': ['name_1', 'name_3', 'name_5'],
            'created_at': ['2022-12-12 15:15:15', '2023-12-12 15:15:15',
                           '2030-12-12 15:15:15'],
            'last_updated': ['2022-12-12 15:15:15', '2023-12-12 15:15:15',
                             '2030-12-12 15:15:15']
        })
        output = compile_full_csv_table(mock_csv_bucket,
                                        "nc-group3-ingestion-bucket",
                                        'item')
        assert output.equals(test_expected)


class TestWriteToBucket:
    def test_dataframe_is_saved_to_bucket(
            self, mock_parquet_bucket
    ):
        '''
        tests that write_to_bucket saves parquet data
        to transoformation bucket
        '''
        test_df = currency_dataframe()
        response = write_to_bucket(
            mock_parquet_bucket, 'currency',
            test_df, '2022-11-03 14:20:49.962000'
        )
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    def test_parquet_file_is_added_to_bucket_and_is_readable(
            self, mock_parquet_bucket
    ):
        '''
        tests that the parquet file that is added is readable when downloaded
        '''
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


class TestParquetChecker:
    def test_gives_list_of_directories(self, mock_parquet_bucket):
        '''
        test that check_transformation_bucket returns a list of
        directories, i.e. table names.
        '''
        mock_parquet_bucket.put_object(
            Bucket='nc-group3-transformation-bucket',
            Body='string',
            Key='fact_sales_order/fact_sales_order20221103150000.parquet')
        mock_parquet_bucket.put_object(
            Bucket='nc-group3-transformation-bucket',
            Body='string',
            Key='dim_currency/dim_currency20221103150000.parquet')
        expected = ['dim_currency', 'fact_sales_order']
        output = check_transformation_bucket(
            mock_parquet_bucket, 'nc-group3-transformation-bucket')
        assert output == expected
