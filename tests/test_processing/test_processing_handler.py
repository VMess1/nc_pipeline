from src.processing.processing_handler import read_csv, dim_remove_dates
from moto import mock_s3
import boto3
# import pytest
from tests.test_processing.strings import new_string
import pandas as pd


# @pytest.fixture(scope="function")
# def aws_credentials():
#     os.environ["AWS_ACCESS_KEY_ID"] = "test"
#     os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
#     os.environ["AWS_SECURITY_TOKEN"] = "test"
#     os.environ["AWS_SESSION_TOKEN"] = "test"
#     os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


class TestReadCSV:
    def test_csv_is_read(self):
        output = read_csv('tests/test_processing/test.csv')
        assert output.iloc[1]['payment_id'] == 2

    @mock_s3
    def test_csv_is_read_from_s3(self):
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group3-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        conn.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Body=new_string(),
            Key='payment/20221103150000')
        response = conn.get_object(
            Bucket="nc-group3-ingestion-bucket",
            Key="payment/20221103150000")
        data = read_csv(response['Body'])
        assert data.iloc[0]['payment_id'] == 2


class TestDimRemoveDates:
    def test_dates_are_removed_from_basic_tables(self):
        test_dataframe = pd.DataFrame(data={
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'EUR'],
            'created_at': ['2022-11-03 14:20:49.962000',
                           '2022-11-03 14:20:49.962000',
                           '2022-11-03 14:20:49.962000'],
            'last_updated': ['2022-11-03 14:20:49.962000',
                             '2022-11-03 14:20:49.962000',
                             '2022-11-03 14:20:49.962000']
        })
        expected_dataframe = pd.DataFrame(data={
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'EUR']
        })
        assert dim_remove_dates(test_dataframe).equals(expected_dataframe)
