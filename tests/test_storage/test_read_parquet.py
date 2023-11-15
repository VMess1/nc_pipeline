import pytest
import boto3
import os
import pandas as pd
from moto import mock_s3
from io import BytesIO
from parquet_data import (currency_dataframe,
                          dim_location_dataframe1,
                          dim_location_dataframe2)
from src.storage.read_parquet import (
    get_file_list,
    get_parquet_data,
    compile_parquet_data)


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope='function')
def mock_parquet_bucket(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group3-transformation-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield conn


class TestGetFileListFunction:
    def test_returns_all_files_when_passed_old_timestamp(
            self,
            mock_parquet_bucket):
        """Tests that the get_file_list function is able to return
        all the files if a passed timestamp is older than all file
        names"""
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='payment/payment20221103150000.parquet',
            Body=BytesIO()
        )
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='payment/payment20231103150000.parquet',
            Body=BytesIO()
        )
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='payment/payment20241103150000.parquet',
            Body=BytesIO()
        )
        result = get_file_list(mock_parquet_bucket,
                               "nc-group3-transformation-bucket",
                               "payment",
                               "20201103150000")
        test_expected = ['payment/payment20221103150000.parquet',
                         'payment/payment20231103150000.parquet',
                         'payment/payment20241103150000.parquet']
        assert result == test_expected

    def test_returns_only_most_recent_files_compared_to_timestamp(
            self,
            mock_parquet_bucket
    ):
        """Tests that the get_file_list function is able to return
        all the files if a passed timestamp is older than all file
        names"""
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='payment/payment20221103150000.parquet',
            Body=BytesIO()
        )
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='payment/payment20231103150000.parquet',
            Body=BytesIO()
        )
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='payment/payment20241103150000.parquet',
            Body=BytesIO()
        )
        result = get_file_list(mock_parquet_bucket,
                               "nc-group3-transformation-bucket",
                               "payment",
                               "20231003150000")
        test_expected = ['payment/payment20231103150000.parquet',
                         'payment/payment20241103150000.parquet']
        assert result == test_expected

    def test_returns_empty_list_if_no_new_files_compared_to_timestamp(
            self,
            mock_parquet_bucket
    ):
        """Tests that the get_file_list function is able to return
        all the files if a passed timestamp is older than all file
        names"""
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='payment/payment20221103150000.parquet',
            Body=BytesIO()
        )
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='payment/payment20231103150000.parquet',
            Body=BytesIO()
        )
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='payment/payment20241103150000.parquet',
            Body=BytesIO()
        )
        result = get_file_list(mock_parquet_bucket,
                               "nc-group3-transformation-bucket",
                               "payment",
                               "20251003150000")
        test_expected = []
        assert result == test_expected

    def test_returns_empty_when_no_files_found_matching_prefix(
        self,
        mock_parquet_bucket
    ):
        '''Tests that an empty list is returned when
        there are no new files.'''
        result = get_file_list(mock_parquet_bucket,
                               "nc-group3-transformation-bucket",
                               "payment",
                               "10001003150000")
        assert result == []


class TestReadParquetFunction():
    def test_parquet_is_read_from_s3(self, mock_parquet_bucket):
        '''Tests that get_parquet_data func can retrieve
        parquet data from s3 bucket and convert to dataframe'''
        test_dataframe = currency_dataframe()
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='payment/payment20221103150000.parquet',
            Body=BytesIO(test_dataframe.to_parquet())
        )
        result = get_parquet_data(mock_parquet_bucket,
                                  "nc-group3-transformation-bucket",
                                  'payment/payment20221103150000.parquet')
        assert isinstance(result, pd.DataFrame)
        assert result.equals(test_dataframe)


class TestCompileTransformedParquetData():
    def test_returns_merged_dataframe_from_all_newest_parquet_files(
            self,
            mock_parquet_bucket
    ):
        '''Tests that all new parquet data files are merged into one
        dataframe ready for insertion if there are new files'''
        test_dataframe1 = dim_location_dataframe1()
        test_dataframe2 = dim_location_dataframe2()
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='location/location20221103150000.parquet',
            Body=BytesIO(test_dataframe1.to_parquet())
        )
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='location/location20221130150000.parquet',
            Body=BytesIO(test_dataframe2.to_parquet())
        )
        result = compile_parquet_data(mock_parquet_bucket,
                                      "nc-group3-transformation-bucket",
                                      "location",
                                      "20221100150000")
        test_expected = pd.DataFrame(data={
            'location_id': [1, 2, 3, 4, 5],
            'address_line_1': ['Herzog Via',
                               'Alexie Cliffs',
                               'Sincere Fort',
                               'Daniel Daniels',
                               'David Davids'],
            'address_line_2': ['None', 'None', 'None', 'None', 'None'],
            'district': ['Avon1', 'Avon', 'Avon', 'Avon1', 'Babylon'],
            'city': ['New Patienceburgh1',
                     'New Patienceburgh',
                     'New Patienceburgh',
                     'New Patienceburgh1',
                     'Luton'],
            'postal_code': ['28441', '28441', '28441', '28441', '12345'],
            'country': ['Turkey1',
                        'Turkey',
                        'Turkey',
                        'Brazil',
                        'England'],
            'phone': ['1803 637401',
                      '1803 637401',
                      '1803 637401',
                      '1803 637401',
                      '1234 567890']
        })
        assert isinstance(result, pd.DataFrame)
        assert result.equals(test_expected)

    def test_returns_empty_array_if_no_new_files(
            self,
            mock_parquet_bucket
    ):
        '''Test that function returns an empty array if no
        new parquet files in bucket as would cause ValueError if not
        accounted for'''
        new_timestamp = '20231100161000'
        test_dataframe1 = dim_location_dataframe1()
        test_dataframe2 = dim_location_dataframe2()
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='location/location20221101153000.parquet',
            Body=BytesIO(test_dataframe1.to_parquet())
        )
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='location/location20221101154000.parquet',
            Body=BytesIO(test_dataframe2.to_parquet())
        )
        result = compile_parquet_data(mock_parquet_bucket,
                                      "nc-group3-transformation-bucket",
                                      "location",
                                      new_timestamp)
        assert result.equals(pd.DataFrame())

    def test_returns_correct_dataframe_if_file_empty(
            self,
            mock_parquet_bucket
    ):
        '''Testing that function will still work and return correct
        dataframe if a returned parquet file happened to be empty'''
        test_dataframe1 = dim_location_dataframe1()
        test_dataframe2 = dim_location_dataframe2()
        empty_dataframe = pd.DataFrame(data={})
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='location/location20221101153000.parquet',
            Body=BytesIO(test_dataframe1.to_parquet())
        )
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='location/location20221101154000.parquet',
            Body=BytesIO(test_dataframe2.to_parquet())
        )
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='location/location20231101154000.parquet',
            Body=BytesIO(empty_dataframe.to_parquet())
        )
        result = compile_parquet_data(mock_parquet_bucket,
                                      "nc-group3-transformation-bucket",
                                      "location",
                                      "20221001154000")
        test_expected = pd.DataFrame(data={
            'location_id': [1, 2, 3, 4, 5],
            'address_line_1': ['Herzog Via',
                               'Alexie Cliffs',
                               'Sincere Fort',
                               'Daniel Daniels',
                               'David Davids'],
            'address_line_2': ['None', 'None', 'None', 'None', 'None'],
            'district': ['Avon1', 'Avon', 'Avon', 'Avon1', 'Babylon'],
            'city': ['New Patienceburgh1',
                     'New Patienceburgh',
                     'New Patienceburgh',
                     'New Patienceburgh1',
                     'Luton'],
            'postal_code': ['28441', '28441', '28441', '28441', '12345'],
            'country': ['Turkey1',
                        'Turkey',
                        'Turkey',
                        'Brazil',
                        'England'],
            'phone': ['1803 637401',
                      '1803 637401',
                      '1803 637401',
                      '1803 637401',
                      '1234 567890']
        })
        assert result.equals(test_expected)
