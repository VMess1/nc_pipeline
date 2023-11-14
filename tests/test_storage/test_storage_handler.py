from src.storage.storage_handler import (
    main)
from moto import mock_s3, mock_secretsmanager
from dotenv import load_dotenv
import boto3
import pytest
import pandas as pd
import os
from io import BytesIO
import logging
from tests.test_storage.data.seed_data import (
    get_create_location_query,
    get_create_sales_query,
    get_seed_location_query,
    get_seed_sales_query
)
from pg8000.native import Connection
from unittest.mock import patch
import datetime
# from botocore.exceptions import ClientError

from tests.test_storage.data.main_dataframes import (
    dim_location_df0,
    dim_location_df1,
    dim_location_df2,
    dim_location_df3
)


load_dotenv()

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


@pytest.fixture(scope="function")
def test_connection():
    return Connection(
        user=os.environ["USER"],
        host="localhost",
        database=os.environ["TEST_DATA_WAREHOUSE"],
        password=os.environ["PASSWORD"],
    )


@pytest.fixture(scope="function")
def seeded_connection(test_connection):
    test_connection.run("DROP TABLE IF EXISTS fact_test_sales_order;")
    test_connection.run("DROP TABLE IF EXISTS dim_test_location;")
    test_connection.run(get_create_location_query())
    test_connection.run(get_create_sales_query())
    test_connection.run(get_seed_location_query())
    test_connection.run(get_seed_sales_query())
    return test_connection


@pytest.fixture(scope="function")
def secrets(aws_credentials):
    with mock_secretsmanager():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


@pytest.fixture(scope='function')
def mock_bucket(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group3-transformation-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield conn


@pytest.fixture(scope='function')
def mock_bucket_location(mock_bucket):
    data = [dim_location_df0(), dim_location_df1(),
            dim_location_df2(), dim_location_df3()]
    for index in range(4):
        mock_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key=f'dim_test_location/dim_test_location202{index}1103150000.parquet',
            Body=data[index].to_parquet())
    return mock_bucket


@pytest.fixture(scope='function')
def mock_missing_parquet_bucket(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="eu-west-2")
        yield conn


class TestBasicFunctionRuns:
    @patch('src.storage.storage_handler.get_table_list')
    @patch('src.storage.storage_handler.get_credentials')
    @patch('src.storage.storage_handler.get_con')
    @patch('src.storage.storage_handler.get_s3_client')
    @patch('src.storage.storage_handler.get_last_timestamp',
           return_value='20210101010101')
    @patch('src.storage.storage_handler.write_current_timestamp')
    def test_dim_location_table_updates(
            self,
            mock_write,
            mock_time,
            mock_s3,
            mock_con,
            mock_creds,
            mock_table_list,
            seeded_connection,
            mock_bucket_location):
        mock_table_list.return_value = [
            'dim_test_location', 'dim_test_date', 'dim_test_currency',
            'dim_test_design', 'dim_test_staff',
            'dim_test_counterparty',
            'fact_test_sales_order'
        ]
        mock_s3.return_value = mock_bucket_location
        mock_con.return_value = seeded_connection
        main(None, None)
        result = seeded_connection.run("SELECT * FROM dim_test_location")
        test_expected = [[1, 'street_1', 'place_1', 'district_1', 'city_1', 'A111AA',
                          'country_1', '1803 637401'],
                         [2, 'street_2', None, 'district_2', 'city_2', 'B222BB',
                          'country_2', '1803 637401'],
                         [3, 'street_3', None, 'district_3', 'city_3', 'C333CC',
                          'country_3', '1803 637401'],
                         [5, 'street_5', 'place_5', 'district_5', 'city_5', '28445',
                          'country_5', '1803 637405'],
                         [6, 'street_6', 'place_6', 'district_6', 'city_6', '28446',
                          'country_6', '1803 637406'],
                         [7, 'street_7', 'place_7', 'district_7', 'city_7', '28447',
                          'country_7', '1803 637407'],
                         [8, 'street_8', 'place_8', 'district_8', 'city_8', '28448',
                          'country_8', '1803 637408'],
                         [4, 'street_9', 'place_9', 'district_9', 'city_9', '28449',
                          'country_9', '1803 637409']]
        assert result == test_expected

# class TestErrorHandling:

#     def test_transformation_bucket_not_found(
#             self, mock_missing_parquet_bucket,
#             monkeypatch, caplog
#     ):
#         def mock_get():
#             return mock_missing_parquet_bucket
#         monkeypatch.setattr(
#             'src.storage.storage_handler.get_client',
#             mock_get
#         )
#         with caplog.at_level(logging.INFO):
#             main(None, None)
#             expected = "Bucket not found: nc-group3-transformation-bucket"
#             assert expected in caplog.text

    # def test_handler_logs_internal_service_errors(
    #         self, mock_buckets,
    #         monkeypatch, caplog
    # ):
    #     def mock_get():
    #         response = {"Error": {"Code": "InternalServiceError"}}
    #         error = ClientError(response, 'test')
    #         raise error
    #     monkeypatch.setattr(
    #         'src.processing.processing_handler.get_client',
    #         mock_get
    #     )
    #     test_event = {'Records': [{
    #         's3': {
    #             'bucket': {'name': 'nc-group3-ingestion-bucket'},
    #             'object': {'key': 'currency/currency20221103150000.csv'}
    #         }
    #     }]}
    #     with caplog.at_level(logging.INFO):
    #         main(test_event, None)
    #         expected = "Internal service error detected."
    #         assert expected in caplog.text

    # def test_handler_logs_error_for_incorrect_file_type(
    #         self, mock_buckets,
    #         monkeypatch, caplog
    # ):
    #     def mock_get():
    #         return mock_buckets
    #     monkeypatch.setattr(
    #         'src.processing.processing_handler.get_client',
    #         mock_get
    #     )
    #     test_event = {'Records': [{
    #         's3': {
    #             'bucket': {'name': 'nc-group3-ingestion-bucket'},
    #             'object': {'key': 'currency/currency20221103150000.txt'}
    #         }
    #     }]}
    #     with caplog.at_level(logging.INFO):
    #         main(test_event, None)
    #         expected = "Incorrect parameter type: File type is not csv."
    #         assert expected in caplog.text

    # # def test_file_not_found_error_returned_for_missing_csv_file(
    # #         self, mock_buckets, monkeypatch, caplog):
    # #     def mock_get():
    # #         return mock_buckets
    # #     monkeypatch.setattr(
    # #         'src.processing.processing_handler.get_client',
    # #         mock_get)
    # #  test_event = {'table_list': ['currency'], 'timestamp': 20221103150000}
    # #     with caplog.at_level(logging.INFO):
    # #         main(test_event, None)
    # #         expected = ""
    # #         assert expected in caplog.text
