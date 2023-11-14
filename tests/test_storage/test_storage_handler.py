from src.storage.storage_handler import (
    main)
from moto import mock_s3, mock_secretsmanager
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
def mock_parquet_bucket(aws_credentials):
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
        yield conn


class TestBasicFunctionRuns:
    @patch('src.storage.storage_handler.get_credentials')
    @patch('src.storage.storage_handler.get_con', return_value = seeded_connection)
    @patch('src.storage.storage_handler.get_s3_client', return_value = mock_parquet_bucket)
    @patch('src.storage.storage_handler.get_last_timestamp', return_value = datetime.datetime(2020,1,1,10,30,0))
    @patch('src.storage.storage_handler.write_current_timestamp')
    def test_dim_location_table_updates(self, mock_write, mock_time, mock_s3, mock_con, mock_creds, mock_parquet_bucket):
        test_df = pd.DataFrame(data={
            'location_id': [1, 2, 3],
            'address_line_1': ['street_1', 'street_2', 'street_3'],
            'address_line_2': ['place_1', 'None', 'None'],
            'district': ['district_1', 'district_2', 'district_3'],
            'city': ['city_1',
                     'city_2',
                     'city_3'],
            'postal_code': ['A111AA', 'B222BB', 'C333CC'],
            'country': ['country_1', 'country_2', 'country_3'],
            'phone': ['1803 637401', '1803 637401', '1803 637401']
        })
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='dim_location/dim_location2020110315000.parquet',
            Body=BytesIO(bytes(pd.DataFrame, encoding='utf-8'))
        )
        print(datetime.datetime(2020,1,1,10,30,0)) 
    


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