from src.storage.storage_handler import (
    main)
from moto import mock_s3
import boto3
import pytest
import pandas as pd
import os
from io import BytesIO
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
    def test_payment_table_updates(self, mock_parquet_bucket):
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
        test_string = test_df.to_string()
        mock_parquet_bucket.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key='payment/payment20221103150000.parquet',
            Body=BytesIO(bytes(test_string, encoding='utf-8'))
        )
        expected = (
            'INSERT INTO dim_location \n'
            '(location_id, address_line_1, address_line_2, district, '
            'city, postal_code, country, phone) \n' 'VALUES \n'
            '(1, street_1, place_1, district_1, city_1, A111AA, '
            'country_1, 1803 637401)\n'
            '(2, street_2, None, district_2, city_2, B222BB, '
            'country_2, 1803 637401)\n'
            '(3, street_3, None, district_3, city_3, C333CC, '
            'country_3, 1803 637401)\n;'
        )
        actual = main(None, None)
        assert actual == expected
        

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
    # #   test_event = {'table_list': ['currency'], 'timestamp': 20221103150000}
    # #     with caplog.at_level(logging.INFO):
    # #         main(test_event, None)
    # #         expected = ""
    # #         assert expected in caplog.text
