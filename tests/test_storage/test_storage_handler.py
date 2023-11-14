from src.storage.storage_handler import (
    main)
from moto import mock_s3, mock_secretsmanager
from dotenv import load_dotenv
import boto3
import pytest
import os
import logging
from tests.test_storage.data.seed_data import (
    get_create_location_query,
    get_create_sales_query,
    get_seed_location_query,
    get_seed_sales_query
)
from pg8000.native import Connection
from unittest.mock import patch
from datetime import date, time
from decimal import Decimal


from tests.test_storage.data.main_dataframes import (
    dim_location_df0,
    dim_location_df1,
    dim_location_df2,
    dim_location_df3,
    fact_sales_order_df0,
    fact_sales_order_df1,
    fact_sales_order_df2,
    fact_sales_order_df3
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
def seeded_connection(test_connection):
    test_connection.run("DROP TABLE IF EXISTS fact_test_sales_order;")
    test_connection.run("DROP TABLE IF EXISTS dim_test_location;")
    test_connection.run(get_create_location_query())
    test_connection.run(get_create_sales_query())
    test_connection.run(get_seed_location_query())
    test_connection.run(get_seed_sales_query())
    return test_connection


@pytest.fixture(scope="function")
def test_connection():
    return Connection(
        user=os.environ["USER"],
        host="localhost",
        database=os.environ["TEST_DATA_WAREHOUSE"],
        password=os.environ["PASSWORD"],
    )


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
            Key=('dim_test_location/dim_test_location' +
                 f'2023101{index}113030.parquet'),
            Body=data[index].to_parquet())
    return mock_bucket


@pytest.fixture(scope='function')
def mock_bucket_filled(mock_bucket_location):
    data = [fact_sales_order_df0(), fact_sales_order_df1(),
            fact_sales_order_df2(), fact_sales_order_df3()]
    for index in range(4):
        mock_bucket_location.put_object(
            Bucket="nc-group3-transformation-bucket",
            Key=('fact_test_sales_order/fact_test_sales_order' +
                 f'2023101{index}113030.parquet'),
            Body=data[index].to_parquet())
    return mock_bucket_location


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
           return_value='20231011110000')
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
            'dim_test_date', 'dim_test_currency',
            'dim_test_design', 'dim_test_staff',
            'dim_test_location', 'dim_test_counterparty',
            'fact_test_sales_order'
        ]
        mock_s3.return_value = mock_bucket_location
        mock_con.return_value = seeded_connection
        main(None, None)
        result = seeded_connection.run("SELECT * FROM dim_test_location")
        test_expected = [
            [1, 'street_1', 'place_1', 'district_1', 'city_1',
             'A111AA', 'country_1', '1803 637401'],
            [2, 'street_2', None, 'district_2', 'city_2',
             'B222BB', 'country_2', '1803 637401'],
            [3, 'street_3', None, 'district_3', 'city_3',
             'C333CC', 'country_3', '1803 637401'],
            [5, 'street_5', 'place_5', 'district_5', 'city_5',
             '28445', 'country_5', '1803 637405'],
            [6, 'street_6', 'place_6', 'district_6', 'city_6',
             '28446', 'country_6', '1803 637406'],
            [7, 'street_7', 'place_7', 'district_7', 'city_7',
             '28447', 'country_7', '1803 637407'],
            [8, 'street_8', 'place_8', 'district_8', 'city_8',
             '28448', 'country_8', '1803 637408'],
            [4, 'street_9', 'place_9', 'district_9', 'city_9',
             '28449', 'country_9', '1803 637409']]
        assert result == test_expected

    @patch('src.storage.storage_handler.get_table_list')
    @patch('src.storage.storage_handler.get_credentials')
    @patch('src.storage.storage_handler.get_con')
    @patch('src.storage.storage_handler.get_s3_client')
    @patch('src.storage.storage_handler.get_last_timestamp',
           return_value='20231011110000')
    @patch('src.storage.storage_handler.write_current_timestamp')
    def test_fact_sales_order_table_updates(
        self,
            mock_write,
            mock_time,
            mock_s3,
            mock_con,
            mock_creds,
            mock_table_list,
            seeded_connection,
            mock_bucket_filled):
        mock_table_list.return_value = [
            'dim_test_date', 'dim_test_currency',
            'dim_test_design', 'dim_test_staff',
            'dim_test_location', 'dim_test_counterparty',
            'fact_test_sales_order'
        ]
        mock_s3.return_value = mock_bucket_filled
        mock_con.return_value = seeded_connection
        main(None, None)
        test_expected = [
            [1, 1, date(2023, 10, 10), time(11, 30, 30),
             date(2023, 10, 10), time(11, 30, 30), 10,
             Decimal('1.50'), 1],
            [2, 2, date(2023, 10, 10), time(11, 30, 30),
             date(2023, 10, 10), time(11, 30, 30), 20,
             Decimal('1.50'), 2],
            [3, 3, date(2023, 10, 10), time(11, 30, 30),
             date(2023, 10, 10), time(11, 30, 30), 30,
             Decimal('1.50'), 3],
            [4, 4, date(2023, 10, 14), time(11, 30, 30),
             date(2023, 10, 14), time(11, 30, 30), 40,
             Decimal('1.50'), 4],
            [5, 5, date(2023, 10, 15), time(11, 30, 30),
             date(2023, 10, 14), time(11, 30, 30), 50,
             Decimal('1.50'), 5],
            [6, 6, date(2023, 10, 16), time(11, 30, 30),
             date(2023, 10, 14), time(11, 30, 30), 60,
             Decimal('1.50'), 6],
            [7, 7, date(2023, 10, 17), time(11, 30, 30),
             date(2023, 10, 17), time(11, 30, 30), 70,
             Decimal('1.50'), 7],
            [8, 8, date(2023, 10, 18), time(11, 30, 30),
             date(2023, 10, 18), time(11, 30, 30), 80,
             Decimal('1.50'), 8],
            [9, 4, date(2023, 10, 14), time(11, 30, 30),
             date(2023, 10, 19), time(11, 30, 30), 140,
             Decimal('4.50'), 6]]
        result = seeded_connection.run("SELECT * FROM fact_test_sales_order")
        assert result == test_expected
