from moto import mock_secretsmanager
from dotenv import load_dotenv
import pandas as pd
import pytest
import boto3
import os
import json
from datetime import date, time
from pg8000.native import Connection
from src.storage.write_database import (
    get_credentials,
    run_insert_query
)
from tests.test_storage.data.seed_data import (
    get_create_location_query,
    get_create_sales_query,
    get_seed_location_query,
    get_seed_sales_query
)

load_dotenv()


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


class TestGetCredentials:
    def test_get_credentials(self, secrets):
        secret_id = "test_secret"
        secret_values = {
            "engine": "postgres",
            "username": "test_user",
            "password": "test_password",
            "host": "test-database.us-west-2.rds.amazonaws.com",
            "dbname": "test-database",
            "port": "2222",
        }
        secrets.create_secret(Name=secret_id,
                              SecretString=json.dumps(secret_values))
        output = get_credentials(secret_id)
        assert output == secret_values


class TestRunInsertQuery:
    def test_updates_dim_table_with_new_records(self, seeded_connection):
        test_input = pd.DataFrame(data={
            'location_id': [4, 5],
            'address_line_1': ['street_4', 'street_5'],
            'address_line_2': ['place_4', None],
            'district': ['district_4', 'district_5'],
            'city': ['city_4',
                     'city_5'],
            'postal_code': ['D444DD', 'E555EE'],
            'country': ['country_4', 'country_5'],
            'phone': ['1803 637401', '1803 637401']
        })
        run_insert_query(seeded_connection, 'dim_test_location', test_input)
        result = seeded_connection.run('SELECT * FROM dim_test_location;')
        assert result == [
            [1, 'street_1', 'place_1', 'district_1', 'city_1',
             'A111AA', 'country_1', '1803 637401'],
            [2, 'street_2', None, 'district_2', 'city_2',
             'B222BB', 'country_2', '1803 637401'],
            [3, 'street_3', None, 'district_3', 'city_3',
             'C333CC', 'country_3', '1803 637401'],
            [4, 'street_4', 'place_4', 'district_4', 'city_4',
             'D444DD', 'country_4', '1803 637401'],
            [5, 'street_5', None, 'district_5', 'city_5',
             'E555EE', 'country_5', '1803 637401']]

    def test_updates_dim_table_with_conflicting_records(
            self, seeded_connection):
        test_input = pd.DataFrame(data={
            'location_id': [2, 5],
            'address_line_1': ['street_4', 'street_5'],
            'address_line_2': ['place_4', None],
            'district': ['district_4', 'district_5'],
            'city': ['city_4',
                     'city_5'],
            'postal_code': ['D444DD', 'E555EE'],
            'country': ['country_4', 'country_5'],
            'phone': ['1803 637401', '1803 637401']
        })
        run_insert_query(seeded_connection, 'dim_test_location', test_input)
        result = seeded_connection.run('SELECT * FROM dim_test_location;')
        assert result == [
            [1, 'street_1', 'place_1', 'district_1', 'city_1',
             'A111AA', 'country_1', '1803 637401'],
            [3, 'street_3', None, 'district_3', 'city_3',
             'C333CC', 'country_3', '1803 637401'],
            [2, 'street_4', 'place_4', 'district_4', 'city_4',
             'D444DD', 'country_4', '1803 637401'],
            [5, 'street_5', None, 'district_5', 'city_5',
             'E555EE', 'country_5', '1803 637401']]

    def test_updates_facts_table_with_new_records(self, seeded_connection):
        test_input = pd.DataFrame(data={
            'sales_order_id': [4, 5],
            'created_date': ['2024-10-10', '2025-10-10'],
            'created_time': ['11:30:30', '11:30:30'],
            'last_updated_date': ['2024-10-10', '2025-10-10'],
            'last_updated_time': ['11:30:30',
                                  '11:30:30'],
            'units_sold': [40, 50],
            'unit_price': [1.5, 1.5],
            'agreed_delivery_location': [1, 2]
        })
        print(date(2023, 10, 10))
        test_expected = [
            [1, 1, date(2023, 10, 10), time(11, 30, 30),
             date(2023, 10, 10), time(11, 30, 30), 10, 1.5, 1],
            [2, 2, date(2023, 10, 10), time(11, 30, 30),
             date(2023, 10, 10), time(11, 30, 30), 20, 1.5, 2],
            [3, 3, date(2023, 10, 10), time(11, 30, 30),
             date(2023, 10, 10), time(11, 30, 30), 30, 1.5, 3],
            [4, 4, date(2024, 10, 10), time(11, 30, 30),
             date(2024, 10, 10), time(11, 30, 30), 40, 1.5, 1],
            [5, 5, date(2025, 10, 10), time(11, 30, 30),
             date(2025, 10, 10), time(11, 30, 30), 50, 1.5, 2]
        ]
        run_insert_query(
            seeded_connection,
            'fact_test_sales_order',
            test_input)
        result = seeded_connection.run('SELECT * FROM fact_test_sales_order;')
        print(result)
        assert result == test_expected

    def test_updates_facts_table_with_modified_records(
            self, seeded_connection):
        test_input = pd.DataFrame(data={
            'sales_order_id': [2, 1],
            'created_date': ['2024-10-10', '2025-10-10'],
            'created_time': ['11:30:30', '11:30:30'],
            'last_updated_date': ['2024-10-10', '2025-10-10'],
            'last_updated_time': ['11:30:30',
                                  '11:30:30'],
            'units_sold': [25, 15],
            'unit_price': [2.5, 2.5],
            'agreed_delivery_location': [1, 2]
        })
        print(date(2023, 10, 10))
        test_expected = [
            [1, 1, date(2023, 10, 10), time(11, 30, 30),
             date(2023, 10, 10), time(11, 30, 30), 10, 1.5, 1],
            [2, 2, date(2023, 10, 10), time(11, 30, 30),
             date(2023, 10, 10), time(11, 30, 30), 20, 1.5, 2],
            [3, 3, date(2023, 10, 10), time(11, 30, 30),
             date(2023, 10, 10), time(11, 30, 30), 30, 1.5, 3],
            [4, 2, date(2024, 10, 10), time(11, 30, 30),
             date(2024, 10, 10), time(11, 30, 30), 25, 2.5, 1],
            [5, 1, date(2025, 10, 10), time(11, 30, 30),
             date(2025, 10, 10), time(11, 30, 30), 15, 2.5, 2]
        ]
        run_insert_query(
            seeded_connection,
            'fact_test_sales_order',
            test_input)
        result = seeded_connection.run('SELECT * FROM fact_test_sales_order;')
        print(result)
        assert result == test_expected
