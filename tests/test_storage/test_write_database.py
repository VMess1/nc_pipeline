from moto import mock_secretsmanager
from dotenv import load_dotenv
import pandas as pd
import pytest
import boto3
import os
import json
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
    test_connection.run("DROP TABLE IF EXISTS test_fact_sales_order;")
    test_connection.run("DROP TABLE IF EXISTS test_dim_location;")
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
        run_insert_query(seeded_connection, 'test_dim_location', test_input)
        result = seeded_connection.run('SELECT * FROM test_dim_location;')
        assert result ==[
            [1, 'street_1', 'place_1', 'district_1', 'city_1', 'A111AA', 'country_1', '1803 637401'],
            [2, 'street_2', None, 'district_2', 'city_2', 'B222BB', 'country_2', '1803 637401'],
            [3, 'street_3', None, 'district_3', 'city_3', 'C333CC', 'country_3', '1803 637401'],
            [4, 'street_4', 'place_4', 'district_4', 'city_4', 'D444DD', 'country_4', '1803 637401'],
            [5, 'street_5', None, 'district_5', 'city_5', 'E555EE', 'country_5', '1803 637401']]
    
    def test_updates_dim_table_with_conflicting_records(self, seeded_connection):
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
        run_insert_query(seeded_connection, 'test_dim_location', test_input)
        result = seeded_connection.run('SELECT * FROM test_dim_location;')
        assert result ==[
            [1, 'street_1', 'place_1', 'district_1', 'city_1', 'A111AA', 'country_1', '1803 637401'],
            [3, 'street_3', None, 'district_3', 'city_3', 'C333CC', 'country_3', '1803 637401'],
            [2, 'street_4', 'place_4', 'district_4', 'city_4', 'D444DD', 'country_4', '1803 637401'],
            [5, 'street_5', None, 'district_5', 'city_5', 'E555EE', 'country_5', '1803 637401']]
