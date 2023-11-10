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
    create_insert_statement,
    run_insert_query
)
from tests.test_storage.seed_data import (
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


class TestCreateInsertStatement:
    def test_returns_correct_insert_statement_for_multirow_table(self):
        test_input = pd.DataFrame(data={
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
        output = create_insert_statement('dim_location', test_input)
        test_expected = (
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
        assert output == test_expected


class TestRunInsertQuery:
    def test_updates_dim_table_with_new_records(self, seeded_connection):
        test_query = (
            "INSERT INTO test_dim_location \n"
            "(location_id, address_line_1, address_line_2, district, "
            "city, postal_code, country, phone) \n" "VALUES \n"
            "(4, 'street_4', 'place_4', 'district_4', 'city_4', 'D444DD', "
            "'country_4', '1803 637401'),\n"
            "(5, 'street_5', NULL, 'district_5', 'city_4', 'E555EE', "
            "'country_5', '1803 637401');\n"
        )
        run_insert_query(seeded_connection, test_query)
        result = seeded_connection.run('SELECT * FROM test_dim_location;')
        print(result)
