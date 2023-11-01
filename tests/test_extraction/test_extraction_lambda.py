from moto import mock_secretsmanager
from dotenv import load_dotenv
import pytest
import boto3
import os
import json
from botocore.exceptions import ClientError
from src.extraction.extraction_lambda import (
    get_credentials,
    select_table,
    select_table_headers,
)
from pg8000.native import Connection, InterfaceError, DatabaseError


load_dotenv()


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
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
        database=os.environ["TEST_DATABASE"],
        password=os.environ["PASSWORD"],
    )


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
        secrets.create_secret(Name=secret_id, SecretString=json.dumps(secret_values))
        output = get_credentials(secret_id)
        assert output == secret_values

    def test_get_credentials(self, secrets):
        secret_id = "test_secret"
        output = get_credentials(secret_id)
        assert output["Error"]["Code"] == "ResourceNotFoundException"
        assert output["ResponseMetadata"]["HTTPStatusCode"] == 404


class TestSelectFunctions:
    def test_select_table_returns_department_table_rows(self, test_connection):
        data = select_table(test_connection, "department")
        assert data[0] == [
            1,
            "departmentname-1",
            "location-1",
            "manager-1",
            "01-01-2015",
            "01-02-2015",
        ]
        assert data[5] == [
            6,
            "departmentname-6",
            "location-6",
            "manager-6",
            "06-01-2015",
            "06-02-2015",
        ]

    def test_select_table_returns_staff_table_rows(self, test_connection):
        data = select_table(test_connection, "staff")
        print(data)
        assert data[0] == [
            1,
            "firstname-1",
            "lastname-1",
            1,
            "name-1@email.com",
            "01-01-2020",
            "01-02-2020",
        ]
        assert data[5] == [
            6,
            "firstname-6",
            "lastname-6",
            6,
            "name-6@email.com",
            "06-01-2020",
            "06-02-2020",
        ]

    def test_select_table_headers_returns_department_table_headers(
        self, test_connection
    ):
        data = select_table_headers(test_connection, "department")
        assert data[0][0] == "department_id"
        assert data[5][0] == "last_updated"

    def test_select_table_headers_returns_staff_table_headers(self, test_connection):
        data = select_table_headers(test_connection, "staff")
        print(data)
        assert data[0][0] == "staff_id"
        assert data[5][0] == "created_at"
