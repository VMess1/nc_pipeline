from moto import mock_secretsmanager
from dotenv import load_dotenv
import pytest
import boto3
import os
from datetime import datetime
import json
from botocore.exceptions import ClientError
from src.extraction.extraction_lambda import (
    get_credentials,
    select_table,
    select_table_headers,
    convert_to_csv,
    upload_to_s3
)
from pg8000.native import Connection, InterfaceError, DatabaseError
from moto import mock_s3
from tests.test_extraction import strings

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

    def test_get_credentials_errors(self, secrets):
        secret_id = "test_secret"
        output = get_credentials(secret_id)
        assert output["Error"]["Code"] == "ResourceNotFoundException"
        assert output["ResponseMetadata"]["HTTPStatusCode"] == 404


class TestSelectFunctions:
    def test_select_table_returns_department_table_rows(self, test_connection):
        data = select_table(
            test_connection, "department", datetime(2022, 10, 10, 11, 30, 30)
        )
        assert data[0] == [
            1,
            "departmentname-1",
            "location-1",
            "manager-1",
            datetime(2023, 10, 10, 11, 30, 30),
            datetime(2023, 10, 10, 11, 30, 30),
        ]
        assert data[5] == [
            6,
            "departmentname-6",
            "location-6",
            "manager-6",
            datetime(2023, 10, 10, 11, 30, 30),
            datetime(2023, 10, 10, 11, 30, 30),
        ]

    def test_select_table_returns_department_table_only_rows_in_which_last_updated_is_greater_than_last_extraction(
        self, test_connection
    ):
        data = select_table(
            test_connection, "department", datetime(2024, 10, 10, 11, 30, 30)
        )
        assert data[0] == [
            9,
            "departmentname-9",
            "location-9",
            "manager-9",
            datetime(2023, 10, 10, 11, 30, 30),
            datetime(2025, 10, 10, 11, 30, 30),
        ]

    def test_select_table_returns_staff_table_rows(self, test_connection):
        data = select_table(
            test_connection, "staff", datetime(2022, 10, 10, 11, 30, 30)
        )
        assert data[0] == [
            1,
            "firstname-1",
            "lastname-1",
            1,
            "name-1@email.com",
            datetime(2023, 10, 10, 11, 30, 30),
            datetime(2023, 10, 10, 11, 30, 30),
        ]
        assert data[5] == [
            6,
            "firstname-6",
            "lastname-6",
            6,
            "name-6@email.com",
            datetime(2023, 10, 10, 11, 30, 30),
            datetime(2023, 10, 10, 11, 30, 30),
        ]

    def test_select_table_headers_returns_department_table_headers(
        self, test_connection
    ):
        data = select_table_headers(test_connection, "department")
        assert data[0][0] == "department_id"
        assert data[5][0] == "last_updated"

    def test_select_table_headers_returns_staff_table_headers(self, test_connection):
        data = select_table_headers(test_connection, "staff")
        assert data[0][0] == "staff_id"
        assert data[5][0] == "created_at"


class TestSqlToCsv:
    def test_returns_correct_string_for_csv(self, test_connection):
        csv = "department\ndepartment_id, department_name, location, manager, created_at, last_updated\n9, departmentname-9, location-9, manager-9, 2023-10-10 11:30:30, 2025-10-10 11:30:30\n"
        data = select_table(
                test_connection, "department", datetime(2024, 10, 10, 11, 30, 30)
            )
        headers = select_table_headers(test_connection, "department")
        result = convert_to_csv('department', data, headers)
        
        assert result == csv
       

class TestUploadToCsv:
    @mock_s3
    def test_s3_upload(self, test_connection):
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group3-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        new_csv = strings.difference_1()
        res = upload_to_s3(new_csv)
        assert res == "file uploaded"


    @mock_s3
    def test_bucket_naming_errors_handled_correctly(self, test_connection):
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group2-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        new_csv = strings.difference_1()
        res = upload_to_s3(new_csv)
        assert res == "The specified bucket does not exist"


    @mock_s3
    def test_parameter_errors_handled_correctly(self, test_connection):
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group3-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        res = upload_to_s3(None)
        assert 'Incorrect parameter type' in str(res)