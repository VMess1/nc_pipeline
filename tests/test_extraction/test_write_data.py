from moto import mock_s3
from dotenv import load_dotenv
import pytest
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime
from pg8000.native import Connection
from src.extraction.read_database import select_table, select_table_headers
from src.extraction.write_data import convert_to_csv, upload_to_s3
from tests.test_extraction import strings

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
        database=os.environ["TEST_DATABASE"],
        password=os.environ["PASSWORD"],
    )


class TestSqlToCsv:
    def test_returns_correct_string_for_csv(self, test_connection):
        csv = (
            "department_id;department_name;location;"
            + "manager;created_at;last_updated\n"
            + "9;departmentname-9;location-9;manager-9;"
            + "2023-10-10 11:30:30;2025-10-10 11:30:30\n"
        )
        data = select_table(
            test_connection, "department", datetime(2024, 10, 10, 11, 30, 30)
        )
        headers = select_table_headers(test_connection, "department")
        result = convert_to_csv(data, headers)
        assert result == csv


class TestUploadToCsv:
    @mock_s3
    def test_s3_upload(self, test_connection):
        """
        Tests that upload_to_s3() can successfully connect to an s3 bucket
        and place a mock csv file into the bucket.
        """
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group3-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        test_datestamp = '2023-11-08 09:59:24'
        test_tablename = 'test_table'
        new_csv = strings.new_string()
        res = upload_to_s3(test_datestamp, new_csv, test_tablename)
        assert res == "file uploaded"

    @mock_s3
    def test_bucket_naming_errors_handled_correctly(self, test_connection):
        """
        Checks that any naming errors that occur during upload_to_s3()
        execution are handed correctly by the function.
        """
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group2-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        test_datestamp = '2023-11-08 09:59:24'
        test_tablename = 'test_table'
        new_csv = strings.new_string()
        with pytest.raises(ClientError) as excinfo:
            upload_to_s3(test_datestamp, new_csv, test_tablename)
        assert str(excinfo.value) == (
            "An error occurred (NoSuchBucket) when calling the PutObject "
            + "operation: The specified bucket does not exist"
        )

    @mock_s3
    def test_parameter_errors_handled_correctly(self, test_connection):
        """
        Tests that any parameter errors that occur during upload_to_s3()
        execution are handled correctly by the function.
        )
        """
        conn = boto3.client("s3", region_name="eu-west-2")
        conn.create_bucket(
            Bucket="nc-group3-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        test_datestamp = '2023-11-08 09:59:24'
        test_tablename = 'test_table'
        with pytest.raises(TypeError) as excinfo:
            upload_to_s3(test_datestamp, None, test_tablename)
        assert str(excinfo.value) == "Incorrect csv formatting."
