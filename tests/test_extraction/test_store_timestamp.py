from moto import mock_ssm
import pytest
import boto3
import os
from datetime import datetime
from botocore.exceptions import ClientError
from src.extraction.store_timestamp import (
    get_last_timestamp, 
    write_current_timestamp
)

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def mock_params(aws_credentials):
    with mock_ssm():
        yield boto3.client("ssm", region_name="eu-west-2")


class TestGetLastTimestamp:
    def test_returns_value_if_parameter_found(self, mock_params):
        test_name = "Test-parameter"
        test_value = datetime(2023, 10, 10, 11, 30, 30)
        mock_params.put_parameter(
            Name=test_name,
            Value="2023-10-10 11:30:30",
            Overwrite=True,
        )
        assert get_last_timestamp(test_name) == test_value

    def test_raises_error_if_parameter_not_found(self, mock_params):
        test_name = "Test-parameter"
        with pytest.raises(ClientError) as excinfo:
            get_last_timestamp(test_name)
        assert str(excinfo.value) == (
            "An error occurred (ParameterNotFound) "
            + "when calling the GetParameter operation: "
            + f"Parameter {test_name} not found."
        )


class TestWriteCurrentTimestamp:
    def test_returns_correct_status_response_when_successful(
            self, mock_params
    ):
        test_name = "Test-parameter"
        test_value = datetime(2025, 10, 10, 11, 30, 30)
        response = write_current_timestamp(test_name, test_value)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        output = mock_params.get_parameter(
            Name=test_name
        )["Parameter"]["Value"]
        assert output == "2025-10-10 11:30:30"

    def test_overwrites_existing_parameter(self, mock_params):
        test_name = "Test-parameter"
        test_value_1 = datetime(2025, 10, 10, 11, 30, 30)
        test_value_2 = datetime(1999, 4, 10, 6, 30, 30)
        write_current_timestamp(test_name, test_value_1)
        response = write_current_timestamp(test_name, test_value_2)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        output = mock_params.get_parameter(
            Name=test_name
        )["Parameter"]["Value"]
        assert output == "1999-04-10 06:30:30"
