from moto import mock_secretsmanager
import pytest
import boto3
import os
import json
from botocore.exceptions import ClientError
from src.extraction.extraction_lambda import get_credentials

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
   
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
            "port": "2222"  
        }
        secrets.create_secret(
            Name=secret_id, SecretString=json.dumps(secret_values)
        )
        output = get_credentials(secret_id)
        assert output == secret_values

    def test_get_credentials(self, secrets):
        secret_id = "test_secret"
        output = get_credentials(secret_id)
        assert output["Error"]["Code"] == "ResourceNotFoundException"
        assert output["ResponseMetadata"]["HTTPStatusCode"] == 404
        