import boto3
import json
from pg8000.native import Connection


def get_credentials(secret_name):
    """Get credentials from AWS Secrets Manager"""

    client = boto3.client("secretsmanager", region_name="eu-west-2")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_con(credentials):
    """Gets connection using credentials obtained"""

    return Connection(
        user=credentials["username"],
        host=credentials["host"],
        database=credentials["dbname"],
        password=credentials["password"],
    )