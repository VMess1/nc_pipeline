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


def create_insert_statement(table_name, dataframe):
    column_list = dataframe.columns.tolist()

    def list_to_string(list): return (
        '(' + ', '.join([str(i) for i in list]) + ')')
    column_string = list_to_string(column_list)
    values = dataframe.values.tolist()
    values_list = [list_to_string(item) for item in values]
    insert = (
        f'INSERT INTO {table_name} \n'
        f'{column_string} \n'
        'VALUES \n'
    )
    for item in values_list:
        insert += item + '\n'

    return insert + ';'
