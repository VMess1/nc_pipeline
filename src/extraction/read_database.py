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


def get_tables(con):
    """returns table names"""
    
    query = """SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
              WHERE table_schema='public' AND TABLE_TYPE = 'BASE TABLE';"""
    return con.run(query)


def select_table_headers(con, table_name):
    """returns column names from a table"""

    query = f"""select column_name from INFORMATION_SCHEMA.COLUMNS where
     table_name = '{table_name}' ORDER BY ORDINAL_POSITION"""
    data = con.run(query)
    return data


def select_table(con, table_name, last_extraction):
    """returns all rows from the named table that have been updated"""
    
    query = f"""SELECT * FROM {table_name}
    WHERE last_updated > TIMESTAMP '{last_extraction}';"""
    data = con.run(query)
    return data
