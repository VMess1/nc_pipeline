import pprint
import boto3
import os
import json
from botocore.exceptions import ClientError
from pg8000.native import Connection, InterfaceError, DatabaseError
from src.extraction.sql2csv_convert import convert_to_csv


def get_credentials(secret_name):
    """Return AWS secret for oltp database credentials"""

    try:
        client = boto3.client("secretsmanager", region_name="eu-west-2")
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except ClientError as err:
        if err.response["Error"]["Code"] == "ResourceNotFoundException":
            print("Credentials not found.")
            return err.response
        if err.response["Error"]["Code"] == "InternalServiceError":
            print("Internal service error detected.")
            return err.response
    except Exception as err:
        print("An unexpected error has occurred.")
        return err

def get_con(credentials):
    return Connection(
        user=credentials["username"],
        host=credentials["host"],
        database=credentials["dbname"],
        password=credentials["password"],
    )


def select_table(con, table_name):
    query = f"SELECT * FROM {table_name}"
    data = con.run(query)
    print(data)
    return data


def select_table_headers(con, table_name):
    query = f"select column_name from INFORMATION_SCHEMA.COLUMNS where table_name = '{table_name}' ORDER BY ORDINAL_POSITION"
    data = con.run(query)
    return data


def main():
    credentials = get_credentials("OLTPCredentials")
    con = get_con(credentials)
    data = select_table(con, "department")
    headers = select_table_headers(con, "department")
    csv = convert_to_csv("department", data, headers)


# con = get_con(credentials)
# pprint.pprint(select_table(con, "payment"))
# pprint.pprint(select_table_headers(con, "payment"))
