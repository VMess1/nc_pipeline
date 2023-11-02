import pprint
import boto3
import os
import json
from botocore.exceptions import ClientError
from pg8000.native import Connection, InterfaceError, DatabaseError
from src.extraction.sql2csv_convert import convert_to_csv
from datetime import datetime
from src.extraction.upload_to_s3 import upload_to_s3


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


def select_table(con, table_name, last_extraction):
    query = f"SELECT * FROM {table_name} WHERE last_updated > '{last_extraction}'"
    data = con.run(query)
    return data


def select_table_headers(con, table_name):
    query = f"select column_name from INFORMATION_SCHEMA.COLUMNS where table_name = '{table_name}' ORDER BY ORDINAL_POSITION"
    data = con.run(query)
    return data


def main():
    credentials = get_credentials("OLTPCredentials")
    con = get_con(credentials)
    data = select_table(con, "department", datetime(2021, 10, 10, 11, 30, 30))
    headers = select_table_headers(con, "department")
    csv = convert_to_csv("department", data, headers)
    return_message = upload_to_s3(csv)
    print(return_message)


main()


# con = get_con(credentials)
# pprint.pprint(select_table(con, "payment"))
# pprint.pprint(select_table_headers(con, "payment"))
