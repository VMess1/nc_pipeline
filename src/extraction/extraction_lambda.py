# import boto3
# import json
# from botocore.exceptions import ClientError
# from pg8000.native import Connection, InterfaceError, DatabaseError
# from datetime import datetime

# '''
# EXTRACTION LAMBDA:
# Triggered by eventbridge, new SQL data is extracted
# and pushed to s3 bucket in CSV format.
# '''

# """Return AWS secret for oltp database credentials"""


# def get_credentials(secret_name):
#     '''Get connection based on credentials obtained'''

#     try:
#         client = boto3.client("secretsmanager", region_name="eu-west-2")
#         response = client.get_secret_value(SecretId=secret_name)
#         return json.loads(response["SecretString"])
#     except ClientError as err:
#         if err.response["Error"]["Code"] == "ResourceNotFoundException":
#             print("Credentials not found.")
#             return err.response
#         if err.response["Error"]["Code"] == "InternalServiceError":
#             print("Internal service error detected.")
#             return err.response
#     except Exception as err:
#         print("An unexpected error has occurred.")
#         return err


# def convert_to_csv(table_name, data, headers):
#     '''converts returned sql data to csv string'''
#     the_goods = ''
#     the_goods += table_name + '\n'
#     for index, collumn in enumerate(headers):
#         if index == len(headers) - 1:
#             the_goods += f'{collumn[0]}\n'
#         else:
#             the_goods += f'{collumn[0]}, '
#     for index, datum in enumerate(data):
#         for index, dat in enumerate(datum):
#             if index == len(datum) - 1:
#                 the_goods += f'{dat}\n'
#             else:
#                 the_goods += f'{dat}, '
#     return the_goods


# def upload_to_s3(csv_string):
#     '''uploads csv string to s3 ingestion bucket'''
#     try:

#     except ClientError as err:
#         print(err.response["Error"]["Code"])
#         if err.response["Error"]["Code"] == "NoSuchBucket":
#             print("Bucket not found.")
#             return err.response["Error"]["Message"]
#         if err.response["Error"]["Code"] == "InternalServiceError":
#             print("Internal service error detected.")
#             return err.response
#     except TypeError as err:
#         return f'Incorrect parameter type: {err}'
#     except Exception as err:
#         print(f"An unexpected error has occurred: {str(err)}")
#         return err


"""
Obtains credentials, establishes connection, gets timestamp of last extraction.
Obtains tables of database. For each table, gets column names and new data.
If there is new data, timestamp is updated to ssm, data is converted to csv
and uploaded to s3.
"""


# def main():
#     credentials = get_credentials("OLTPCredentials")
#     con = get_con(credentials)
#     last_extraction = get_last_timestamp('last_extraction')
#     table_names = get_tables(con)
#     for table_name in table_names:
#         if table_name[0][0] != '_':
#             data = select_table(con, table_name[0], last_extraction)
#             if len(data) > 0:
#                 print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#                 write_current_timestamp('last_extraction', datetime.now())
#                 headers = select_table_headers(con, table_name[0])
#                 csv = convert_to_csv(table_name[0], data, headers)
#                 upload_to_s3(csv)


# main()
