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
'''uploads csv string to s3 ingestion bucket'''


def upload_to_s3(csv_string):
    try:
        if not isinstance(csv_string, str):
            raise TypeError
        else:
            table_name = csv_string.split("\n")
            file_key = (
                table_name[0]
                + "/"
                + str(datetime.now().year)
                + str(datetime.now().strftime('%m'))
                + str(datetime.now().strftime('%d'))
                + str(datetime.now().strftime('%H'))
                + str(datetime.now().strftime('%M'))
                + str(datetime.now().strftime('%S'))
                + ".csv"
            )
            s3 = boto3.client("s3", region_name="eu-west-2")
            s3.put_object(
                Bucket="nc-group3-ingestion-bucket",
                Key=file_key,
                Body=csv_string)
            return "file uploaded"
    except ClientError as err:
        print(err.response["Error"]["Code"])
        if err.response["Error"]["Code"] == "NoSuchBucket":
            print("Bucket not found.")
            return err.response["Error"]["Message"]
        if err.response["Error"]["Code"] == "InternalServiceError":
            print("Internal service error detected.")
            return err.response
    except TypeError as err:
        return f'Incorrect parameter type: {err}'
    except Exception as err:
        print(f"An unexpected error has occurred: {str(err)}")
        return err


'''Returns the last time at which the AWS lambda was triggered.'''


def get_last_timestamp(parameter_name):
    conn = boto3.client('ssm', region_name='eu-west-2')
    response = conn.get_parameter(
        Name=parameter_name
    )
    last_timestamp = response['Parameter']['Value']
    return last_timestamp[:-7]


'''Writes the current time to AWS parameters'''


def write_current_timestamp(parameter_name, current_time):
    formatted_time = current_time.isoformat(sep=' ', timespec='auto')
    conn = boto3.client('ssm', region_name='eu-west-2')
    response = conn.put_parameter(
        Name=parameter_name,
        Value=formatted_time,
        Type='String',
        Overwrite=True
    )
    return response


'''
Obtains credentials, establishes connection, gets timestamp of last extraction.
Obtains tables of database. For each table, gets column names and new data.
If there is new data, timestamp is updated to ssm, data is converted to csv
and uploaded to s3.
"""


def main():
    credentials = get_credentials("OLTPCredentials")
    con = get_con(credentials)
    last_extraction = get_last_timestamp('last_extraction')
    table_names = get_tables(con)
    for table_name in table_names:
        if table_name[0][0] != '_':
            data = select_table(con, table_name[0], last_extraction)
            if len(data) > 0:
                datestamp = datetime.now().replace(microsecond=0)
                write_current_timestamp('last_extraction', datestamp)
                headers = select_table_headers(con, table_name[0])
                csv = convert_to_csv(table_name[0], data, headers)
                upload_to_s3(csv)
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
