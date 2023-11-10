import boto3
from botocore.exceptions import ClientError
import logging
import re
from datetime import datetime

from src.storage.read_parquet import (get_file_list,
                                      get_parquet_data,
                                      compile_parquet_data)
from src.storage.store_timestamp import (get_last_timestamp,
                                         write_current_timestamp)
from src.storage.write_database import (get_credentials,
                                        get_con,
                                        create_insert_statement)

logger = logging.getLogger("LPY2Logger")
logger.setLevel(logging.INFO)


def get_client():
    s3 = boto3.client("s3", region_name="eu-west-2")
    return s3

def main(event, context):
    '''Cycles through all tables and updates warehouse when updates are found.'''
    current_time = datetime.now().replace(microsecond=0)
    credentials = get_credentials("OLAPCredentials")
    con = get_con(credentials)
    table_list = ['dim_date',
                  'dim_currency',
                  'dim_design',
                  'dim_staff',
                  'dim_location',
                  'dim_counterparty',
                  'fact_sales_order']
    client = get_client()
    timestamp = get_last_timestamp('last_insertion')
    target_bucket = 'nc-group3-transformation-bucket'
    for table_name in table_list:
        dataframe = compile_parquet_data(client, target_bucket, table_name, timestamp)
        insert_statement = create_insert_statement(table_name, dataframe)

    try:
        pass
    except ClientError as err:
        if err.response["Error"]["Code"] == "NoSuchKey":
            logger.error("No such key")
        if err.response["Error"]["Code"] == "InternalServiceError":
            logger.error("Internal service error detected.")
        if err.response["Error"]["Code"] == "NoSuchBucket":
            logger.error(
                f"Bucket not found: {err.response['Error']['BucketName']}"
            )
    except TypeError as err:
        logger.error(f"Incorrect parameter type: {err}")
    except Exception as err:
        logger.error(f"An unexpected error has occurred: {str(err)}")
        return err