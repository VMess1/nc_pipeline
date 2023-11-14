import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime

from src.storage.read_parquet import (compile_parquet_data)
from src.storage.store_timestamp import (get_last_timestamp,
                                         write_current_timestamp)
from src.storage.write_database import (get_credentials,
                                        get_con,
                                        run_insert_query)

logger = logging.getLogger("LPY2Logger")
logger.setLevel(logging.INFO)


def get_s3_client():
    s3 = boto3.client("s3", region_name="eu-west-2")
    return s3


def get_table_list():
    return ['dim_date',
            'dim_currency',
            'dim_design',
            'dim_staff',
            'dim_location',
            'dim_counterparty',
            'fact_sales_order']


def main(event, context):
    '''Cycles through tables and updates warehouse when updates are found.'''
    try:
        table_list = get_table_list()
        current_time = datetime.now().replace(microsecond=0)
        credentials = get_credentials("OLAPCredentials")
        con_warehouse = get_con(credentials)
        s3client = get_s3_client()
        timestamp = get_last_timestamp('last_insertion')
        target_bucket = 'nc-group3-transformation-bucket'
        for table_name in table_list:
            dataframe = compile_parquet_data(
                s3client, target_bucket, table_name, timestamp)
            if not dataframe.empty:
                run_insert_query(con_warehouse, table_name, dataframe)
            else:
                logger.info(f'No updates made to {table_name}')
        write_current_timestamp('last_insertion', current_time)
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
