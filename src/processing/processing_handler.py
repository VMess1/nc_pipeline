import boto3
from botocore.exceptions import ClientError
import logging
import re
from src.processing.read_write_files import (
    get_csv_data,
    write_to_bucket,
    compile_full_csv_table,
    check_transformation_bucket
)
from src.processing.dim_table_transformation import (
    dim_remove_dates,
    dim_insert_currency_name,
    dim_join_department,
    join_address,
    dim_locationtf,
    dim_date_tf
)
from src.processing.fact_table_transformation import fact_sales_order_tf

logger = logging.getLogger("LPY2Logger")
logger.setLevel(logging.INFO)


def get_client():
    s3 = boto3.client("s3", region_name="eu-west-2")
    return s3


COUNT = 0


def main(event, context):
    '''Checks parquet bucket for directories.
    If dim_date is not a directory, the table is created.
    Based on table name, tables are transformed as required.
    Table is written to transformation bucket in parquet format.
    '''

    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        logger.info(bucket)
        filepath = event['Records'][0]['s3']['object']['key']
        logger.info(filepath)
        table_name = filepath.split('/')[0]
        logger.info(table_name)
        last_time_stamp = re.findall(r'\d{14,}', filepath)[0]
        logger.info(last_time_stamp)
        if filepath[-4:] != '.csv':
            raise TypeError('File type is not csv.')
        s3 = get_client()
        df = get_csv_data(s3, bucket, filepath)
        test = str(df.head(5))
        logger.info(test)
        # parquet_table_list = []
        # parquet_table_list += check_transformation_bucket(
        #     s3, 'nc-group3-transformation-bucket')
        # if 'dim_date' not in parquet_table_list:
        #     new_table_name = 'dim_date'
        #     dim_date = dim_date_tf()
        #     write_to_bucket(s3, new_table_name, dim_date, last_time_stamp)
        #     global COUNT
        #     COUNT += 1

        if table_name == 'currency':
            new_table_name = 'dim_currency'
            dim_currency = dim_remove_dates(df)
            dim_currency = dim_insert_currency_name(dim_currency)
            write_to_bucket(s3, new_table_name, dim_currency, last_time_stamp)
            if 'Invalid' in dim_currency['currency_name'].tolist():
                logger.warning('Invalid currency code detected in file: ' +
                               f'{filepath}')
        elif table_name == 'design':
            new_table_name = 'dim_design'
            dim_design = dim_remove_dates(df)
            write_to_bucket(s3, new_table_name, dim_design, last_time_stamp)
        elif table_name == 'staff':
            new_table_name = 'dim_staff'
            logger.info("hello")
            department_df = compile_full_csv_table(
                s3, 'nc-group3-ingestion-bucket', 'department')
            test = str(department_df.head(5))
            logger.info("second hello??", test)  # <<<< We don't get this far
            dim_staff = dim_join_department(df, department_df)
            test = str(dim_staff.head(5))
            logger.info("please be a hello??", test)
            write_to_bucket(s3, new_table_name, dim_staff, last_time_stamp)
        elif table_name == 'counterparty':
            new_table_name = 'dim_counterparty'
            address_df = compile_full_csv_table(
                s3, 'nc-group3-ingestion-bucket', 'address')
            dim_counterparty = join_address(df, address_df)
            write_to_bucket(
                s3,
                new_table_name,
                dim_counterparty,
                last_time_stamp)
        elif table_name == 'address':
            new_table_name == 'dim_location'
            dim_location = dim_locationtf(df)
            write_to_bucket(s3, new_table_name, dim_location, last_time_stamp)
        elif table_name == 'sales_order':
            new_table_name == 'fact_sales_order'
            fact_sales_order = fact_sales_order_tf(df)
            write_to_bucket(
                s3,
                new_table_name,
                fact_sales_order,
                last_time_stamp)
        else:
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
