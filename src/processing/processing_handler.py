import boto3
from botocore.exceptions import ClientError
import logging
import re
from src.processing.read_write_files import (
    get_csv_data,
    write_to_bucket
)
from src.processing.dim_table_transformation import (
    dim_remove_dates,
    dim_insert_currency_name
    #   dim_join_department
)

logger = logging.getLogger("LPY2Logger")
logger.setLevel(logging.INFO)


def get_client():
    s3 = boto3.client("s3", region_name="eu-west-2")
    return s3


def main(event, context):
    '''Docstring incoming'''

    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        filepath = event['Records'][0]['s3']['object']['key']
        table_name = filepath.split('/')[0]
        last_time_stamp = re.findall(r'\d{14,}', filepath)[0]
        if filepath[-4:] != '.csv':
            raise TypeError('File type is not csv.')
        s3 = get_client()
        df = get_csv_data(s3, bucket, filepath)

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
            # elif table_name == 'department':
            #     staff_table_data = get_csv_data('staff', time_stamp)
            #     dim_staff = dim_join_department(staff_table_data)
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
