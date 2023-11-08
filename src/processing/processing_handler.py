import boto3
from botocore.exceptions import ClientError
import logging
from src.processing.read_write_files import (
    read_csv,
    get_csv_data,
    write_to_bucket
)
from src.processing.dim_table_transformation import (
    dim_remove_dates,
    #   dim_join_department
)


logger = logging.getLogger("LPY1Logger")
logger.setLevel(logging.INFO)


def get_client():
    s3 = boto3.client("s3", region_name="eu-west-2")
    return s3


def main(event, context):
    # tables = ['design',
    #           'staff',
    #           'counterparty',
    #           'currency',
    #           'address',
    #           'sales_order',
    #           'departments'
    #           ]
    try:
        for table_name in event['table_list']:
            last_time_stamp = event['timestamp']
            s3 = get_client()
            table_data = get_csv_data(s3, table_name, last_time_stamp)
            df = read_csv(table_data['Body'])
            if table_name == 'currency':
                dim_currency = dim_remove_dates(df)
                write_to_bucket(s3, table_name, dim_currency, last_time_stamp)
            elif table_name == 'design':
                dim_design = dim_remove_dates(df)
                write_to_bucket(s3, table_name, dim_design, last_time_stamp)
            # elif table_name == 'department':
            #     staff_table_data = get_csv_data('staff', time_stamp)
            #     dim_staff = dim_join_department(staff_table_data)
    except ClientError as err:
        if err.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.error("Credentials not found.")
        if err.response["Error"]["Code"] == "InternalServiceError":
            logger.error("Internal service error detected.")
        if err.response["Error"]["Code"] == "NoSuchBucket":
            logger.error("Bucket not found.")
    except TypeError as err:
        logger.error(f"Incorrect parameter type: {err}")
    except Exception as err:
        logger.error(f"An unexpected error has occurred: {str(err)}")
        return err
