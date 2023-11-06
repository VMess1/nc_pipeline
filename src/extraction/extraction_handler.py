from datetime import datetime
from botocore.exceptions import ClientError
import logging
from src.extraction.access_database import (
    get_credentials,
    get_con,
    get_tables,
    select_table,
    select_table_headers,
)
from src.extraction.write_data import convert_to_csv, upload_to_s3
from src.extraction.store_timestamp import get_last_timestamp, write_current_timestamp

logger = logging.getLogger("LPY1Logger")
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        credentials = get_credentials("OLTPCredentials")
        con = get_con(credentials)
        last_extraction = get_last_timestamp("last_extraction")
        table_names = get_tables(con)
        for table_name in table_names:
            if table_name[0][0] != "_":
                data = select_table(con, table_name[0], last_extraction)
                if len(data) > 0:
                    logger.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    datestamp = datetime.now().replace(microsecond=0)
                    write_current_timestamp('last_extraction', datestamp)
                    headers = select_table_headers(con, table_name[0])
                    csv = convert_to_csv(table_name[0], data, headers)
                    upload_to_s3(csv)
    except ClientError as err:
        if err.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.error("Credentials not found.")
        if err.response["Error"]["Code"] == "InternalServiceError":
            logger.error("Internal service error detected.")
        if err.response["Error"]["Code"] == "NoSuchBucket":
            logger.error("Bucket not found.")
        if err.response["Error"]["Code"] == "InternalServiceError":
            logger.error("Internal service error detected.")
    except TypeError as err:
        logger.error(f"Incorrect parameter type: {err}")
    except Exception as err:
        logger.error(f"An unexpected error has occurred: {str(err)}")
        return err


lambda_handler({}, {})
