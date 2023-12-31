from datetime import datetime
from botocore.exceptions import ClientError
import logging
from src.extraction.read_database import (
    get_credentials,
    get_con,
    get_tables,
    select_table,
    select_table_headers,
)
from src.extraction.write_data import convert_to_csv, upload_to_s3
from src.extraction.store_timestamp import (get_last_timestamp,
                                            write_current_timestamp)

logger = logging.getLogger("LPY1Logger")
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Main function for extracting information from a DB:
    Connects to database to return data from each table inside
    the database, converts the data into a csv format and saves
    the data as .csv files inside a specified bucket, ready to
    be used by a transformation lambda for data manipulation.
    Handles errors that may arise during the extraction process.
    """
    try:
        credentials = get_credentials("OLTPCredentials")
        datestamp = datetime.now().replace(microsecond=0)
        con = get_con(credentials)
        last_extraction = get_last_timestamp("last_extraction")
        table_names = get_tables(con)
        for table_name in table_names:
            if table_name[0][0] != "_":
                data = select_table(con, table_name[0], last_extraction)
                if len(data) > 0:
                    logger.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    headers = select_table_headers(con, table_name[0])
                    csv = convert_to_csv(data, headers)
                    upload_to_s3(str(datestamp), csv, table_name[0])
        write_current_timestamp('last_extraction', datestamp)
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
