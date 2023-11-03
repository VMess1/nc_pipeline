from datetime import datetime 
from botocore.exceptions import ClientError
from pg8000.native import InterfaceError, DatabaseError
from src.extraction.access_database import (
    get_credentials, 
    get_con, 
    get_tables,
    select_table, 
    select_table_headers
)
from src.extraction.write_data import (
    convert_to_csv, 
    upload_to_s3
)
from src.extraction.store_timestamp import (
    get_last_timestamp, 
    write_current_timestamp
)

def lambda_handler(event, context):
    try:
        credentials = get_credentials("OLTPCredentials")
        con = get_con(credentials)
        last_extraction = get_last_timestamp('last_extraction')
        table_names = get_tables(con)
        for table_name in table_names:
            if table_name[0][0] != '_':
                data = select_table(con, table_name[0], last_extraction)
                if len(data) > 0:
                    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    write_current_timestamp('last_extraction', datetime.now())
                    headers = select_table_headers(con, table_name[0])
                    csv = convert_to_csv(table_name[0], data, headers)
                    upload_to_s3(csv)
    except ClientError as err:
        if err.response["Error"]["Code"] == "ResourceNotFoundException":
            # change to logger
            print("Credentials not found.")
            return err.response
        if err.response["Error"]["Code"] == "InternalServiceError":
            # change to logger
            print("Internal service error detected.")
            return err.response
        if err.response["Error"]["Code"] == "NoSuchBucket":
            # change to logger
            print("Bucket not found.")
            return err.response["Error"]["Message"]
        if err.response["Error"]["Code"] == "InternalServiceError":
            print("Internal service error detected.")
    except TypeError as err:
        return f'Incorrect parameter type: {err}'
    except Exception as err:
        # change to logger
        print(f"An unexpected error has occurred: {str(err)}")
        return err
