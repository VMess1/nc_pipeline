import boto3
from botocore.exceptions import ClientError
from datetime import datetime

"""
Function to upload csv string into csv file in s3 bucket.
Function takes string as argument.
Function returns status code and message.
"""


def upload_to_s3(csv_string):
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


    try:
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.put_object(
            Bucket="nc-group3-ingestion-bucket", Key=file_key, Body=csv_string
        )
        return "file uploaded"
    except ClientError as err:
        print(err.response["Error"]["Code"])
        if err.response["Error"]["Code"] == "NoSuchBucket":
            print("Bucket not found.")
            return err.response["Error"]["Message"]
        if err.response["Error"]["Code"] == "InternalServiceError":
            print("Internal service error detected.")
            return err.response
    except Exception as err:
        print(f"An unexpected error has occurred: {str(err)}")
        return err
