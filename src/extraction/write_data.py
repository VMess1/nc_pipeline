import boto3
from datetime import datetime


def convert_to_csv(table_name, data, headers):
    """converts returned sql data to csv string"""
    the_goods = ""
    the_goods += table_name + "\n"
    for index, collumn in enumerate(headers):
        if index == len(headers) - 1:
            the_goods += f"{collumn[0]}\n"
        else:
            the_goods += f"{collumn[0]}, "
    for index, datum in enumerate(data):
        for index, dat in enumerate(datum):
            if index == len(datum) - 1:
                the_goods += f"{dat}\n"
            else:
                the_goods += f"{dat}, "
    return the_goods


def upload_to_s3(csv_string):
    """uploads csv string to s3 ingestion bucket"""
    if not isinstance(csv_string, str):
        raise TypeError("Incorrect csv formatting.")
    else:
        table_name = csv_string.split("\n")
        file_key = (
            table_name[0]
            + "/"
            + str(datetime.now().year)
            + str(datetime.now().strftime("%m"))
            + str(datetime.now().strftime("%d"))
            + str(datetime.now().strftime("%H"))
            + str(datetime.now().strftime("%M"))
            + str(datetime.now().strftime("%S"))
            + ".csv"
        )
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.put_object(
            Bucket="nc-group3-ingestion-bucket", Key=file_key, Body=csv_string
        )
        return "file uploaded"
