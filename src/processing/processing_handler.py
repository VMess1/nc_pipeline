import pandas as pd
import boto3


def get_credentials(table, timestamp):
    client = boto3.client('s3', region_name="eu-west-2")
    response = client.get_object(
        Bucket="nc-group3-ingestion-bucket",
        Key=f'{table}/{timestamp}')
    return response


def read_csv(file_name):
    df = pd.read_csv(file_name)
    return (df)


def main(timestamp):
    date_table = get_credentials('date', timestamp)
    read_csv(date_table)
