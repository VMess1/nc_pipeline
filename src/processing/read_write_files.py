import pandas as pd
import boto3
from io import BytesIO


def get_csv_data(table, timestamp):
    client = boto3.client('s3', region_name="eu-west-2")
    response = client.get_object(
        Bucket="nc-group3-ingestion-bucket",
        Key=f'{table}/{timestamp}')
    return response


def read_csv(file_name):
    df = pd.read_csv(file_name)
    return (df)


def write_to_bucket(client, table_name, df, timestamp):
    file_key = table_name + '/' + table_name + timestamp + '.parquet'
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    out_buffer.seek(0)
    response = client.put_object(
        Bucket="nc-group3-transformation-bucket",
        Key=file_key,
        Body=out_buffer)
    return response
