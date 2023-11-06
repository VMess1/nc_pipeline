import pandas as pd
import boto3


def get_csv_data(table, timestamp):
    client = boto3.client('s3', region_name="eu-west-2")
    response = client.get_object(
        Bucket="nc-group3-ingestion-bucket",
        Key=f'{table}/{timestamp}')
    return response


def read_csv(file_name):
    df = pd.read_csv(file_name)
    return (df)


def dim_remove_dates(data):
    data.drop('created_at', inplace=True, axis=1)
    data.drop('last_updated', inplace=True, axis=1)
    return data


def main(timestamp):
    tables = ['design',
              'staff',
              'counterparty',
              'currency',
              'address',
              'sales_order',
              'departments'
              ]
    for table in tables:
        table_data = get_csv_data(table, timestamp)
        read_csv(table_data['Body'])
