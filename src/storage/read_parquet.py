import pandas as pd
from io import BytesIO
import re


def get_file_list(client, target_bucket, table_name, last_timestamp):
    '''Retrieves the most recent parquet files from s3 bucket
    compared to the last_timestamp'''
    def extract_timestamp(filepath):
        timestamp = re.findall(r'\d{14,}', filepath)[0]
        return int(timestamp)
    response = client.list_objects(Bucket=target_bucket,
                                   Prefix=f'{table_name}/')
    contents = response.get('Contents', [])
    if contents:
        file_list = [obj['Key'] for obj in response['Contents']]
        file_list.sort(key=extract_timestamp)
        last_timestamp_int = int(last_timestamp.replace('-', '')
                                 .replace(':', '').replace(' ', ''))
        newest_files = [file for file in file_list if
                        extract_timestamp(file) > last_timestamp_int]
        return newest_files
    else:
        return []


def get_parquet_data(client, target_bucket, filepath):
    '''Retrieves parquet data from an S3 bucket and converts to dataframe'''
    response = client.get_object(
        Bucket=target_bucket,
        Key=filepath)
    return pd.read_parquet(BytesIO(response['Body'].read()))


def compile_parquet_data(client, target_bucket, table_name, timestamp):
    '''Compiles parquet files, if there are new ones, into a dataframe
    or returns an empty list if no new files'''
    file_list = get_file_list(client, target_bucket, table_name, timestamp)
    data_rows = []
    if file_list:
        for filepath in file_list:
            data = get_parquet_data(client, target_bucket, filepath)
            if not data.empty:
                data_rows.append(data)
        return pd.concat(data_rows, axis=0, ignore_index=True)
    else:
        return pd.DataFrame()
