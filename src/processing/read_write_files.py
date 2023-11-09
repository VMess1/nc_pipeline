import pandas as pd
from io import BytesIO
import re


def get_csv_data(client, target_bucket, filepath):
    '''Retrieves csv data from an S3 bucket and converts to dataframe'''
    response = client.get_object(
        Bucket=target_bucket,
        Key=filepath)
    return pd.read_csv(response['Body'])


def compile_full_csv_table(client, target_bucket, table_name):
    '''Compiles csv files into a dataframe and removes duplicate'''
    def extract_timestamp(filepath):
        timestamp = re.findall(r'\d{14,}', filepath)[0]
        return int(timestamp)

    response = client.list_objects(Bucket=target_bucket,
                                   Prefix=f'{table_name}/')
    file_list = [obj['Key'] for obj in response['Contents']]
    file_list.sort(key=extract_timestamp)

    data_rows = []
    for filepath in file_list:
        data_rows.append(get_csv_data(client, target_bucket, filepath))
    data = pd.concat(data_rows, ignore_index=True)
    return data.drop_duplicates(subset=[f'{table_name}_id'],
                                keep='last',
                                ignore_index=True)


def write_to_bucket(client, table_name, df, timestamp):
    '''Writes dataframe to parquet format in an S3 bucket'''
    file_key = table_name + '/' + table_name + str(timestamp) + '.parquet'
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    out_buffer.seek(0)
    response = client.put_object(
        Bucket="nc-group3-transformation-bucket",
        Key=file_key,
        Body=out_buffer)
    return response
