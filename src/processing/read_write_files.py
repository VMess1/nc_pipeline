import pandas as pd
import re
import logging
from io import BytesIO


logger = logging.getLogger("LPY2Logger")
logger.setLevel(logging.INFO)


def get_csv_data(client, target_bucket, filepath):
    '''Retrieves csv data from an S3 bucket and converts to dataframe'''
    response = client.get_object(
        Bucket=target_bucket,
        Key=filepath)
    df = pd.read_csv(response.get('Body'), sep=';')
    return df


def check_transformation_bucket(client, target_bucket):
    '''returns a list of directories in transformation bucket'''
    response = client.list_objects(Bucket=target_bucket)
    table_list = []
    if 'Contents' in response:
        response_list = [obj['Key'] for obj in response.get('Contents', [])]
        for i in response_list:
            table_list.append(i.split('/')[0])
    return table_list


def compile_full_csv_table(client, target_bucket, table_name):
    '''Compiles csv files into a dataframe and removes duplicate'''
    def extract_timestamp(filepath):
        timestamp = re.findall(r'\d{14,}', filepath)[0]
        return int(timestamp)
    response = client.list_objects(Bucket=target_bucket,
                                   Prefix=table_name)
    file_list = [obj['Key'] for obj in response.get('Contents', [])]
    logger.info(file_list)
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
    df.to_parquet(out_buffer, index=False,)
    out_buffer.seek(0)
    response = client.put_object(
        Bucket="nc-group3-transformation-bucket",
        Key=file_key,
        Body=out_buffer)
    return response
