import pandas as pd
from io import BytesIO


def get_csv_data(client, target_bucket, filepath):
    '''Retrieves csv data from an S3 bucket and converts to dataframe'''
    response = client.get_object(
        Bucket=target_bucket,
        Key=filepath)
    return pd.read_csv(response['Body'])


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
