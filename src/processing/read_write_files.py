import pandas as pd
from io import BytesIO


def get_csv_data(client, filepath):
    response = client.get_object(
        Bucket="nc-group3-ingestion-bucket",
        Key=filepath)
    return response


def read_csv(file_name):
    df = pd.read_csv(file_name)
    return (df)


def write_to_bucket(client, table_name, df, timestamp):
    file_key = table_name + '/' + table_name + str(timestamp) + '.parquet'
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    out_buffer.seek(0)
    response = client.put_object(
        Bucket="nc-group3-transformation-bucket",
        Key=file_key,
        Body=out_buffer)
    return response
