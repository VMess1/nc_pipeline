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


def dim_join_department(staff_data, departments_data, timestamp):
    result = pd.merge(staff_data, departments_data, on="department_id")

    # new_df = staff_data.set_index('department_id').join(
    # departments_data.set_index('department_id'))
    return result


def get_client():
    s3 = boto3.client("s3", region_name="eu-west-2")
    return s3


def write_to_bucket(client, table_name, df, timestamp):
    file_key = table_name + '/' + table_name + timestamp
    parquet = df.to_parquet(
        f's3://nc-group3-transformation-bucket/{df}/{df}{timestamp}.parquet',
        engine='auto',
        compression='snappy',
        index=None,
        partition_cols=None)
    client.put_object(
        Bucket="nc-group3-transformation-bucket", Key=file_key, Body=parquet)


def main(event, context):
    # tables = ['design',
    #           'staff',
    #           'counterparty',
    #           'currency',
    #           'address',
    #           'sales_order',
    #           'departments'
    #           ]
    for table_name in event['table_list']:
        time_stamp = event['Timestamp']
        table_data = get_csv_data(table_name, time_stamp)
        df = read_csv(table_data['Body'])
        if table_name == 'currency':
            # dim_currency = dim_remove_dates(df, table_name)
            s3 = get_client()
            write_to_bucket(s3, table_name, df, time_stamp)
        # elif table_name == 'design':
        #     dim_design = dim_remove_dates(df)
            # push_to_bucket()
        # elif table_name == 'department':
        #     staff_table_data = get_csv_data('staff', time_stamp)
        #     dim_staff = dim_join_department(staff_table_data)
