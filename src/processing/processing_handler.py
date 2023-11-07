import boto3

from read_write_files import (
    read_csv,
    get_csv_data,
    write_to_bucket
)
# from dim_table_transformation import (
#     dim_remove_dates,
#     dim_join_department
# )


def get_client():
    s3 = boto3.client("s3", region_name="eu-west-2")
    return s3


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
