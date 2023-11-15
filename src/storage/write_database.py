import boto3
import json
from pg8000.native import Connection


def get_credentials(secret_name):
    """
    Takes OLAP credentials name and returns credentials
    from AWS Secrets Manager
    """
    client = boto3.client("secretsmanager", region_name="eu-west-2")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_con(credentials):
    """Takes OLAP credentials and returns connection
    to Data Warehouse"""

    return Connection(
        user=credentials["username"],
        host=credentials["host"],
        database=credentials["dbname"],
        password=credentials["password"],
    )


def remove_dim_duplicates(table_name, dataframe):
    '''
    Takes table name and data and returns a new
    dataframe, with any duplicates removed, ensuring
    that the latest duplicate is the one that is saved,
    in case of any updates.
    '''
    new_data = dataframe.copy()
    primary_key = dataframe.columns.tolist()[0]
    new_data = new_data.drop_duplicates(subset=[primary_key],
                                        keep='last',
                                        ignore_index=True)
    return new_data


def run_insert_query(client, table_name, dataframe):
    '''
    Takes data warehouse client, table name and data.
    Inserts the data into the SQL database table.
    '''
    entries = {}
    table_type = table_name.split('_')[0]
    if table_type == 'dim':
        dataframe = remove_dim_duplicates(
            table_name,
            dataframe
        )
    column_list = dataframe.columns.tolist()
    values_list = dataframe.values.tolist()
    insert_query = f'INSERT INTO {table_name} \n('
    values_query = 'VALUES \n'
    conflict_query = f'ON CONFLICT ({column_list[0]}) DO UPDATE SET \n'
    for index, column in enumerate(column_list):
        insert_query += column + ', '
        entries[f'column_{index}'] = column
        conflict_query += f'{column}=EXCLUDED.{column}, '
    insert_query = insert_query[:-2] + ')\n'
    conflict_query = conflict_query[:-2] + ';'
    count = 0
    for row in values_list:
        values_query += '('
        for value in row:
            values_query += f':value_{count}, '
            entries[f'value_{count}'] = value
            count += 1
        values_query = values_query[:-2] + '),\n'
    values_query = values_query[:-2] + '\n'
    insert_statement = insert_query + values_query
    if table_type == 'dim':
        insert_statement += conflict_query
    else:
        insert_statement += ';'
    client.run(insert_statement, **entries)
