import boto3
import json
from pg8000.native import Connection


def get_credentials(secret_name):
    """
    Gets credentials from AWS Secrets Manager using
    a specified secret name which will need to be
    set up manually with a JSON object with your own
    credentials in order to access the inital OLTP database.
    Required saved structure for example is:
        {
            "username": "YOUR_USERNAME",
            "password": "YOUR_PASSWORD",
            "engine": "YOUR_DB_ENGINE",
            "port": "1234"
            "host": "YOUR_HOST",
            "dbname": "YOUR_DB_NAME"
        }
    Returns a python dictionary structure that is used in
    getting the connection to the database
    Credentials for this project include:
        - username
        - host
        - database name (as dbname)
        - password
    """
    client = boto3.client("secretsmanager", region_name="eu-west-2")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def get_con(credentials):
    """
    Gets the connection to a database using the returned
    credentials obtained.
    Uses pg8000 to establish the connection.
    Returns an instance of the pg8000 connection class.
    """
    return Connection(
        user=credentials["username"],
        host=credentials["host"],
        database=credentials["dbname"],
        password=credentials["password"],
    )


def get_tables(con):
    """
    Executes an SQL query using a passed connection parameter to
    your DB that was established using get_con and
    get_credentials. The specific query retrives all the
    table names that are in PostGresSQL DB to later
    retrieve the information from the tables

    Returns a nested list of lists with each table name in their
    own list inside the returned list.
    """
    query = """SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
              WHERE table_schema='public' AND TABLE_TYPE = 'BASE TABLE';"""
    return con.run(query)


def select_table(con, table_name, last_extraction):
    """
    Executes an SQL query using the parameters of a passed connection
    to the DB, a specified table name that you wish to obtain the data
    from, and the last_extraction parameter which is a string of a
    timestamp that has the time of a prior connection to the database.

    If this is the first time connecting to the specified table,
    the last_extraction parameter will be pre-populated with an old
    timestamp to be able to gather all data from the table.

    This will then only select new data that is put into the database
    from the previous extraction timestanp which is updated via another
    function.

    Returns a list of the all the information from the specified table
    """
    query = f"""SELECT * FROM {table_name}
    WHERE last_updated > TIMESTAMP '{last_extraction}';"""
    data = con.run(query)
    print(type(data))
    return data


def select_table_headers(con, table_name):
    """
    Retrieves all the names of the columns of a specified
    table from a PostGreSQL DB.
    Parameters include the established connection to the DB
    and the specific table name to return the 'headers' or 'names'
    of from that table.
    Returns a nested list of lists that contains the header
    name in each nested list.
    """
    query = f"""select column_name from INFORMATION_SCHEMA.COLUMNS where
     table_name = '{table_name}' ORDER BY ORDINAL_POSITION"""
    data = con.run(query)
    return data
