from pg8000.native import Connection, InterfaceError, DatabaseError
import os
from dotenv import load_dotenv
import csv
from pprint import pprint

load_dotenv()

# Database connection:
# To be changed to real database when appropriate

DB_DB = 'dummy_database'
DB_USER = os.environ['USER']
DB_HOST = 'localhost'
DB_PASSWORD = os.environ['PASSWORD']

# Connect to database:


def get_conn():
    return Connection(
        user=DB_USER,
        host=DB_HOST,
        database=DB_DB,
        password=DB_PASSWORD)

# Get list of tables:


def get_tables(conn):
    query = ("""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
              WHERE table_schema='public' AND TABLE_TYPE = 'BASE TABLE';""")
    return conn.run(query)

# Get collumn names:


def get_headers(conn, table_name):
    query = ("""SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
              WHERE TABLE_NAME = :table ORDER BY ORDINAL_POSITION;""")
    return conn.run(query, table=table_name)

# Get content of database:


def select_table(conn, table_name):
    query = ("SELECT * FROM ")
    query += table_name + ';'
    return conn.run(query, table=table_name)


def main(table):
    try:
        table_name = table
        conn = get_conn()
        # table_list = get_tables(conn)
        # pprint(table_list)
        headers = (get_headers(conn, table_name))
        data = select_table(conn, table_name)
        # a new file is created for each table:
        filepath = 'src/extraction/sql_to_csv/database_' + table_name + '.csv'
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            # headers need to be un-nested by one level:
            header_list = []
            for head in headers:
                header_list.append(head[0])
            # write headers to csv:
            writer.writerow(header_list)
            # data is written one row at a time:
            for item in data:
                writer.writerow(item)
        csvfile.close()
    # error handling:
    except (InterfaceError, DatabaseError) as d:
        print(d)
    except Exception as e:
        print(e)
    finally:
        conn.close()


if __name__ == '__main__':
    conn = get_conn()
    tables = get_tables(conn)
    table_list = []
    for table in tables:
        table_list.append(table[0])
    for table in table_list:
        main(table)
