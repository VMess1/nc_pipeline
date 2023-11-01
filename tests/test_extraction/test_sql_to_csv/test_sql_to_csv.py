from src.extraction.sql2csv_convert import convert_to_csv
from pg8000.native import Connection
import os
from dotenv import load_dotenv


'''Before running:
Run setup_dummy_database.sql
EXPORT USER=**sql username**
EXPORT PASSWORD=**sql password**
'''

load_dotenv()
DB_DB = 'dummy_database'
DB_USER = os.environ['USER']
DB_HOST = 'localhost'
DB_PASSWORD = os.environ['PASSWORD']


def get_conn():
    return Connection(
        user=DB_USER,
        host=DB_HOST,
        database=DB_DB,
        password=DB_PASSWORD)


def get_tables(conn):
    query = ("""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
              WHERE table_schema='public' AND TABLE_TYPE = 'BASE TABLE';""")
    return conn.run(query)


def get_headers(conn, table_name):
    query = ("""SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
              WHERE TABLE_NAME = :table ORDER BY ORDINAL_POSITION;""")
    return conn.run(query, table=table_name)


def select_table(conn, table_name):
    query = ('SELECT * FROM ')
    query += table_name + ';'
    return conn.run(query, table=table_name)


def main(table):
    table_name = table
    conn = get_conn()
    headers = (get_headers(conn, table_name))
    data = select_table(conn, table_name)
    return (convert_to_csv(table_name, data, headers))


def test_returns__correct_string_for_csv():
    payment_result = 'payment\n'
    payment_result += 'payment_id, created_at, last_updated, transaction_id, '
    payment_result += 'counterparty_id, payment_amount, currency_id, '
    payment_result += 'payment_type_id, paid, payment_date, '
    payment_result += 'company_ac_number, counterparty_ac_number\n'
    payment_result += '2, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 2, 15, '
    payment_result += '552548.62, 2, 3, False, 2022-11-04, 67305075, '
    payment_result += '31622269\n'
    payment_result += '3, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 3, 18, '
    payment_result += '205952.22, 3, 1, False, 2022-11-03, 81718079, '
    payment_result += '47839086\n'
    payment_result += '5, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 5, 17, '
    payment_result += '57067.20, 2, 3, False, 2022-11-06, 66213052, '
    payment_result += '91659548\n'
    payment_result += '8, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 8, 2, '
    payment_result += '254007.12, 3, 3, False, 2022-11-05, 32948439, '
    payment_result += '90135525\n'
    payment_result += '16, 2022-11-03 14:20:52, 2022-11-03 14:20:52, 16, 15, '
    payment_result += '250459.52, 2, 1, False, 2022-11-05, 34445327, '
    payment_result += '71673373\n'

    assert main('payment') == payment_result
