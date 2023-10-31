from pg8000.native import Connection, InterfaceError, DatabaseError
from dotenv import load_dotenv
import os
import pprint

load_dotenv()


def get_con():
    return Connection(
        user=os.environ["OLTP_USER"],
        host=os.environ["OLTP_HOST"],
        database=os.environ["OLTP_DATABASE"],
        password=os.environ["OLTP_PASSWORD"],
    )


def select_payment(con):
    query = "SELECT * FROM payment LIMIT 1"
    data = con.run(query)
    return data


con = get_con()
pprint.pprint(select_payment(con))
