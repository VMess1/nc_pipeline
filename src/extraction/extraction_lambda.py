import pg8000
import sys
import boto3
import os
import json
from botocore.exceptions import ClientError


def get_credentials(secret_name):
    '''Return AWS secret for oltp database credentials'''

    try:
        client = boto3.client("secretsmanager", region_name="eu-west-2")
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except ClientError as err:
        if err.response["Error"]["Code"] == 'ResourceNotFoundException':   
            print('Credentials not found.')
            return err.response
        if err.response["Error"]["Code"] == 'InternalServiceError':
            print('Internal service error detected.')
            return err.response
    except Exception as err:
        print('An unexpected error has occurred.')
        return err


# ENDPOINT = ""
# PORT = ""
# USER = ""
# REGION = "eu-west-2"
# DBNAME = ""

# # gets the credentials from .aws/credentials
# session = boto3.Session(profile_name="OLTPCredentials")
# client = session.client("rds")

# token = client.generate_db_auth_token(
#     DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION
# )

# try:
#     conn = psycopg2.connect(
#         host=ENDPOINT,
#         port=PORT,
#         database=DBNAME,
#         user=USER,
#         password=token,
#         sslrootcert="SSLCERTIFICATE",
#     )
#     cur = conn.cursor()
#     cur.execute("""SELECT now()""")
#     query_results = cur.fetchall()
#     print(query_results)
# except Exception as e:
#     print("Database connection failed due to {}".format(e))
