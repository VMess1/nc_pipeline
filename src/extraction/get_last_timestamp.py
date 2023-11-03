import boto3
from botocore.exceptions import ClientError 

def get_last_timestamp(parameter_name):
    '''Returns the last time at which the AWS lambda was triggered.'''

    try:
        conn = boto3.client('ssm', region_name='eu-west-2')
        response = conn.get_parameter(
        Name=parameter_name
        )
        return response['Parameter']['Value']
    except ClientError as err:
        if err.response['Error']['Code'] == 'ParameterNotFound':
            return None

def write_last_timestamp():
    pass