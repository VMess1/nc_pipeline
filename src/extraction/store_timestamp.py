import boto3
from datetime import datetime


def get_last_timestamp(parameter_name):
    """Returns the last time at which the AWS lambda was triggered."""
    conn = boto3.client("ssm", region_name="eu-west-2")
    response = conn.get_parameter(Name=parameter_name)
    last_timestamp = response["Parameter"]["Value"]
    return last_timestamp[:-7]


def write_current_timestamp(parameter_name, current_time):
    """Writes the current time to AWS parameters"""
    formatted_time = current_time.isoformat(sep=" ", timespec="auto")
    conn = boto3.client("ssm", region_name="eu-west-2")
    response = conn.put_parameter(
        Name=parameter_name, Value=formatted_time, Type="String", Overwrite=True
    )
    return response
