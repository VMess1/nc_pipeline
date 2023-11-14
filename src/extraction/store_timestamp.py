import boto3


"""
This file contains functions for retrieving and writing a
timestamp to aid with only retrieving the most recent updates
to your database.

You will need to modify the terraform/ssm.tf to set what you want
to call your parameter_name to save this timestamp and needs to
match the parameter_name used by these functions.

On deployment, the timestamp is set to the year 2020, via
terraform as no data from our DB was provided before this time,
this then ensures that on first deployment, all data will be extracted.
If your database contains data older than the year 2020, this
will need to be reset to match a time period that is before
any data was uploaded to your specified DB. This can be set
in the 'value' argument inside terraform/ssm.tf and needs to be
a string in the format of: "2020-11-08 14:52:35"
"""


def get_last_timestamp(parameter_name):
    """
    Takes a parameter_name (str) which will need to correlate with
    the specified name in the terraform file ssm.tf.
    Returns a string of the last time that an extraction took
    place in order to only retrieve the newest data since last
    extraction.
    """
    conn = boto3.client("ssm", region_name="eu-west-2")
    response = conn.get_parameter(Name=parameter_name)
    last_timestamp = response["Parameter"]["Value"]
    return last_timestamp


def write_current_timestamp(parameter_name, current_time):
    """
    Takes a parameter_name (str) which will need to correlate with
    the specified name in the terraform file ssm.tf.
    Takes another parameter of current_time (str) that writes over
    the previous timestamp to continue to only extract the most
    recent uploaded data.
    Returns a response dictionary containing information about
    the updated parameter
    """
    formatted_time = current_time.isoformat(sep=" ", timespec="auto")
    conn = boto3.client("ssm", region_name="eu-west-2")
    response = conn.put_parameter(
        Name=parameter_name,
        Value=formatted_time,
        Type="String",
        Overwrite=True
    )
    return response
