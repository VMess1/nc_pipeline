import boto3


def get_last_timestamp(parameter_name):
    """
    Takes the last_insertion parameter name and returns
    the value as stored in AWS Systems Manager, i.e. the
    last time at which the storage lambda was triggered.
    """
    conn = boto3.client("ssm", region_name="eu-west-2")
    response = conn.get_parameter(Name=parameter_name)
    last_timestamp = response["Parameter"]["Value"]
    return last_timestamp


def write_current_timestamp(parameter_name, current_time):
    """
    Takes the last_insertion parameter name and current time
    and writes the time to AWS Systems Manager parameter.
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
