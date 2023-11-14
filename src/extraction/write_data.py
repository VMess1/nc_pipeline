import boto3


def convert_to_csv(table_name, data, headers):
    """converts returned sql data to csv string"""
    the_goods = ""
    for index, collumn in enumerate(headers):
        if index == len(headers) - 1:
            the_goods += f"{collumn[0]}\n"
        else:
            the_goods += f"{collumn[0]};"
    for index, datum in enumerate(data):
        for index, dat in enumerate(datum):
            if index == len(datum) - 1:
                if "," in str(dat):
                    dat = str(dat).replace(",", "")
                the_goods += f"{dat}\n"
            else:
                if "," in str(dat):
                    dat = str(dat).replace(",", "")
                the_goods += f"{dat};"
    return the_goods


def upload_to_s3(datestamp, csv_string, table_name):
    """uploads csv string to s3 ingestion bucket"""
    if not isinstance(csv_string, str):
        raise TypeError("Incorrect csv formatting.")
    else:
        file_key = (
            table_name
            + '/'
            + table_name
            + datestamp.replace('-', '').replace(':', '').replace(' ', '')
            + ".csv"
        )
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.put_object(
            Bucket="nc-group3-ingestion-bucket", Key=file_key, Body=csv_string
        )
        return "file uploaded"
