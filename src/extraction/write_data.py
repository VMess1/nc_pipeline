import boto3


def convert_to_csv(data, headers):
    """
    Takes a data list that is returned from select_table()
    function which contains all the data to convert into
    a csv file ready for storage in an s3 bucket
    Takes a headers parameter which is a nested list of lists
    created by the select_table_headers() function

    Returns a string of all of the data formatted into a csv
    format to allow the data to be saved to an s3 bucket.
    """
    the_goods = ""
    for index, column in enumerate(headers):
        if index == len(headers) - 1:
            the_goods += f"{column[0]}\n"
        else:
            the_goods += f"{column[0]};"
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
    """
    Takes a datestamp parameter which is a string of the time that the
    extraction was executed in order to save this in the filename. This
    then organises the files in the s3 bucket in an order that can be easily
    sorted and understood.
    Takes a csv_string which is the string returned from the covert_to_csv()
    function which is all of the extracted data from a specified table and
    formatted into a csv style to save the data in a tabular format.
    Takes a table_name (str) which is the name of the specified table that
    the data has been extracted from.
    The specified bucket in this specific case would need to be changed
    depending on what you wish to call your bucket to save the data in.
    Terraform will set up the bucket on deployment and so you will need
    to make sure that your bucket name is changed in the terraform and
    on line 64 of this function.
    Returns a string of "file uploaded".
    """
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
