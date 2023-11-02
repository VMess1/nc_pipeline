import boto3
from moto import mock_s3
from src.extraction.upload_to_s3 import lambda_handler
from tests.test_extraction.test_compare_csv import strings


@mock_s3
def test_s3_upload():
    conn = boto3.resource('s3', region_name='eu-west-2')
    conn.create_bucket(
        Bucket='landing-bucket-team-3',
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'})
    new_csv = strings.difference_1()
    res = lambda_handler(new_csv)
    print(res['body'])
