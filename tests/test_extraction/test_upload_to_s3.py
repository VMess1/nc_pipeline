import boto3
from moto import mock_s3
from src.extraction.upload_to_s3 import upload_to_s3
from tests.test_extraction.test_compare_csv import strings
import pytest


@mock_s3
def test_s3_upload():
    conn = boto3.client("s3", region_name="eu-west-2")
    conn.create_bucket(
        Bucket="nc-group3-ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    new_csv = strings.difference_1()
    res = upload_to_s3(new_csv)
    assert res == "file uploaded"


@mock_s3
def test_errors_handled_correctly():
    conn = boto3.client("s3", region_name="eu-west-2")
    conn.create_bucket(
        Bucket="nc-group2-ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    new_csv = strings.difference_1()
    res = upload_to_s3(new_csv)
    assert res == "The specified bucket does not exist"


@mock_s3
def test_errors_handled_correct():
    conn = boto3.client("s3", region_name="eu-west-2")
    conn.create_bucket(
        Bucket="nc-group3-ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    res = upload_to_s3(None)
    assert "Parameter validation failed" in str(res)
