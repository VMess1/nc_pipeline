"""Dummy lambda function to test terraform config is correct"""

import boto3
import random
import string

s3 = boto3.client('s3')

def handler(event, context):
    """Test function that creates a random name for objects and places
    that object in a specified bucket name"""
    try:
        random_key = ''.join(random.choice(string.ascii_letters)
                             for num in range(10))
        response = s3.put_object(
            Bucket="nc-group3-ingestion-bucket",
            Key=random_key,
            Body="test"
        )
        return {
            'statusCode': 200,
            'body': f'Object with key {random_key} created in the S3 bucket'
        }
    except Exception as e:
        print(f"Error from put_object: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
