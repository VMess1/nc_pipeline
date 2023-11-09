from unittest.mock import patch
from botocore.exceptions import ClientError
from re import search
import logging
import os
import pytest
from src.extraction.extraction_handler import lambda_handler


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@patch("src.extraction.extraction_handler.get_credentials")
def test_get_credentials_called_correctly(mock_credentials):
    lambda_handler({}, {})
    mock_credentials.assert_called_with('OLTPCredentials')


@patch("src.extraction.extraction_handler.get_last_timestamp")
def test_get_last_timestamp_called_correctly(mock_credentials):
    lambda_handler({}, {})
    mock_credentials.assert_called_with('last_extraction')


@patch("src.extraction.extraction_handler.get_con")
@patch("src.extraction.extraction_handler.get_credentials",
       return_value='test_credentials')
def test_get_con_called_correctly(mock_credentials, mock_con):
    lambda_handler({}, {})
    mock_con.assert_called_with('test_credentials')


@patch("src.extraction.extraction_handler.get_tables")
@patch("src.extraction.extraction_handler.get_con",
       return_value='test_con')
def test_get_tables_called_correctly(mock_con, mock_tables):
    lambda_handler({}, {})
    mock_tables.assert_called_with('test_con')


@patch("src.extraction.extraction_handler.select_table")
@patch("src.extraction.extraction_handler.get_con",
       return_value="test_con")
@patch("src.extraction.extraction_handler.get_tables",
       return_value=[['test_table']])
@patch("src.extraction.extraction_handler.get_last_timestamp",
       return_value="test_timestamp")
def test_select_table_called_correctly(mock_timestamp,
                                       mock_tables,
                                       mock_con,
                                       mock_selection):
    lambda_handler({}, {})
    mock_selection.assert_called_with('test_con',
                                      'test_table',
                                      'test_timestamp')


@patch("src.extraction.extraction_handler.select_table_headers")
@patch("src.extraction.extraction_handler.get_tables",
       return_value=[['test_table_name']])
@patch("src.extraction.extraction_handler.get_con",
       return_value='test_con')
@patch("src.extraction.extraction_handler.select_table",
       return_value=['test_data'])
def test_select_table_headers_called_correctly(mock_selection,
                                               mock_con,
                                               mock_tables,
                                               mock_select_table_headers):
    lambda_handler({}, {})
    mock_select_table_headers.assert_called_with('test_con', 'test_table_name')


@patch("src.extraction.extraction_handler.convert_to_csv")
@patch("src.extraction.extraction_handler.select_table_headers",
       return_value='test_headers')
@patch("src.extraction.extraction_handler.get_tables",
       return_value=[['test_table_name']])
@patch("src.extraction.extraction_handler.select_table",
       return_value=['test_data'])
def test_convert_to_csv_called_correctly(mock_selection,
                                         mock_tables,
                                         mock_headers,
                                         mock_conversion):
    lambda_handler({}, {})
    mock_conversion.assert_called_with('test_table_name',
                                       ['test_data'],
                                       'test_headers')


@patch("src.extraction.extraction_handler.upload_to_s3")
@patch("src.extraction.extraction_handler.convert_to_csv",
       return_value='test_csv')
@patch("src.extraction.extraction_handler.get_tables",
       return_value=[['test_table_name']])
@patch("src.extraction.extraction_handler.select_table",
       return_value=['test_data'])
def test_upload_to_s3_called_correctly(mock_selection,
                                       mock_tables,
                                       mock_csv,
                                       mock_upload):
    lambda_handler({}, {})
    assert mock_upload.call_args.args[1] == 'test_csv'
    regex = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
    assert search(regex, str(mock_upload.call_args.args[0])) is not None


@patch("src.extraction.extraction_handler.write_current_timestamp")
@patch("src.extraction.extraction_handler.get_tables", return_value=[['_']])
def test_write_current_timestamp_called_correctly(mock_table, mock_write):
    lambda_handler({}, {})
    assert mock_write.call_args.args[0] == 'last_extraction'
    regex = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
    assert search(regex, str(mock_write.call_args.args[1])) is not None


@patch("src.extraction.extraction_handler.logger.info")
@patch("src.extraction.extraction_handler.select_table",
       return_value=['test_data'])
@patch("src.extraction.extraction_handler.get_tables",
       return_value=[['test_table_name']])
def test_logs_current_datetime_to_cloudwatch_if_has_data(mock_tables,
                                                         mock_select_table,
                                                         mock_logging):
    lambda_handler({}, {})
    regex = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
    assert search(regex, str(mock_logging.call_args)) is not None


LOGGER = logging.getLogger(__name__)


@patch("src.extraction.extraction_handler.get_credentials",
       return_value=500)
def test_logs_correct_message_if_TypeError(mock_credentials,
                                           caplog):
    lambda_handler({}, {})
    assert "Incorrect parameter type:" in caplog.text


@patch("src.extraction.extraction_handler.get_credentials")
def test_logs_correct_err_message_if_resource_not_found(mock_credentials,
                                                        caplog):
    mock_credentials.side_effect = ClientError(
        error_response={"Error": {"Code": "ResourceNotFoundException"}},
        operation_name="ClientError"
    )
    lambda_handler({}, {})
    assert "Credentials not found." in caplog.text


@patch("src.extraction.extraction_handler.get_credentials")
def test_logs_correct_message_if_bucket_not_found(mock_credentials,
                                                  caplog):
    mock_credentials.side_effect = ClientError(
        error_response={"Error": {"Code": "NoSuchBucket"}},
        operation_name="ClientError"
    )
    lambda_handler({}, {})
    assert "Bucket not found." in caplog.text


@patch("src.extraction.extraction_handler.get_credentials")
def test_logs_correct_message_if_internal_service_error(mock_credentials,
                                                        caplog):
    mock_credentials.side_effect = ClientError(
        error_response={"Error": {"Code": "InternalServiceError"}},
        operation_name="ClientError"
    )
    lambda_handler({}, {})
    assert "Internal service error detected." in caplog.text
