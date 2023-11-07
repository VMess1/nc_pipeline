from datetime import datetime
from unittest.mock import patch
from botocore.exceptions import ClientError
import logging
import os
import pytest
from freezegun import freeze_time
from moto import mock_logs
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


@patch("src.extraction.extraction_handler.get_con")
@patch("src.extraction.extraction_handler.get_credentials", return_value='test_credentials')
def test_get_con_called_correctly(mock_credentials,mock_con):
    lambda_handler({}, {})
    mock_con.assert_called_with('test_credentials')


@patch("src.extraction.extraction_handler.get_last_timestamp")
def test_get_last_timestamp_called_correctly(mock_credentials):
    lambda_handler({}, {})
    mock_credentials.assert_called_with('last_extraction')


@patch("src.extraction.extraction_handler.get_tables")
@patch("src.extraction.extraction_handler.get_con", return_value='test_con')
def test_get_tables_called_correctly(mock_con, mock_tables):
    lambda_handler({}, {})
    mock_tables.assert_called_with('test_con')


@patch("src.extraction.extraction_handler.select_table")
@patch("src.extraction.extraction_handler.get_con", return_value="test_con")
@patch("src.extraction.extraction_handler.get_tables", return_value=[['test_table']])
@patch("src.extraction.extraction_handler.get_last_timestamp", return_value="test_timestamp")
def test_select_table_called_correctly(mock_timestamp, mock_tables, mock_con, mock_selection):
    lambda_handler({}, {})
    mock_selection.assert_called_with('test_con', 'test_table', 'test_timestamp')


@patch("src.extraction.extraction_handler.select_table_headers")
@patch("src.extraction.extraction_handler.get_tables", return_value=[['test_table_name']])
@patch("src.extraction.extraction_handler.get_con", return_value='test_con')
@patch("src.extraction.extraction_handler.select_table", return_value=['test_data'])
def test_select_table_headers_called_correctly(mock_selection, mock_con, mock_tables, mock_select_table_headers):
    lambda_handler({}, {})
    mock_select_table_headers.assert_called_with('test_con', 'test_table_name')


@patch("src.extraction.extraction_handler.convert_to_csv")
@patch("src.extraction.extraction_handler.select_table_headers", return_value='test_headers')
@patch("src.extraction.extraction_handler.get_tables", return_value=[['test_table_name']])
@patch("src.extraction.extraction_handler.select_table", return_value=['test_data'])
def test_convert_to_csv_called_correctly(mock_selection, mock_tables, mock_headers, mock_conversion):
    lambda_handler({}, {})
    mock_conversion.assert_called_with('test_table_name', ['test_data'], 'test_headers')


@patch("src.extraction.extraction_handler.upload_to_s3")
@patch("src.extraction.extraction_handler.convert_to_csv", return_value='test_csv')
@patch("src.extraction.extraction_handler.get_tables", return_value=[['test_table_name']])
@patch("src.extraction.extraction_handler.select_table", return_value=['test_data'])
def test_upload_to_s3_called_correctly(mock_selection, mock_tables, mock_csv, mock_upload):
    lambda_handler({}, {})
    mock_upload.assert_called_with('test_csv')


# @patch("src.extraction.extraction_handler.write_current_timestamp")
# @patch("src.extraction.extraction_handler.get_tables", return_value=[['_']])
# @freeze_time("2022-01-01")
# def test_write_current_timestamp_called_correctly(mock_table , mock_write):
#     lambda_handler({}, {})
#     mock_write.assert_called_with('last_extraction', "2022-01-01")



# @patch("src.extraction.extraction_handler.logging.info")
# @patch("src.extraction.extraction_handler.select_table", return_value=['test_data'])
# @patch("src.extraction.extraction_handler.get_tables", return_value=[['test_table_name']])
# def test_logs_current_datetime_to_cloudwatch_if_has_data(mock_tables, mock_select_table, mock_logging):
#     lambda_handler({}, {})
#     mock_logging.assert_called_with(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#     print(dir(mock_logging.get_log_events))
#     assert mock_logging.called == True
