from datetime import datetime
from unittest.mock import patch
from botocore.exceptions import ClientError
import logging
from src.extraction.extraction_handler import lambda_handler
# from src.extraction.access_database import (
#     get_credentials,
#     get_con,
#     get_tables,
#     select_table,
#     select_table_headers,
# )
from src.extraction.write_data import convert_to_csv, upload_to_s3
from src.extraction.store_timestamp import get_last_timestamp, write_current_timestamp


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


