import logging
import unittest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from datetime import datetime
from src.extraction.extraction_handler import lambda_handler


class TestLambdaHandlerFunctionality(unittest.TestCase):
    @patch('src.extraction.extraction_handler.logging.getLogger')
    @patch('src.extraction.extraction_handler.datetime')
    @patch('src.extraction.extraction_handler.write_current_timestamp')
    @patch('src.extraction.extraction_handler.upload_to_s3')
    @patch('src.extraction.extraction_handler.convert_to_csv',
           return_value='test_csv')
    @patch('src.extraction.extraction_handler.select_table_headers',
           return_value='test_headers')
    @patch('src.extraction.extraction_handler.select_table',
           return_value='test')
    @patch('src.extraction.extraction_handler.get_tables',
           return_value=[['test_table1', {}]])
    @patch('src.extraction.extraction_handler.get_last_timestamp')
    @patch('src.extraction.extraction_handler.get_con')
    @patch('src.extraction.extraction_handler.get_credentials', autospec=True)
    def test_lambda_handler_calls_functions_correctly(
                        self,
                        mock_get_credentials, mock_get_con,
                        mock_get_last_timestamp, mock_get_tables,
                        mock_select_table, mock_select_table_headers,
                        mock_convert_to_csv, mock_upload_to_s3,
                        mock_write_timestamp, mock_datetime, mock_get_logger
                        ):
        """Test checks that the lambda_handler function, which is the main
        function deployed to AWS lambda calls all of the util functions
        correctly and with the correct returned parameters"""
        mock_log = MagicMock()
        mock_get_logger.return_value = mock_log
        mock_datetime.now.return_value = datetime(2000, 1, 1, 11, 0, 0)
        mock_datestamp = mock_datetime.now.return_value.replace(microsecond=0)
        lambda_handler({}, {})
        mock_get_credentials.assert_called_once_with("OLTPCredentials")
        mock_get_con.assert_called_once_with(mock_get_credentials.return_value)
        mock_get_last_timestamp.assert_called_once_with("last_extraction")
        mock_get_tables.assert_called_once_with(mock_get_con.return_value)
        mock_select_table.assert_called_once_with(
            mock_get_con.return_value,
            'test_table1',
            mock_get_last_timestamp.return_value)
        mock_select_table_headers.assert_called_once_with(
            mock_get_con.return_value,
            'test_table1'
            )
        mock_convert_to_csv.assert_called_once_with(
            'test_table1',
            'test',
            'test_headers'
            )
        mock_upload_to_s3.assert_called_once_with(
            str(mock_datestamp),
            'test_csv'
            )
        mock_write_timestamp.assert_called_with(
            'last_extraction',
            mock_datestamp)


class TestLambdaHandlerErrorHandling:

    LOGGER = logging.getLogger(__name__)

    @patch("src.extraction.extraction_handler.get_credentials",
           return_value=500)
    def test_logs_correct_message_if_TypeError(self, mock_credentials,
                                               caplog):
        """Checks to see if TypeError message is correctly returned if
        a TypeError occurs in the main lambda handler"""
        mock_credentials.side_effect = TypeError
        lambda_handler({}, {})
        assert "Incorrect parameter type:" in caplog.text

    @patch("src.extraction.extraction_handler.get_credentials")
    def test_logs_correct_err_message_if_resource_not_found(
            self,
            mock_credentials,
            caplog
            ):
        """Checks to see if correct ClientError message given if a
        ResourceNotFound error occurs in the main lambda handler"""
        mock_credentials.side_effect = ClientError(
            error_response={"Error": {"Code": "ResourceNotFoundException"}},
            operation_name="ClientError"
        )
        lambda_handler({}, {})
        assert "Credentials not found." in caplog.text

    @patch("src.extraction.extraction_handler.get_credentials")
    def test_logs_correct_message_if_bucket_not_found(self, mock_credentials,
                                                      caplog):
        """Checks to see if correct ClientError message given if a NoSuchBucket
         error occurs in the main lambda handler"""
        mock_credentials.side_effect = ClientError(
            error_response={"Error": {"Code": "NoSuchBucket"}},
            operation_name="ClientError"
        )
        lambda_handler({}, {})
        assert "Bucket not found." in caplog.text

    @patch("src.extraction.extraction_handler.get_credentials")
    def test_logs_correct_message_if_internal_service_error(
                                            self,
                                            mock_credentials,
                                            caplog
                                            ):
        """Checks to see if correct ClientError message given if an
        InternalServiceError occurs in the main lambda handler"""
        mock_credentials.side_effect = ClientError(
            error_response={"Error": {"Code": "InternalServiceError"}},
            operation_name="ClientError"
        )
        lambda_handler({}, {})
        assert "Internal service error detected." in caplog.text

    @patch("src.extraction.extraction_handler.get_credentials")
    def test_logs_correct_message_if_Exception_error(
                                            self,
                                            mock_credentials,
                                            caplog
                                            ):
        """Checks to see if correct Exception message if the lambda
        handler runs into any other type of error"""
        mock_credentials.side_effect = Exception
        lambda_handler({}, {})
        assert "An unexpected error has occurred:" in caplog.text


class TestLambdaHandler(unittest.TestCase):

    @patch('src.extraction.extraction_handler.upload_to_s3')
    @patch('src.extraction.extraction_handler.convert_to_csv',
           return_value='test_csv')
    @patch('src.extraction.extraction_handler.select_table_headers',
           return_value='test_headers')
    @patch('src.extraction.extraction_handler.select_table',
           return_value='test')
    @patch('src.extraction.extraction_handler.get_tables',
           return_value=[['test_table1', {}]])
    @patch('src.extraction.extraction_handler.get_last_timestamp',
           return_value='fake_stamp')
    @patch('src.extraction.extraction_handler.get_con')
    @patch('src.extraction.extraction_handler.get_credentials', autospec=True)
    def test_logging_info(self, mock_credentials, mock_con, mock_get_timestamp,
                          mock_get_tables, mock_select_table,
                          mock_select_table_headers,
                          mock_convert_to_s3,
                          mock_upload_to_s3
                          ):
        with patch("src.extraction.extraction_handler.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2000, 1, 1, 12, 0, 0)
            with patch("src.extraction.extraction_handler.logger") as mock_log:
                lambda_handler({}, {})
        expected_log_message = "2000-01-01 12:00:00"
        mock_log.info.assert_called_with(expected_log_message)
