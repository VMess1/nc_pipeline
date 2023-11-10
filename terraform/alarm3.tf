# #Create log group for third lambda alerts
# resource "aws_cloudwatch_log_group" "lambda3" {
#   name = "lambda_warehouse_errors"
# }

# #ClientError alarm filter
# resource "aws_cloudwatch_log_metric_filter" "ClientError3" {
#     name           = "Warehouse function Client Error"
#     pattern        = "ClientError"
#     log_group_name = aws_cloudwatch_log_group.lambda3.name
#     metric_transformation {
#         name      = "loggingClienterror"
#         namespace = "Errors"
#         value     = "1"
#   }
# }

# #ClientError alarm
# resource "aws_cloudwatch_metric_alarm" "ClientError_alarm3" {
#   alarm_name                = "Warehouse function Client error alarm"
#   comparison_operator       = "GreaterThanOrEqualToThreshold"
#   evaluation_periods        = 1
#   metric_name               = "loggingClienterror"
#   namespace                 = "Errors"
#   period                    = 30
#   statistic                 = "SampleCount"
#   threshold                 = 1
#   alarm_actions             = ["arn:aws:sns:eu-west-2:858570386577:test-error-alerts"]
#  }

#  #ExceptionError alarm filter
# resource "aws_cloudwatch_log_metric_filter" "ExceptionError3" {
#     name           = "Warehouse function Exception Error"
#     pattern        = "Exception"
#     log_group_name = aws_cloudwatch_log_group.lambda3.name
#     metric_transformation {
#         name      = "loggingExceptionerror"
#         namespace = "Errors"
#         value     = "1"
#   }
# }

# #Exception alarm
# resource "aws_cloudwatch_metric_alarm" "ExceptionError_alarm3" {
#   alarm_name                = "Warehouse function Exception error alarm"
#   comparison_operator       = "GreaterThanOrEqualToThreshold"
#   evaluation_periods        = 1
#   metric_name               = "loggingExceptionerror"
#   namespace                 = "Errors"
#   period                    = 30
#   statistic                 = "SampleCount"
#   threshold                 = 1
#   alarm_actions             = ["arn:aws:sns:eu-west-2:858570386577:test-error-alerts"]
#  }

#  #TypeError alarm filter
# resource "aws_cloudwatch_log_metric_filter" "TypeError3" {
#     name           = "Warehouse function TypeError"
#     pattern        = "TypeError"
#     log_group_name = aws_cloudwatch_log_group.lambda3.name
#     metric_transformation {
#         name      = "loggingTypeError"
#         namespace = "Errors"
#         value     = "1"
#   }
# }

# #TypeError alarm
# resource "aws_cloudwatch_metric_alarm" "TypeError_alarm3" {
#   alarm_name                = "Warehouse function TypeError alarm"
#   comparison_operator       = "GreaterThanOrEqualToThreshold"
#   evaluation_periods        = 1
#   metric_name               = "loggingTypeError"
#   namespace                 = "Errors"
#   period                    = 30
#   statistic                 = "SampleCount"
#   threshold                 = 1
#   alarm_actions             = ["arn:aws:sns:eu-west-2:858570386577:test-error-alerts"]
#  }
 