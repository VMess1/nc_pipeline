
# #ClientError alarm filter
# resource "aws_cloudwatch_log_metric_filter" "ClientError" {
#     name           = "Ingestion function Client Error"
#     pattern        = "ClientError"
#     log_group_name = "/aws/lambda/ingestion-errors"
#     metric_transformation {
#         name      = "loggingClienterror"
#         namespace = "Errors"
#         value     = "1"
#   }
# }
# #ClientError alarm
# resource "aws_cloudwatch_metric_alarm" "ClientError_alarm" {
#   alarm_name                = "Ingestion function Client error alarm"
#   comparison_operator       = "GreaterThanOrEqualToThreshold"
#   evaluation_periods        = 1
#   metric_name               ="loggingClienterror"
#   namespace                 = "Errors"
#   period                    = 30
#   statistic                 = "SampleCount"
#   threshold                 = 1
#   alarm_actions             = ["add someone's arn"]
#  }

#  #ExceptionError alarm filter
# resource "aws_cloudwatch_log_metric_filter" "ExceptionError" {
#     name           = "Ingestion function Exception Error"
#     pattern        = "Exception"
#     log_group_name = "/aws/lambda/ingestion-errors"
#     metric_transformation {
#         name      = "loggingExceptionerror"
#         namespace = "Errors"
#         value     = "1"
#   }
# }
# #Exception alarm
# resource "aws_cloudwatch_metric_alarm" "ExceptionError_alarm" {
#   alarm_name                = "Ingestion function Exception error alarm"
#   comparison_operator       = "GreaterThanOrEqualToThreshold"
#   evaluation_periods        = 1
#   metric_name               ="loggingExceptionerror"
#   namespace                 = "Errors"
#   period                    = 30
#   statistic                 = "SampleCount"
#   threshold                 = 1
#   alarm_actions             = ["add someone's arn"]
#  }


# #example specific to exceution time error, doesn't require filter
# resource "aws_cloudwatch_metric_alarm" "alert_code_execution_errors" {
#   alarm_name                = "terraform-test-code-execution-error"
#   comparison_operator       = "GreaterThanOrEqualToThreshold"
#   evaluation_periods        = 1
#   metric_name               = "Duration"
#   namespace                 = "AWS/Lambda"
#   period                    = 30
#   statistic                 = "Maximum"
#   threshold                 = 600
#   unit                      = "Milliseconds"
#   alarm_description         = "Code Execution Error Count Exceeded Threshold of 1"
#   alarm_actions             = ["arn recieved when setting up email"]
# }

