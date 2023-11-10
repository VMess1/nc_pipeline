#Create log group for first lambda alerts
resource "aws_cloudwatch_log_group" "lambda1" {
  name = "lambda_ingestion_errors"
}


#ClientError alarm filter
resource "aws_cloudwatch_log_metric_filter" "ClientError" {
    name           = "Ingestion function Client Error"
    pattern        = "ClientError"
    log_group_name = aws_cloudwatch_log_group.lambda1.name
    metric_transformation {
        name      = "loggingClienterror"
        namespace = "Errors"
        value     = "1"
  }
}

#ClientError alarm
resource "aws_cloudwatch_metric_alarm" "ClientError_alarm" {
  alarm_name                = "Ingestion function Client error alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "loggingClienterror"
  namespace                 = "Errors"
  period                    = 30
  statistic                 = "SampleCount"
  threshold                 = 1
  alarm_actions             = ["arn:aws:sns:eu-west-2:858570386577:test-error-alerts"]
 }

 #ExceptionError alarm filter
resource "aws_cloudwatch_log_metric_filter" "ExceptionError" {
    name           = "Ingestion function Exception Error"
    pattern        = "Exception"
    log_group_name = aws_cloudwatch_log_group.lambda1.name
    metric_transformation {
        name      = "loggingExceptionerror"
        namespace = "Errors"
        value     = "1"
  }
}

#Exception alarm
resource "aws_cloudwatch_metric_alarm" "ExceptionError_alarm" {
  alarm_name                = "Ingestion function Exception error alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "loggingExceptionerror"
  namespace                 = "Errors"
  period                    = 30
  statistic                 = "SampleCount"
  threshold                 = 1
  alarm_actions             = ["arn:aws:sns:eu-west-2:858570386577:test-error-alerts"]
 }

 #TypeError alarm filter
resource "aws_cloudwatch_log_metric_filter" "TypeError" {
    name           = "Ingestion function TypeError"
    pattern        = "TypeError"
    log_group_name = aws_cloudwatch_log_group.lambda1.name
    metric_transformation {
        name      = "loggingTypeError"
        namespace = "Errors"
        value     = "1"
  }
}

#TypeError alarm
resource "aws_cloudwatch_metric_alarm" "TypeError_alarm" {
  alarm_name                = "Ingestion function TypeError alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "loggingTypeError"
  namespace                 = "Errors"
  period                    = 30
  statistic                 = "SampleCount"
  threshold                 = 1
  alarm_actions             = ["arn:aws:sns:eu-west-2:858570386577:test-error-alerts"]
 }

#Execution Time Error
resource "aws_cloudwatch_metric_alarm" "alert_code_execution_errors" {
  alarm_name                = "terraform-test-code-execution-error"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "Duration"
  namespace                 = "AWS/Lambda"
  period                    = 30
  statistic                 = "Maximum"
  threshold                 = 60000
  unit                      = "Milliseconds"
  alarm_description         = "Code Execution Error Count Exceeded Threshold of 1"
  alarm_actions             = ["arn:aws:sns:eu-west-2:858570386577:test-error-alerts"]
}
