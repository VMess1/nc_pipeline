# Eventually needs cloudwatch alarm based on error handling in python function
#Example of creating an error metric and alarm

resource "aws_cloudwatch_log_metric_filter" "example_error" {
    name           = "Fred"
    pattern        = "MultipleOfThreeError"
    log_group_name = "/aws/lambda/ingestion/test"
    metric_transformation {
        name      = "loggingerror"
        namespace = "Errors"
        value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "example_error_alarm" {
  alarm_name                = "terraform-Fred-error"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               ="loggingerror"
  namespace                 = "Errors"
  period                    = 30
  statistic                 = "SampleCount"
  threshold                 = 5
  alarm_actions             = ["arn recieved when setting up email"]
}
#example specific to exceution time error, doesn't require filter
resource "aws_cloudwatch_metric_alarm" "alert_code_execution_errors" {
  alarm_name                = "terraform-test-code-execution-error"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "Duration"
  namespace                 = "AWS/Lambda"
  period                    = 30
  statistic                 = "Maximum"
  threshold                 = 600
  unit                      = "Milliseconds"
  alarm_description         = "Code Execution Error Count Exceeded Threshold of 1"
  alarm_actions             = ["arn recieved when setting up email"]
}

