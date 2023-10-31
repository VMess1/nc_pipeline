#create an eventbridge rule
resource "aws_cloudwatch_event_rule" "event_rule" {
    name = "event_rule"
    schedule_expression = "rate(2 minutes)"
}

#target for the rule
resource "aws_cloudwatch_event_target" "event_target" {
    arn  =  aws_lambda_function.lambda_ingestion.arn
    rule = aws_cloudwatch_event_rule.event_rule.name
}
