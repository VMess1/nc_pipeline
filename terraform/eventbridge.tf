# Creates an eventbridge rule to invoke ingestion lambda every 2 mins
resource "aws_cloudwatch_event_rule" "event_rule" {
    name = "event_rule"
    schedule_expression = "rate(2 minutes)"
}

# Assigns the above rule to the ingestion lambda
resource "aws_cloudwatch_event_target" "event_target" {
    arn  =  aws_lambda_function.lambda_ingestion.arn
    rule = aws_cloudwatch_event_rule.event_rule.name
}
