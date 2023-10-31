#lambda function 
resource "aws_lambda_function" "lambda_ingestion" {
    function_name = var.lambda_ingestion
    role = aws_iam_role.lambda_ingestion_role.arn
    s3_bucket = aws_s3_bucket.code_bucket.id
    s3_key = aws_s3_object.lambda_code.key
    handler = "testfunc.handler"    #update this with function name
    runtime = "python3.11"

}

#giving eventbridge permission to invoke lambda

resource "aws_lambda_permission" "allow_eventbridge" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.lambda_ingestion.function_name    #double check
  principal      = "events.amazonaws.com"
  source_arn     = aws_cloudwatch_event_rule.event_rule.arn
  
}






