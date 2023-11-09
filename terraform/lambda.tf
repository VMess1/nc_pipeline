#INGESTION LAMBDA

#lambda function 
resource "aws_lambda_function" "lambda_ingestion" {
    function_name = var.lambda_ingestion
    role = aws_iam_role.lambda_ingestion_role.arn
    s3_bucket = aws_s3_bucket.code_bucket.id
    s3_key = aws_s3_object.lambda_code.key
    handler = "extraction_handler.lambda_handler"
    runtime = "python3.11"
    # source_code_hash = data.archive_file.lambda.output_base64sha256
    layers = [aws_lambda_layer_version.layer_dependencies.arn]
    timeout=60
}

# lambda dependencies
resource "aws_lambda_layer_version" "layer_dependencies" {
  layer_name = "layer_dependencies"
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = aws_s3_object.layer_code.key
  compatible_runtimes = ["python3.11"]
}

#giving eventbridge permission to invoke lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.lambda_ingestion.function_name 
  principal      = "events.amazonaws.com"
  source_arn     = aws_cloudwatch_event_rule.event_rule.arn
}

#giving the ingestion lambda permission to trigger the transformation lambda
resource "aws_lambda_permission" "s3_trigger" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_transformation.arn
  principal     = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.ingestion_bucket.arn
}

#creates the trigger for the transformation lambda
resource "aws_s3_bucket_notification" "ingestion_bucket_notification" {
  bucket = aws_s3_bucket.ingestion_bucket.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_transformation.arn
    events              = ["s3:ObjectCreated:*"]
  }
}