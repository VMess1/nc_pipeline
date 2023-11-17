#TRANSFORMATION LAMBDA

# lambda function
resource "aws_lambda_function" "lambda_transformation" {
    function_name = var.lambda_transformation
    role = aws_iam_role.lambda_transformation_role.arn
    s3_bucket = aws_s3_bucket.code_bucket_2.id
    s3_key = aws_s3_object.lambda_code_2.key
    handler = "processing_handler.main"
    runtime = "python3.11"
    layers = [aws_lambda_layer_version.layer_dependencies_2.arn,
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:2"]
    timeout=120
}

# lambda2 dependencies
resource "aws_lambda_layer_version" "layer_dependencies_2" {
  layer_name = "layer_dependencies"
  s3_bucket = aws_s3_bucket.code_bucket_2.id
  s3_key = aws_s3_object.layer_code_2.key
  compatible_runtimes = ["python3.11"]
}
