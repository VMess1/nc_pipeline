resource "aws_lambda_function" "lambda_transformation" {
    function_name = var.lambda_transformation
    role = aws_iam_role.lambda_transformation_role.arn
    s3_bucket = aws_s3_bucket.code_bucket_2.id
    s3_key = aws_s3_object.lambda_code_2.key
    handler = "processing_handler.main"
    runtime = "python3.11"
    layers = [aws_lambda_layer_version.layer_dependencies_2.arn]
    # aws_lambda_layer_version.layer_dependencies_3.arn]
    timeout=60
}

# lambda2 dependencies
resource "aws_lambda_layer_version" "layer_dependencies_2" {
  layer_name = "layer_dependencies"
  s3_bucket = aws_s3_bucket.code_bucket_2.id
  s3_key = aws_s3_object.layer_code_2.key
  compatible_runtimes = ["python3.11"]
}

# # lambda2 pyarrow dependencies
# resource "aws_lambda_layer_version" "layer_dependencies_3" {
#   layer_name = "layer_dependencies2"
#   s3_bucket = aws_s3_bucket.code_bucket_2.id
#   s3_key = aws_s3_object.layer_code_3.key
#   compatible_runtimes = ["python3.11"]
# }

#giving the transformation s3 bucket permission to trigger the warehouse lambda
resource "aws_lambda_permission" "s3_trigger2" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_warehouse.arn
  principal     = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.transformation_bucket.arn
}

# #creates the trigger for the warehouse lambda
# resource "aws_s3_bucket_notification" "transformation_bucket_notification" {
#   bucket = aws_s3_bucket.transformation_bucket.id
#   lambda_function {
#     lambda_function_arn = aws_lambda_function.lambda_warehouse.arn
#     events              = ["s3:ObjectCreated:*"]
#   }
# }