# resource "aws_lambda_function" "lambda_warehouse" {
#     function_name = var.lambda_warehouse
#     role = aws_iam_role.lambda_warehouse_role.arn
#     s3_bucket = aws_s3_bucket.code_bucket_3.id
#     s3_key = aws_s3_object.lambda_code_3.key
#     handler = "storage_handler.main"  
#     runtime = "python3.11"
#     layers = [aws_lambda_layer_version.layer_dependencies_3.arn,
#     "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:2"]
#     timeout = 120
# }

# lambda3 dependencies
resource "aws_lambda_layer_version" "layer_dependencies_3" {
  layer_name = "layer_dependencies"
  s3_bucket = aws_s3_bucket.code_bucket_3.id
  s3_key = aws_s3_object.layer_code_3.key
  compatible_runtimes = ["python3.11"]
}
