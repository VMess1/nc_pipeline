# resource "aws_lambda_function" "lambda_warehouse" {
#     function_name = var.lambda_warehouse
#     role = aws_iam_role.lambda_warehouse_role.arn
#     s3_bucket = aws_s3_bucket.code_bucket_3.id
#     s3_key = aws_s3_object.lambda_code_3.key
#     handler = "storage_handler.main"   #update
#     runtime = "python3.11"
#     layers = [] #add layers
# }