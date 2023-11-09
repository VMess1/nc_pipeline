# resource "aws_lambda_function" "lambda_transformation" {
#     function_name = var.lambda_transformation
#     role = aws_iam_role.lambda_transformation_role.arn
#     s3_bucket = aws_s3_bucket.code_bucket_2.id
#     s3_key = aws_s3_object.lambda_code_2.key
#     handler = "processing_handler.main"
#     runtime = "python3.11"
#     layers = [] #add layers
# }