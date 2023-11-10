# resource "aws_lambda_function" "lambda_transformation" {
#     function_name = var.lambda_transformation
#     role = aws_iam_role.lambda_transformation_role.arn
#     s3_bucket = aws_s3_bucket.code_bucket_2.id
#     s3_key = aws_s3_object.lambda_code_2.key
#     handler = "processing_handler.main"
#     runtime = "python3.11"
#     layers = [] #add layers
# }

# NEEDS TO BE PUT IN WHEN WAREHOUSE LAMBDA READY
# giving the transformation s3 bucket permission to trigger the warehouse lambda
# resource "aws_lambda_permission" "s3_trigger2" {
#   statement_id  = "AllowExecutionFromS3"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.lambda_warehouse.arn
#   principal     = "s3.amazonaws.com"
#   source_arn = aws_s3_bucket.transformation_bucket.arn
# }

# #creates the trigger for the warehouse lambda
# resource "aws_s3_bucket_notification" "transformation_bucket_notification" {
#   bucket = aws_s3_bucket.transformation_bucket.id
#   lambda_function {
#     lambda_function_arn = aws_lambda_function.lambda_warehouse.arn
#     events              = ["s3:ObjectCreated:*"]
#   }
# }