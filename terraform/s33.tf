# STORAGE LAMBDA

# sets up the code bucket
resource "aws_s3_bucket" "code_bucket_3" {
 bucket = "nc-group3-code-bucket3"
}

# lambda 3 function put in bucket
resource "aws_s3_object" "lambda_code_3" {
  key    = "warehouse_function/function3.zip"
  source = "${path.module}/../function3.zip"
  bucket = aws_s3_bucket.code_bucket_3.id
}

# Puts the dependencies into the bucket
resource "aws_s3_object" "layer_code_3" {
  key    = "warehouse_function/layer_code3.zip"
  source = "${path.module}/../aws_utils/layer_code3.zip"
  bucket = aws_s3_bucket.code_bucket_3.id
}

