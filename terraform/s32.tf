resource "aws_s3_bucket" "code_bucket_2" {
  bucket = "nc-group3-code-bucket2"
}

resource "aws_s3_bucket" "transformation_bucket" {
  bucket = "nc-group3-transformation-bucket"
}

# Lambda 2 function placed in bucket
 resource "aws_s3_object" "lambda_code_2" {
  key    = "transformation_function/function2.zip"
  source = "${path.module}/../function2.zip"
  bucket = aws_s3_bucket.code_bucket_2.id
}

resource "aws_s3_object" "layer_code_2" {
  key    = "ingestion_function/layer_code2.zip"
  source = "${path.module}/../aws_utils/layer_code2.zip"
  bucket = aws_s3_bucket.code_bucket_2.id
}

# resource "aws_s3_object" "layer_code_3" {
#   key    = "ingestion_function/layer_code3.zip"
#   source = "${path.module}/../aws_utils/layer_code3.zip"
#   bucket = aws_s3_bucket.code_bucket_2.id
# }