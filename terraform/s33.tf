resource "aws_s3_bucket" "code_bucket_3" {
 bucket = "nc-group3-code-bucket2"
}

#NEEDS TO BE PUT IN WHEN WAREHOUSE LAMBDA READY
resource "aws_s3_object" "lambda_code_3" {
  key    = "warehouse_function/function3.zip"
  source = "${path.module}/../function3.zip"
  bucket = aws_s3_bucket.code_bucket_3.id
}