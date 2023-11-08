resource "aws_s3_bucket" "code_bucket_2" {
  bucket = "nc-group3-code-bucket2"
}

resource "aws_s3_bucket" "transformation_bucket" {
  bucket = "nc-group3-transformation-bucket"
}

resource "aws_s3_object" "lambda_code_2" {
  key    = "transformation_function/function.zip"      #updating(function name)
  source = "${path.module}/../function.zip"  #update with proper name
  bucket = aws_s3_bucket.code_bucket_2.id
}