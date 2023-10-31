# set up buckets
resource "aws_s3_bucket" "code_bucket" {
  bucket = "nc-group3-code-bucket"
}

resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "nc-group3-ingestion-bucket"
}

resource "aws_s3_object" "lambda_code" {
  key    = "function_name/function.zip"      #updating(function name)
  source = "${path.module}/../function.zip"
  bucket = aws_s3_bucket.code_bucket.id
}