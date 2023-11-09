resource "aws_s3_bucket" "code_bucket_2" {
  bucket = "nc-group3-code-bucket2"
}

resource "aws_s3_bucket" "transformation_bucket" {
  bucket = "nc-group3-transformation-bucket"
}

# NEEDS TO BE PUT IN WHEN TRANSFORMATION LAMBDA READY
# resource "aws_s3_object" "lambda_code_2" {
#   key    = "transformation_function/function2.zip"
#   source = "${path.module}/../function2.zip"
#   bucket = aws_s3_bucket.code_bucket_2.id
# }