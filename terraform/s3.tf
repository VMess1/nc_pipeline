#INGESTION LAMBDA

# sets up the code bucket
resource "aws_s3_bucket" "code_bucket" {
  bucket = "nc-group3-code-bucket"
}

# Sets up the ingestion bucket
resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "nc-group3-ingestion-bucket"
}

# Puts the ingestion lambda code into the bucket
resource "aws_s3_object" "lambda_code" {
  key    = "ingestion_function/function.zip"     
  source = "${path.module}/../function.zip" 
  bucket = aws_s3_bucket.code_bucket.id 
}

# Puts the dependencies into the bucket
resource "aws_s3_object" "layer_code" {
  key    = "ingestion_function/layer_code.zip"      
  source = "${path.module}/../aws_utils/layer_code.zip"   
  bucket = aws_s3_bucket.code_bucket.id
}
