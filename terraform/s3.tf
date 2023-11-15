#INGESTION LAMBDA
# set up buckets
resource "aws_s3_bucket" "code_bucket" {
  bucket = "nc-group3-code-bucket"
}

resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "nc-group3-ingestion-bucket"
}

resource "aws_s3_object" "lambda_code" {
  key    = "ingestion_function/function.zip"     
  source = "${path.module}/../function.zip" 
  bucket = aws_s3_bucket.code_bucket.id 
}

resource "aws_s3_object" "layer_code" {
  key    = "ingestion_function/layer_code.zip"      
  source = "${path.module}/../aws_utils/layer_code.zip"   
  bucket = aws_s3_bucket.code_bucket.id
}
