#Sets the IAM user to the same credentials/user
data "aws_caller_identity" "current" {}

#Sets the region to eu-west-2
data "aws_region" "current" {}

#INGESTION LAMBDA
data "archive_file" "lambda" {
  type        = "zip"
  source_dir = "${path.module}/../src/extraction/" 
  output_path = "${path.module}/../function.zip"  
}

# data "archive_file" "requirements" {
#   type = "zip"
#   source_dir = "${path.module}/../layers"
#   output_path = "${path.module}/../layer_code.zip"
# }

# data "archive_file" "requirements" {
#    type = "zip"
#    source_dir = "${path.module}/../layers"
#    output_path = "${path.module}/../layer_code.zip"
#  }


resource "aws_lambda_layer_version" "my_layer" {
  filename   = "./layer_code.zip"
  layer_name = "my-layer"
}