#Sets the IAM user to the same credentials/user
data "aws_caller_identity" "current" {}

#Sets the region to eu-west-2
data "aws_region" "current" {}

#zips the lambda function ready for use
data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/tftest/testfunc.py"  #change this(function name)
  output_path = "${path.module}/../function.zip"  
}

data "archive_file" "requirements" {
  type = "zip"
  source_dir = "${path.module}/../layers"
  output_path = "${path.module}/../layer_code.zip"
}