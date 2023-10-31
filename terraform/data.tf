#Sets the IAM user to the same credentials/user
data "aws_caller_identity" "current" {}

#Sets the region to eu-west-2
data "aws_region" "current" {}