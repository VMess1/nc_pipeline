resource "aws_ssm_parameter" "param" {
  name  = "last_extraction"
  type  = "String"
  value = "2020-11-08 14:52:35"
}