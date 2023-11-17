# sets the SSM last_extraction
resource "aws_ssm_parameter" "param" {
  name  = "last_extraction"
  type  = "String"
  value = "2020-11-08 14:52:35"
}

# sets the SSM last_insertion
resource "aws_ssm_parameter" "param2" {
  name  = "last_insertion"
  type  = "String"
  value = "2020-11-08 14:52:35"
}