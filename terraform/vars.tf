# Name for first lambda function
variable "lambda_ingestion" {
    type = string
    default = "lambda_ingestion"
}
# Name for the second lambda function
variable "lambda_transformation" {
    type = string
    default = "lambda_transformation"
}

# Name for the final lambda function
variable "lambda_warehouse" {
    type = string
    default = "lambda_warehouse"
}