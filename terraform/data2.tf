#TRANSFORMATION LAMBDA
# data "archive_file" "lambda2" {
#   type        = "zip"
#   source_file = "${path.module}/../src/processing/processing_handler.py" 
#   output_path = "${path.module}/../function2.zip"  
# }