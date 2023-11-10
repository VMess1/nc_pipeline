#WAREHOUSE LAMBDA
# data "archive_file" "lambda3" {
#   type        = "zip"
#   source_file = "${path.module}/../src/storage/storage_handler.py"  #path may be different
#   output_path = "${path.module}/../function3.zip"  
# }