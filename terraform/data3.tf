#WAREHOUSE LAMBDA
data "archive_file" "lambda3" {
  type        = "zip"
  source_file = "${path.module}/../src/storage/storage_handler.py"  
  output_path = "${path.module}/../function3.zip"  
}