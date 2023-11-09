#INGESTION LAMBDA
data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/processing/processing_handler.py" 
  output_path = "${path.module}/../function2.zip"  
}