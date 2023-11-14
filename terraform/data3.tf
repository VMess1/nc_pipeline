#WAREHOUSE LAMBDA
data "archive_file" "lambda3" {
  type        = "zip"
  source_dir = "${path.module}/../src/storage/"  #path may be different
  output_path = "${path.module}/../function3.zip"  
}