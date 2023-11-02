
# resource "null_resource" "my_lambda_buildstep" {
#     triggers = {
#     handler      = "${base64sha256(file("../src/tftest/testfunc.py"))}"
#     requirements = "${base64sha256(file("../src/tftest/requirements.txt"))}"
#     build        = "${base64sha256(file("../src/tftest/build.sh"))}"
#   }

#   provisioner "local-exec" {
#     command = "${path.module}../src/tftest/build.sh"
#   }
# }

# data "archive_file" "my_lambda_function_with_dependencies" {
#   source_dir  = "${path.module}../src/tftest"
#   output_path = "${path.module}/../testlambdafunc.zip"
#   type        = "zip"

#   depends_on = [null_resource.my_lambda_buildstep]
# }


#lambda dependencies
# resource "aws_lambda_layer_version" "layer_dependencies" {
#   layer_name = "layer_dependencies"
#   s3_bucket = aws_s3_bucket.code_bucket.id
#   s3_key = aws_s3_object.layer_code.key
#   compatible_runtimes = ["python3.11"]
# }

# resource "null_resource" "lambda_layer" {
#   triggers = {
#     requirements = filesha1(local.requirements_path)
#   }
#   # the command to install python and dependencies to the machine and zips
#   provisioner "local-exec" {
#     command = <<EOT
#       set -e
#       apt-get update
#       apt install python3 python3-pip zip -y
#       rm -rf python
#       mkdir python
#       pip3 install -r ${local.requirements_path} -t python/
#       zip -r ${local.layer_zip_path} python/
#     EOT
#   }
# }


# locals {
#   layer_zip_path    = "layer.zip"
#   layer_name        = "layer_dependencies"
#   requirements_path = "${path.module}/../requirements.txt"
# }


