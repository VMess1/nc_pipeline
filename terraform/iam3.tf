# # Setting up lambda role
# resource "aws_iam_role" "lambda_warehouse_role" {
#     name_prefix = "role-${var.lambda_warehouse}"
#     assume_role_policy = jsonencode(
#     {
#         "Version": "2012-10-17",
#         "Statement": [
#             {
#                 "Effect": "Allow",
#                 "Action": [
#                     "sts:AssumeRole"
#                 ],
#                 "Principal": {
#                     "Service": [
#                         "lambda.amazonaws.com"
#                     ]
#                 }
#             }
#         ]
#     })
# }

# # Policy document for lambda_transformation to write to cloudwatch
# data "aws_iam_policy_document" "write_to_cw_policy_3" {
#   statement {
#     actions = ["logs:CreateLogGroup",
#                 "logs:GetLogEvents"]
#     resources = [
#       "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*",
#     ]
#   }
#   statement {
#     actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]
#     resources = [
#       "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_warehouse}:*"
#     ]
#   }
# }
# # Attaches cw policy document to policy
# resource "aws_iam_policy" "cw_policy_3" {
#     name_prefix = "cw-policy-${var.lambda_warehouse}"
#     policy = data.aws_iam_policy_document.write_to_cw_policy_3.json
# }
# #Attaches cw policy to the lambda_warehouse_roles
# resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment3" {
#     role = aws_iam_role.lambda_warehouse_role.name
#     policy_arn = aws_iam_policy.cw_policy_3.arn
# }

# # Policy document for allowing lambda to put data into s3 bucket and read from
# data "aws_iam_policy_document" "s3_document_3" {
#   statement {
#     actions = ["s3:GetObject"]  #potentially need to add put object
#     resources = ["${aws_s3_bucket.transformation_bucket.arn}/*"]
#   }
# } 

# # Attaches s3 policy document to policy
# resource "aws_iam_policy" "s3_policy_3" {
#     name_prefix = "s3-policy-${var.lambda_warehouse}"
#     policy = data.aws_iam_policy_document.s3_document_3.json
# }

# # Attaches s3 policy to the lambda_warehouse_role
# resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment_3" {
#     role = aws_iam_role.lambda_warehouse_role.name
#     policy_arn = aws_iam_policy.s3_policy_3.arn
# }