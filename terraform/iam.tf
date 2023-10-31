# Setting up lambda role
resource "aws_iam_role" "lambda_ingestion_role" {
    name_prefix = "role-${var.lambda_ingestion}"
    assume_role_policy = jsonencode(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    })
}

# Policy document for lambda_ingestion to write to cloudwatch
data "aws_iam_policy_document" "write_to_cw_policy" {
  statement {
    actions = ["logs:CreateLogGroup",
                "logs:GetLogEvents"]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*",
    ]
  }
  statement {
    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_ingestion}:*"
    ]
  }
}

# Policy document for allowing lambda to put data into s3 bucket
data "aws_iam_policy_document" "s3_document" {
  statement {
    actions = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.ingestion_bucket.arn}/*"]
  }
}

# Attaches s3 policy document to policy
resource "aws_iam_policy" "s3_policy" {
    name_prefix = "s3-policy-${var.lambda_ingestion}"
    policy = data.aws_iam_policy_document.s3_document.json
}

# Attaches cw policy document to policy
resource "aws_iam_policy" "cw_policy" {
    name_prefix = "cw-policy-${var.lambda_ingestion}"
    policy = data.aws_iam_policy_document.write_to_cw_policy.json
}

# Attaches s3 policy to the lambda_ingestion_role
resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.lambda_ingestion_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}

#Attaches cw policy to the lambda_ingestion_role
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.lambda_ingestion_role.name
    policy_arn = aws_iam_policy.cw_policy.arn
}
