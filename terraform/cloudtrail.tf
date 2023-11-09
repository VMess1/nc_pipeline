data "aws_s3_bucket" "ingestion_bucket" {
  bucket = "nc-group3-ingestion-bucket"
}

resource "aws_cloudtrail" "data_ingested" {
    s3_bucket_name = "ingestion_bucket"
    name = "data_ingested_cloudtrail"
    event_selector {
        read_write_type           = "WriteOnly"
        include_management_events = true
    data_resource {
        type = "AWS::S3::Object"
        values = ["${data.aws_s3_bucket.ingestion_bucket.arn}/"]
        }
    }
}