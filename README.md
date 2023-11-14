# Northcoders Data Engineering Final Project
This Northcoders Data Engineering Final Project is a pipeline which allows a shop to transform an u SQL database of sales records to a structured data warehouse, hosted in AWS.

1. The pipeline's first lambda function collects both archive and new unique data entries by scanning the database every 2 minutes. It converts the data to CSV files which are stored in an s3 bucket and logs in CloudWatch.
2. This storage event triggers a second function which normalises the data and stores them in parquet format in a second s3 bucket.
3. Finally, every 2 minutes the second bucket is scanned for updates, which the pipeline converts back to SQL and adds to a data warehouse in star format.

## Prerequisites
Before you begin, ensure you have met the following requirements:
* You have installed the latest version of Python and set up a venv.
* Run make-requirements to install required packages.
* To run tests, the test SQL databases must be created:
    * Create a .env file in the nc_pipeline directory with the following information:
    ```
    TEST_HOST = 'localhost'
    TEST_DATABASE = 'oltp_test'
    USER = <local sql username>
    PASSWORD = <local sql password>
    ```
    <!-- do we need olap_test? -->
* To use the pipeline, aws CLI must be installed and the OLTP and OLAP credentials known.