

# Northcoders Data Engineering Final Project
<p align="justify">This Northcoders Data Engineering Final Project is a pipeline which transforms an SQL database  of a shops's sales records (in Online Transaction Processing "OLTP" format) to a structured data warehouse (in Online Analytical Processing "OLAP" format); all hosted in amazon web services (AWS). See the end of the ReadMe for images of the entity relationship diagrams (ERDs) for the initial and transformed databases.</p>

## The Pipeline
<p align="justify">1. The pipeline's "extraction" Lambda function collects both archive and new data entries by scanning the database periodically for updates. It converts new, unique data to CSV files which are stored in an S3 bucket; and logs in CloudWatch. The database credentials are stored in Secrets Manager; and Systems Manager is used to store timestamps.
2. Any bucket upload event triggers a second "processing" Lambda function which transforms and normalises the data and stores them in parquet format in a second S3 bucket.
3. Finally, a third "storage" Lambda function scans the second bucket periodically for updates, which the pipeline converts back to SQL and loads to a data warehouse in star format.

The entire pipeline infrastructure is managed using Terraform.</p>

## Prerequisites
To use the pipeline, ensure you have met the following requirements:
* You have installed the latest version of Python and set up a venv.
* To install required packages:
    ```
    make requirements
    ```

## Using the Pipeline
* To run tests, the test SQL databases must be created.

    * Create a .env file in the nc_pipeline directory with the following information:
    ```
    TEST_HOST = 'localhost'
    TEST_DATABASE = 'oltp_test'
    TEST_DATA_WAREHOUSE = 'olap_test'
    USER = <local sql username>
    PASSWORD = <local sql password>
    ```

    * Create initial otlp test database:
    ```
    psql -f tests/test_extraction/data/test-extraction.sql
    ```

    * Create transformed olap test database:
    ```
    psql -f tests/test_storage/data/test-insertion.sql
    ```
    

* To use the pipeline, the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) must be installed and the OLTP and OLAP database credentials stored in AWS Secrets Manager.

## Contributors
* Tom Avery [@averz87](https://github.com/averz87) 🧮
* Dylan Hickman-Singh [@dylanhs33](https://github.com/dylanhs33) ⚽
* Philippa Clarkson [@philupa](https://github.com/philupa) 🦔
* Philip Taylor [@phil-taylor-sj](https://github.com/phil-taylor-sj) ♟️
* Victoria Messam [@VMess1](https://github.com/VMess1) 👾


OTLP Database ERD:
![SampleDB](https://github.com/VMess1/nc_pipeline/assets/129286879/47f15fb5-1218-4f0f-89c3-3a245e5062e8)

OLAP Database ERD:
![SampleDW-Sales](https://github.com/VMess1/nc_pipeline/assets/129286879/786e2668-e611-40b5-bd8c-0f8687f126a6)


