# Northcoders Data Engineering Final Project
This Northcoders Data Engineering Final Project is a pipeline which allows a shop to transform an under-managed SQL database (see Initial Database ERD image below) of sales records to a structured data warehouse (see Transformed Database ERD image below), hosted in AWS.

1. The pipeline's first lambda function collects both archive and new unique data entries by scanning the database every 2 minutes. It converts the data to CSV files which are stored in an s3 bucket and logs in CloudWatch.
2. This storage event triggers a second function which normalises the data and stores them in parquet format in a second s3 bucket.
3. Finally, every 2 minutes the second bucket is scanned for updates, which the pipeline converts back to SQL and adds to a data warehouse in star format.

## Prerequisites
Before you begin, ensure you have met the following requirements:
* You have installed the latest version of Python and set up a venv.
* Run make-requirements to install required packages.


## Using the Pipeline
* To run tests, the test SQL databases must be created.

    * Create a .env file in the nc_pipeline directory with the following information:
    ```
    TEST_HOST = 'localhost'
    TEST_DATABASE = 'oltp_test'
    USER = <local sql username>
    PASSWORD = <local sql password>

    TEST_HOST = 'localhost'
    TEST_DATABASE = 'olap_test'
    USER = <local sql username>
    PASSWORD = <local sql password>
    ```

    * Create otlp test database:
    ```
    psql -f tests/test_extraction/data/test-extraction.sql
    ```

    * Create olap test database:
    ```
    psql -f tests/test_storage/data/test-insertion.sql
    ```
    

* To use the pipeline, the AWS CLI must be installed and the OLTP and OLAP credentials known.

## Contributors
[@averz87](https://github.com/averz87) 🧮
[@dylanhs33](https://github.com/dylanhs33) ⚽
[@philupa](https://github.com/philupa) 🦔
[@phil-taylor-sj](https://github.com/phil-taylor-sj) ♟️
[@VMess1](https://github.com/VMess1) 👾


Inital Database ERD:
![SampleDB](https://github.com/VMess1/nc_pipeline/assets/129286879/47f15fb5-1218-4f0f-89c3-3a245e5062e8)

Transformed Database ERD:
![SampleDW-Sales](https://github.com/VMess1/nc_pipeline/assets/129286879/786e2668-e611-40b5-bd8c-0f8687f126a6)
