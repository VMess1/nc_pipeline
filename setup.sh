#!/bin/bash
# Sets up the layers folder ready for deploying via terraform to AWS
mkdir layers3
mkdir layers3/python
mkdir layers3/python/src
mkdir layers3/python/src/storage
cp -r venv/lib/python3.11/site-packages/asn1crypto layers3/python
cp -r venv/lib/python3.11/site-packages/asn1crypto-1.5.1.dist-info layers3/python
cp -r venv/lib/python3.11/site-packages/python_dateutil-2.8.2.dist-info layers3/python
cp -r venv/lib/python3.11/site-packages/scramp layers3/python
cp -r venv/lib/python3.11/site-packages/scramp-1.4.4.dist-info layers3/python
cp -r venv/lib/python3.11/site-packages/six-1.16.0.dist-info layers3/python
cp -r venv/lib/python3.11/site-packages/pg8000 layers3/python
cp -r venv/lib/python3.11/site-packages/pg8000-1.30.2.dist-info layers3/python
cp -r src/storage/read_parquet.py layers3/python/src/storage
cp -r src/storage/store_timestamp.py layers3/python/src/storage
cp -r src/storage/write_database.py layers3/python/src/storage
cd layers3
zip -r layer_code3.zip python


# mkdir layers2
# mkdir layers2/python
# mkdir layers2/python/src
# mkdir layers2/python/src/processing
# # cp -r venv/lib/python3.11/site-packages/numpy layers2/python
# # cp -r venv/lib/python3.11/site-packages/numpy-1.26.1.dist-info layers2/python
# # cp -r venv/lib/python3.11/site-packages/numpy.libs layers2/python
# # cp -r venv/lib/python3.11/site-packages/pandas layers2/python
# # cp -r venv/lib/python3.11/site-packages/pandas-2.1.2.dist-info layers2/python
# # cp -r venv/lib/python3.11/site-packages/python_dateutil-2.8.2.dist-info layers2/python
# # cp -r venv/lib/python3.11/site-packages/pytz layers2/python
# # cp -r venv/lib/python3.11/site-packages/pytz-2023.3.post1.dist-info layers2/python
# # cp -r venv/lib/python3.11/site-packages/tzdata layers2/python
# # cp -r venv/lib/python3.11/site-packages/tzdata-2023.3.dist-info layers2/python
# # cp -r venv/lib/python3.11/site-packages/awswrangler layers2/python
# # cp -r venv/lib/python3.11/site-packages/awswrangler-3.4.1.dist-info layers2/python
# cp -r src/processing/dim_table_transformation.py layers2/python/src/processing
# cp -r src/processing/fact_table_transformation.py layers2/python/src/processing
# cp -r src/processing/read_write_files.py layers2/python/src/processing
# cd layers2
# zip -r layer_code2.zip python

# mkdir layers3/python
# cp -r venv/lib/python3.11/site-packages/pyarrow layers2/python
# cp -r venv/lib/python3.11/site-packages/pyarrow-14.0.0.dist-info layers2/python
# cd layers3
# zip -r layer_code3.zip python

# mkdir layers3/python
# cp -r venv/lib/python3.11/site-packages/pyarrow layers2/python
# cp -r venv/lib/python3.11/site-packages/pyarrow-14.0.0.dist-info layers2/python
# cd layers3
# zip -r layer_code3.zip python