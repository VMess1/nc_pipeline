#!/bin/bash


##Needs updating to correctly make the layers
mkdir layers
cp -r venv/lib/python3.11/site-packages/pg8000-1.30.2.dist-info layers/python
cp -r venv/lib/python3.11/site-packages/asn1crypto layers/python
cp -r venv/lib/python3.11/site-packages/asn1crypto-1.5.1.dist-info layers/python
cp -r venv/lib/python3.11/site-packages/python_dateutil-2.8.2.dist-info layers/python
cp -r venv/lib/python3.11/site-packages/scramp layers/python
cp -r venv/lib/python3.11/site-packages/scramp-1.4.4.dist-info layers/python
cp -r venv/lib/python3.11/site-packages/six-1.16.0.dist-info layers/python
cp -r venv/lib/python3.11/site-packages/pg8000 layers/python