# Sparkify: Building a Data Warehouse on Redshift

## Background Information
Sparkify is a new startup that is looking to revolutionize music streaming through the use of its Sparkify Music App. 

As the number of users on Sparkify has grown, the startup is no longer able to manage the data on their local servers. In order to scale up, Sparkify has decided to migrate their data storage to the cloud.

## Problem Statement

As a data engineer at Sparkify, I have been tasked with building an ETL pipeline that extracts JSON data from S3 buckets, loads the data into staging tables on Redshift, and transforms the data into a set of fact and dimension tables that can be used for business analytics. 

## Contents of Repository
- `dwh.cfg` - This configuration file stores the credentials for the IAM Role and Redshift Cluster. It also contains the S3 locations for the raw data.
- `sql_queries.py` - This file contains the SQL code to DROP existing tables, CREATE tables, COPY data from S3 into staging tables, and INSERT data from the staging tables into the fact and dimension tables.
- `create_tables.py` - This file contains the Python code to connect to a Redshift cluster. This code will drop any existing tables before creating the staging tables and fact and dimension tables.
- `etl.py` - This file contains the Python code to extract data from the S3 buckets and COPY the data into staging tables on Redshift. Once the data has been properly loaded into the staging tables, it is then inserted into fact and dimension tables that can be used to perform analytics.

## Instructions
1. Launch a Redshift Cluster.
2. Fill in the blanks in `dwh.cfg` to provide information about your Redshift cluster and IAM role.
2. In the command line, run `create_tables.py`.
3. In the command line, run `etl.py`.
4. Use the Query Editor in the Amazon Redshift console to query your database. It will be found under the *public* schema.
5. Delete Redshift cluster

**NOTE**: This project was completed for Udacity's Data Engineering Nanodegree