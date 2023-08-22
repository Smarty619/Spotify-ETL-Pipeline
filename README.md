# Spotify-ETL-Pipeline
A End to End ETL Pipeline Project

## Description
This project aims to provide a scalable ETL (Extract, Transform, Load) pipeline using the Spotify API on AWS.The pipeline retrieves data from the Spotify API, performs necessary transformations to format the data as per the requirements, and loads it into an AWS data store and then to Snowflake(DataWarehouse) for further processing.This ETL pipeline hasbeen orchestrated using the Workflow Management Tool Apache-Airflow.

The pipeline is built using a combination of popular technologies, which are mentioned below. 

## Technologies Used

* Python
* AWS EC2
* AWS S3
* Apache-Airflow
* Snowflake

## Project Work-Flow
* Setting up an Linux EC2 instance with the necessary security group and IAM role.
* Installing required software such as Python and Apache Airflow on the EC2 instance.
* Writing Python code to create a DAG for the Spotify ETL process
* Extracting and transforming data from Spotify Api.
* Uploading the transformed data to an S3 bucket.
* To configure Snowflake Storage Integration with AWS S3, created necessary IAM roles and Policies on AWS console and configured storage integration on Snowsight.
* The connection has been established and the data in the S3 has been staged first and then copied into a table in Snowlflake where further queries hasbeen performed on top of the stored data.
