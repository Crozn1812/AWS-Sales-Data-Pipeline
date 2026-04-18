# Architecture

## Overview

This project builds a simple data pipeline on AWS.

The system processes sales CSV files automatically after upload.

## Data Flow

1. A CSV file is uploaded to S3 (raw bucket)
2. S3 triggers a Lambda function
3. Lambda processes the file:

   * validate data
   * clean data
4. Clean data is saved to processed bucket
5. Bad data is saved to error bucket
6. Metadata is saved to DynamoDB
7. Logs are saved in CloudWatch

## Architecture Diagram

Store → S3 (raw) → Lambda → S3 (processed / error) → DynamoDB

## Why these services?

* **S3**: store files
* **Lambda**: process data automatically
* **DynamoDB**: store processing info
* **CloudWatch**: view logs
