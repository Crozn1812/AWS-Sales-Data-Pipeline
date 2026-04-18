# Shopmart Sales Data Pipeline (AWS)

## Overview

This project builds an event-driven data pipeline on AWS to process retail sales data.

When a CSV file is uploaded to Amazon S3, the system automatically validates, cleans, and transforms the data, then stores results for analytics.

---

## Architecture

### Services Used

* Amazon S3 (Raw, Processed, Error storage)
* AWS Lambda (Data processing)
* Amazon DynamoDB (Metadata tracking)
* Amazon CloudWatch (Logging)
* AWS CDK (Infrastructure as Code)

### Data Flow

1. CSV file uploaded to S3 Raw bucket
2. S3 triggers Lambda function
3. Lambda processes the file:

   * Validate schema
   * Clean data
   * Split valid and invalid records
4. Output:

   * Clean data → Processed bucket
   * Invalid data → Error bucket
   * Metadata → DynamoDB

---

## Key Features

* Event-driven architecture (S3 → Lambda)
* Data validation and cleaning
* Error handling pipeline
* Partitioned data storage
* Metadata tracking with DynamoDB
* Infrastructure as Code with AWS CDK

---

## Business Metrics

The pipeline computes important metrics:

* Daily revenue
* Top products
* Payment success rate
* Orders per customer

---

## Project Structure

```text
aws-sales-data-pipeline/
├── infrastructure/
├── src/
├── tests/
├── evidence/
├── docs/
├── app.py
├── run_local.py
```

---

## Deployment

### Prerequisites

* AWS CLI configured
* AWS CDK installed

### Deploy

```bash
cdk bootstrap
cdk deploy
```

---

## Testing

```bash
pytest tests/ -v
```

✔ 5 test cases included:

* Happy path
* Missing data
* Duplicate records
* Invalid values
* Error handling

---

## Example Output

### DynamoDB Metadata

| file_id               | clean_records | error_records | status    |
| --------------------- | ------------- | ------------- | --------- |
| sample_sales_data.csv | 99            | 4             | COMPLETED |

---

## Execution Evidence

### Unit Tests

![pytest](./evidence/01_pytest.png)

### Deployment

![deploy](./evidence/02_deploy.png)

### Processed Data (S3)

![processed](./evidence/03_processed.png)

### Error Handling (S3)

![error](./evidence/04_error.png)

### Metadata Tracking (DynamoDB)

![dynamodb](./evidence/05_dynamoDB.png)

### Business Metrics

![business](./evidence/06_business_metrics.png)

### CloudWatch Logs

![cloudwatch](./evidence/07_cloudwatch.png)

---

## Design Decisions

* Lambda is used for simple and scalable processing
* S3 is used for storage and event trigger
* DynamoDB is used for fast metadata tracking

---

## Security & IAM

* Principle of least privilege applied
* S3 buckets block public access
* IAM policies scoped to required resources only

---

## Reliability

* Bad records are separated into error bucket
* Logs are available in CloudWatch
* Future improvement: retry and idempotency

---

## Additional Documentation

* [Architecture Design](./docs/architecture.md)
* [Architecture Decision Records](./docs/adrs.md)
* [Failure Scenarios](./docs/failure-scenarios.md)

---

## Known Gaps and Future Improvements

Due to time constraints, some features are not fully implemented:

* Alerting (CloudWatch Alarm + SNS)
* Retry mechanism
* Idempotency for duplicate processing
* BI integration (Athena / QuickSight)

---

## Cleanup

```bash
cdk destroy
```
