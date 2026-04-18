# Architecture Decisions

## ADR-1: Use Lambda

Context: Need to process files automatically

Decision: Use Lambda

Other options:

* EC2 (need to manage server)
* ECS (more complex)

Result:

* Easy to use
* Auto scale

---

## ADR-2: Use S3

Context: Need to store files

Decision: Use S3 with 3 buckets:

* raw
* processed
* error

Result:

* Easy to manage
* Clear data flow

---

## ADR-3: Use DynamoDB

Context: Need to track file status

Decision: Use DynamoDB

Result:

* Fast
* Simple
