# Failure Scenarios

## 1. Wrong file format

What happens:

* File has wrong columns

Detection:

* Lambda check columns

Action:

* Move to error bucket

---

## 2. Bad data

What happens:

* Missing values or negative numbers

Detection:

* Data validation

Action:

* Separate bad records

---

## 3. Lambda error

What happens:

* Function fails

Detection:

* CloudWatch logs

Action:

* Fix and re-run
