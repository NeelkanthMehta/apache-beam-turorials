# apache-beam-turorials
exploring Apache-Beam framework

## Exercuse 1: developing Batch Processing pipeline

In this case, we are assuming an online food delivery company that has its daily data transactional order data generated, needs to be cleaned, transformed and stored for reporting.

Currently contains two files:
1. e2e-apache_beam-pipeline.py --> pipeline code
2. food_daily.csv ---------------> an example of daily order data that needs to be processed each day

columns: customer_id, date, time, order_id, items, amount, mode, restaurant, status, feedback

## Approach using following components of Google Cloud Platform:
Load the csv files into Cloud storage --> Processing through Cloud DataFlow (using Apache-Beam on python) --> store in BigQuery --> use Data Studio (on GCP)
