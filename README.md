# STEDI Human Balance Analytics: A Udacity Data Engineering Project

## Introduction

This repository houses the work completed for the STEDI Human Balance Analytics Udacity project.

## Project Implementation
The project was divided into several stages:

**Data Acquisition**: AWS S3 directories were created to simulate data coming from various sources. These directories served as landing zones for customer, step trainer, and accelerometer data.

**Data Sanitization**: AWS Glue Studio jobs were written to sanitize customer and accelerometer data from their respective landing zones. The sanitized data was then stored in a trusted zone.

**Data Verification**: AWS Athena was used to query and verify the data in the Glue customer_trusted table.

**Data Curation**: Additional Glue jobs were written to further sanitize the customer data and create a curated zone that only included customers who have accelerometer data and agreed to share their data for research.

**Data Streaming**: Glue Studio jobs were created to read the Step Trainer IoT data stream and populate a trusted zone Glue table.

**Data Aggregation**: Lastly, an aggregated table was created that matched Step Trainer readings and the associated accelerometer reading data for the same timestamp.


## Results and Conclusion
As a result of the project, several AWS Glue and Glue Studio jobs were successfully developed and executed, providing a curated and aggregated dataset ready for machine learning modeling. Screenshots of the queried data in Athena are provided as proof of the successful completion of the tasks.

The project demonstrated how a data lakehouse solution can be effectively built on AWS using Spark and Python. It also provided exposure to Glue Studio jobs and how they are created and managed. It also underscored the importance of data sanitization and data quality management.
