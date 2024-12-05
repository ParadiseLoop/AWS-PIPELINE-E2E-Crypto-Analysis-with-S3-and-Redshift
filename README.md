# AWS PIPELINE - E2E CRYPTO ANALYSIS WITH S3 AND REDSHIFT

Welcome to the **AWS PIPELINE - E2E Crypto Analysis with S3 and Redshift** repository. This project provides an end-to-end (E2E) automated solution for analyzing cryptocurrency data, specifically Bitcoin (BTC) and Monero (XMR). By leveraging AWS cloud services, the pipeline automates data ingestion, cleaning, transformation, and loading into a data warehouse (Amazon Redshift), enabling advanced querying and analytics.

---

## Project Overview

The cryptocurrency market is volatile and fast-paced, requiring accurate, real-time analytics to support informed decision-making. This repository contains a scalable, automated pipeline that processes raw market and Google Trends data into structured datasets for deeper analysis.

### Key Features
- **End-to-End Automation:** Streamlined workflows from raw data ingestion to visualization.
- **Scalable Architecture:** Parallel processing for multiple cryptocurrencies using AWS.
- **Customizable Transformations:** Choose between AWS Glue ETL or Amazon Step Functions for data processing.

---

## Pipeline Architecture

### Input Data
1. **Price Data (BTC/EUR, XMR/EUR):**
   - **Columns:**
     - `Date` (e.g., "03/12/2024")
     - `Price` (e.g., 145.4)
   - **Challenges:** Handle missing values (value `-1`) by:
     - Dropping rows.
     - Filling with the previous value.
     - Filling with a rolling mean/median.

2. **Google Trends Data:**
   - **Columns:**
     - `Week` (e.g., "2024-12-01")
     - `Interest` (integer between 0 and 100).
   - **Granularity Mismatch:** Weekly data smoothed using moving averages to align with daily price data.

---

### Workflow
1. **Data Preparation**
   - **Source:** Raw data uploaded to S3 bucket (`raw`).
   - **Step 1:** Data cleaning and transformation (e.g., handling missing values, smoothing).
   - **Step 2:** Save processed data in Parquet format to S3 bucket (`silver`).

2. **Analysis and Unification**
   - Compute 10-day moving averages for prices.
   - Perform joins between price data and Google Trends data to produce unified datasets.

3. **Redshift Integration**
   - Load unified datasets into Amazon Redshift for SQL-based analytics.

---

## AWS Services Used

### Core Services
- **Amazon S3:** Data storage (raw, silver, and processed outputs).
- **AWS Glue ETL / Amazon EMR:** Flexible options for data cleaning and transformation.
- **Amazon Redshift:** Centralized data warehousing for analysis.

### Orchestration
- **AWS Step Functions:** Coordinate pipeline workflows for BTC and XMR, ensuring efficient parallel execution and monitoring.

---

## How to Use

1. **Set Up AWS Environment**
   - Configure S3 buckets (`raw`, `silver`, `gold`).

2. **Run the Pipeline**
   - Upload raw data to `raw` bucket.
   - Trigger Step Functions to start the pipeline.

3. **Analyze Results**
   - Query the processed data in Redshift.
   - *(Optional)* Visualize results using QuickSight dashboards.

---

## Benefits to CryptoData Insights

- **Automation:** Reduces manual workload, increasing efficiency and reducing errors.
- **Scalability:** Supports large-scale parallel processing and easy extension to new cryptocurrencies.
- **Actionable Insights:** Provides enriched datasets ready for advanced analytics and visualization.
