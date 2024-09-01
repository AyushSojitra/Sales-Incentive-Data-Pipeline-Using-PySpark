# PySpark Data Pipeline Project

This project involves the creation of a data pipeline using PySpark, Hadoop, and MySQL on a Debian-based machine. The pipeline's primary goal is to analyze sales data, generate insights into customer behavior, and reward top-performing salespeople based on monthly performance. 

## Project Overview

The pipeline processes sales data stored in AWS S3, combines it with dimensional data from a MySQL database, and generates data marts to provide actionable insights for data science and business intelligence teams. This helps identify patterns in customer purchases and sales team performance, facilitating customer retention strategies and rewarding high-performing sales staff.

## Objectives

1. **Customer Sales Insights**: 
   - Analyze monthly sales data to identify trends in customer purchasing behavior.
   - Detect customers whose sales are declining, enabling targeted retention strategies.

2. **Sales Team Performance**: 
   - Track monthly sales performance by salesperson at each store.
   - Identify top performers and calculate incentives based on a 1% sales increase.

## Data Pipeline Steps

### 1. Setup Environment

- **Spark Setup**: Configured Apache Spark with Hadoop on a Debian-based machine.
- **MySQL Setup**: Installed and configured MySQL to host dimension tables used in the pipeline.

### 2. Data Ingestion

- **Source Data**: Sales data (fact table) stored in an AWS S3 bucket.
- **Validation**: Identified valid files containing all mandatory columns for processing. Invalid files were marked for correction.
- **Loading Data**: Loaded valid data directly from the S3 bucket into a Spark DataFrame.

### 3. Data Processing

- **Combine Data**: Merged all valid sales data files into a single DataFrame.
- **Data Enrichment**: Joined the sales data with dimension tables from the MySQL database to enrich the dataset.

### 4. Data Mart Creation

- **Customer Data Mart**: 
  - Extracted relevant columns from the enriched DataFrame.
  - Created a data mart focused on customer sales insights and trends.
  
- **Sales Team Data Mart**: 
  - Extracted columns related to sales performance.
  - Created a data mart for analyzing sales team performance at each store.

### 5. Data Storage

- **S3 Storage**: 
  - Stored both data marts in an AWS S3 bucket.
  - Partitioned the data and stored it in Parquet format for efficient querying and analysis.

### 6. Transformation and Analysis

- **Monthly Sales and Incentives**:
  - Performed transformations on the data marts to generate insights into monthly customer sales.
  - Calculated incentive amounts for the top-performing salespersons.
  - Created a table in MySQL to store these results for easy access by business teams.

## How to Run the Project

### Prerequisites

- **Apache Spark**: Installed and configured with Hadoop on a Debian-based machine.
- **MySQL**: Installed and running with necessary dimension tables.
- **AWS S3**: Access to S3 bucket containing the sales data.

### Setup Instructions

1. **Clone the Repository**:
    ```bash
    git clone https://github.com/yourusername/pyspark-data-pipeline.git
    cd pyspark-data-pipeline
    ```

2. **Configure Environment**:
   - Ensure Spark and Hadoop are properly configured on your machine.
   - Set up MySQL and create necessary dimension tables.

3. **Install Dependencies**:
   - Install required Python libraries using pip:
    ```bash
    pip install -r requirements.txt
    ```

4. **Run the Data Pipeline**:
   - Execute the PySpark script to start the data pipeline:
    ```bash
    spark-submit data_pipeline.py
    ```

## Technologies Used

- **PySpark**: For data processing and transformation.
- **Hadoop**: Underlying file system support for Spark.
- **MySQL**: Database for dimension tables and final result storage.
- **AWS S3**: Storage for raw sales data and final data marts.
- **Parquet**: Storage format for efficient data retrieval and analysis.
