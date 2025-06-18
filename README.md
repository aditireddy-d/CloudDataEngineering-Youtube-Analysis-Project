# YouTube Trending Analysis – Cloud Data Engineering Project

## Overview

This project focuses on building a **cloud-native data engineering pipeline** to analyze YouTube trending video data across multiple countries. The goal is to process both **structured and semi-structured data** using a scalable and secure architecture on **AWS**, enabling insightful analytics directly on cloud-stored data.

## Project Objectives

- **Data Ingestion**: Ingest CSV and JSON files for multiple regions
- **ETL Processing**: Use AWS Glue to clean, transform, and prepare the data
- **Centralized Storage**: Store raw and processed files in Amazon S3
- **Scalable Queries**: Run queries directly on S3 using AWS Athena
- **Automation**: Optionally trigger Glue jobs using AWS Lambda
- **Security**: Manage service access using AWS IAM roles

## Tools & Services Used

- **Amazon S3** – Data lake for raw and transformed files  
- **AWS Glue** – Serverless ETL for schema mapping and transformation  
- **AWS Lambda** – (Optional) Trigger ETL on file uploads  
- **AWS Athena** – Query engine for SQL on S3  
- **AWS IAM** – Identity and access control  

## Dataset Used

**Source**: [Kaggle – YouTube Trending Video Dataset](https://www.kaggle.com/datasets/datasnaek/youtube-new)  
Includes:
- Daily CSV files by region (e.g., USvideos.csv, INvideos.csv)
- A category mapping file in JSON format per region
- Fields: `video_id`, `title`, `channel_title`, `publish_time`, `views`, `likes`, `dislikes`, `comment_count`, etc.



