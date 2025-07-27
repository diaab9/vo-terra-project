# Mongo to S3 ETL using AWS Glue & Terraform

## 📌 Overview

This project implements a complete ETL pipeline that:

1. Connects to a MongoDB Atlas cluster.
2. Extracts and flattens JSON documents.
3. Converts the data into a Parquet file.
4. Uploads it to an S3 bucket.
5. Uses AWS Glue to catalog the data.
6. Enables querying with Athena.

All AWS infrastructure is provisioned using **Terraform**.

---

## 📂 Project Structure

```bash
mongo_to_s3_etl/
├── main.tf               
├── provider.tf     
├── variables.tf          
├── terraform.tfvars   
├── README.md             
└── mongo_to_s3.py        
