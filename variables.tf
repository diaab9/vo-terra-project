variable "glue_job_name" {
  default = "mongo_to_s3_etl"
}

variable "s3_bucket_name" {
  default = "vo-glue-scripts"
}

variable "script_s3_path" {
  default = "s3://vo-glue-scripts/scripts/mongo_to_s3.py"
}

variable "glue_db_name" {
  default = "mongo_s3_db"
}

variable "crawler_name" {
  default = "mongo_s3_crawler"
}

variable "s3_output_path" {
  default = "s3://vo-glue-scripts/output/"
}