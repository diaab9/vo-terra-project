resource "aws_s3_bucket" "glue_bucket" {
  bucket = var.s3_bucket_name
}

resource "aws_glue_catalog_database" "catalog_db" {
  name = var.glue_db_name
}

resource "aws_glue_job" "etl_job" {
  name     = var.glue_job_name
  role_arn = ""

  command {
    name            = "glueetl"
    script_location = var.script_s3_path
    python_version  = "3"
  }

  glue_version       = "4.0"
  number_of_workers  = 2
  worker_type        = "G.1X"
  max_retries        = 1

  default_arguments = {
    "--job-language" = "python"
  }

  depends_on = [aws_glue_catalog_database.catalog_db]
}

resource "aws_glue_crawler" "s3_crawler" {
  name          = var.crawler_name
  role          = "" 
  database_name = aws_glue_catalog_database.catalog_db.name

  s3_target {
    path = var.s3_output_path
  }

  depends_on = [aws_glue_catalog_database.catalog_db]
}