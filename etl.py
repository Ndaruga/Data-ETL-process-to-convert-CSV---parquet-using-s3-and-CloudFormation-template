import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
import boto3

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"])

# set params
region= ""
source_file_path="{SOURCE FILE PATH}"
target_file_path="{TARGET FILE PATH}"
glueCrawlerName="{CRAWLER NAME}"

def process_csv_files(source_file_path: str, target_file_path: str):
    
    # Script generated for node S3 bucket
    S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": '"', 
            "withHeader": True, 
            "separator": ",",
            "optimizePerformance": False,
            },
        connection_type="s3",
        format="csv",
        connection_options={
            "paths": [
                source_file_path
            ],
            "recurse": True,
        },
        transformation_ctx="S3bucket_node1",
    )

    # Script generated for node S3 bucket
    S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
        frame=S3bucket_node1,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": target_file_path,
            "partitionKeys": [],
        },
        format_options={"compression": "gzip"},
        transformation_ctx="S3bucket_node3",
    )

def start_glue_crawler(glueCrawlerName, region:str):
    glueClient=boto3.client('glue', region_name=region)

    try:
        results = glueClient.start_crawler(Name= glueCrawlerName)
        return results
    except Exception as startCrawlerException:
        logging.error("An error occured while starting the Glue crawler: {}".format(glueCrawlerName))
        raise(startCrawlerException)


def main():
    """
    main : performs the following actions
    1. ingest CSV data files into Glue Job
    2. start crawler to catalog the data
    """

    logging.info("Data pipeline: STARTED")

    # Ingest csv file(s) to glue
    logging.info("Glue ETL process: STARTED")
    process_csv_files(source_file_path=source_file_path, target_file_path=target_file_path)

    # 2 Start crawler to catalog the data
    logging.info("Crawler: STARTED")
    start_glue_crawler(glueCrawlerName=glueCrawlerName, region=region)

    logging.info("Data Pipeline: FINISHED")
    job.commit()

if __name__ == "__main__":
    main()

