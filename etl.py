import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from Pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import job
import logging
import boto3

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
gluecontext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"])

# set params
region= "us-east-2"
source_file_path="s3://billionaires-06-25/raw_data/billionaires_data/"
target_file_path="s3://billionaires-06-25/Processed_data/"
glueCrawlerName="data_crawler"

def process_csv_files(source_file_path: str, target_file_path: str):
    
    s3bucket_node1=glueContext.create_dynamic_frame.from_options(
        format_options = {
            "quoteChar": '"',
            "withHeader": True,
            "separator": ",",
            "optimizedPerformance":False
        },
        connection_type="s3",
        format="csv",
        connection_options={
            "paths":[
                source_file_path
            ],
            "recurse": True,
        },
        transformation_ctx="s3bucket_node1",
    )

    s3bucket_node3=glueContext.write_dynamic_frame.from_options(
        frame=s3bucket_node1,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "paths": target_file_path,
            "partitionKeys":[],
        },
        format_options={"compression":"gzip"},
        transformation_ctx="s3bucket_node3",
    )

def start_glue_crawler(glueCrawlerName, region:str):
    glueClient=boto3.client('glue', region_name=region)

    try:
        results = glueClient.start_crawler(Name = glueCrawlerName)
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

