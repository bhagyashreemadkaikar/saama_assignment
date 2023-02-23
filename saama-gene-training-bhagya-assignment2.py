import os,io
import sys
import pyspark
import boto3
import pandas as pd
import zipfile,gzip
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from time import sleep
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_client=boto3.client('s3')
glue_client=boto3.client('glue',region_name="ap-south-1")
athena_client=boto3.client('athena',region_name="ap-south-1")
s3_bucket_name='saama-gene-training-data-bucket'
s3=boto3.resource('s3')
my_bucket=s3.Bucket(s3_bucket_name)
inbound_path='BhagyaM/inbound/'
S3_UNZIPPED_FOLDER='BhagyaM/temp/'
landing_path='BhagyaM/landing/'
zip_extn='.zip'
gz_extn='.gz'
csv_extn='.csv'
def extract_zip_file(frm_dir,to_dir):
    for file in my_bucket.objects.filter(Prefix=frm_dir):
        file1=file.key
        if file1.endswith(zip_extn) is True:
            zip_filepath=f's3://{s3_bucket_name}/{file1}'
            zip_obj = s3.Object(bucket_name=s3_bucket_name, key=file1)
            buffer = BytesIO(zip_obj.get()["Body"].read())
            z = zipfile.ZipFile(buffer)
            for filename in z.namelist():
                print(filename)
                file_info = z.getinfo(filename)
                response = s3_client.put_object(
                    Body=z.open(filename).read() ,
                    Bucket=s3_bucket_name,
                    Key=f'{to_dir}{filename}'
                )
                print('unzip is completed')
                
def move_file_to_landing(frm_dir,to_dir):
    for file in my_bucket.objects.filter(Prefix=frm_dir):
        file1=file.key
        if file1.endswith(gz_extn) is True:
            print("Start compressing gz files.")
            file_name = file1.split('/')[2].split('.gz')[0]
            folder_name=file_name.split('_')[1].split('.')[0]
            file_nm=file1.split('/')[2]
            obj = s3.Object(bucket_name=s3_bucket_name ,key=f'{frm_dir}{file_nm}')
            with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzipfile:
                file_name=file_name.split('_')[0]
                content = gzipfile.read()
                s3_client.put_object(Body=content,Bucket=s3_bucket_name, Key=f'{to_dir}{folder_name}/{file_name}.csv')

def create_crawler():
    print('Start to Create crawler!...')
    for file in my_bucket.objects.filter(Prefix=landing_path):
        file1=file.key
        if(file1.endswith(csv_extn)) is True:
            print(file1)
            file_name = file1.split('/')[3]
            crawler_name=f'saama-gene-training-bhagya-assignment2-{file_name}'
            crawler_details = glue_client.list_crawlers()
            response = glue_client.create_crawler(
                Name=crawler_name,
                Role='saama-gene-training-glue-service-role',
                DatabaseName='saama-gene-training-data',
                Targets={
                    'S3Targets': [
                        {
                            'Path': f's3://{s3_bucket_name}/{file1}'
                        }
                    ]
                },
                TablePrefix=f'saama-gene-training-bhagyam-assign2-'
            )
            print(f"{crawler_name}  - Crawler is created successfully....")
            run_crawler(crawler_name)
#Run the crawlers
def run_crawler(crawler_name):
    response = glue_client.start_crawler(
        Name=crawler_name
    )
    sleep(2*60)
    print("Successfully started crawler")
    response_get = glue_client.get_crawler(Name=crawler_name)
    state = response_get["Crawler"]["State"]
    print(state)
    if state=='Succeeded':
        delete_crawler(crawler_name)
    
def delete_crawler(crawler_name):
    response = glue_client.delete_crawler(
        Name=crawler_name
    )
    print("Successfully deleted crawler")
    
#extract_zip_file(inbound_path,S3_UNZIPPED_FOLDER)
#move_file_to_landing(S3_UNZIPPED_FOLDER,landing_path)
create_crawler()