import os,io
import sys
import pyspark
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
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
Preprocess_path='BhagyaM/Preprocess/'
Inbound_path='BhagyaM/Inbound/'
Landing_path='BhagyaM/Landing/'
Output_path='/BhagyaM/Outbound/'
DatabaseName='saama-gene-training-data'
xls_extn='.xls'
csv_extn='.csv'
print("Buckect_name : "+s3_bucket_name)
#Check if indicator file exist or not
def check_ind_file():
    for file in my_bucket.objects.filter(Prefix=Inbound_path):
        key=file.key
    if key.endswith(('.ind')) is True:
        print("Indicator file is exist to start process")
        return 1
    else:
        print("file does not exist")
        return 0
#Copy files from Inbound location to preprocessor location
def copy_file(from_dir,to_dir):
    for file in my_bucket.objects.filter(Prefix=from_dir):
        file1=file.key
        if file1.endswith((xls_extn,csv_extn)) is True:
            print("Start coping the file.")
            destfile = file1.replace(from_dir,to_dir,1)
            copy_source = {'Bucket': s3_bucket_name, 'Key': file1}
            s3_client.copy_object(Bucket=s3_bucket_name, CopySource=copy_source, Key=destfile)
            print(f'File copied to {to_dir} location')
#Convert xls file to csv 
def cnvrt_xls_csv():
    for file in my_bucket.objects.filter(Prefix=Preprocess_path):
        file1=file.key
        if file1.endswith(xls_extn) is True:
            extn = file1.split('.')[-2]
            print(extn)
            print("xls file exist...")
            read_file = pd.read_excel(f's3://{s3_bucket_name}/{file1}',header=0)
            read_file.to_csv(f's3://{s3_bucket_name}/{extn}.csv',index = None,header=True,sep='|')
            print("File converted to csv ....")
#Move files from preprocessor to landing location
def move_to_landing(from_dir,to_dir):
    for file in my_bucket.objects.filter(Prefix=from_dir):
        file1=file.key
        if(file1.endswith(csv_extn)) is True:
            destfile = file1.replace(from_dir,to_dir,1)
            copy_source = {'Bucket': s3_bucket_name, 'Key': file1}
            s3_client.copy_object(Bucket=s3_bucket_name, CopySource=copy_source, Key=destfile)
#create crawler for each file into the landing folder
def create_crawler():
    print('Start to Create crawler!...')
    for file in my_bucket.objects.filter(Prefix=Landing_path):
        file1=file.key
        if(file1.endswith(csv_extn)) is True:
            file_path = file1.split('.')[-2][0:-4]
            file_name = file1.split('/')[3]
            file = file_name.split('.')[-2]
            crawler_name=f'saama-gene-training-bhagya-assignment1-{file_name}'
            crawler_details = glue_client.list_crawlers()
            print(crawler_details)
            if crawler_name not in crawler_details['CrawlerNames']:
                response = glue_client.create_crawler(
                    Name=crawler_name,
                    Role='saama-gene-training-glue-service-role',
                    DatabaseName='saama-gene-training-data',
                    Targets={
                        'S3Targets': [
                            {
                                'Path': f's3://{s3_bucket_name}/{file_path}',
                            }
                        ]
                    },
                    TablePrefix=f'saama-gene-training-bhagyam_'
                )
                print(f"{crawler_name}  - Crawler is created successfully....")
            run_crawler(crawler_name)
#Run the crawlers
def run_crawler(crawler_name):
    response = glue_client.start_crawler(
        Name=crawler_name
        )
    print("Successfully started crawler")

def access_athena():
    for file in my_bucket.objects.filter(Prefix=Landing_path):
        file1=file.key
        if(file1.endswith(csv_extn)) is True:
            file_name = file1.split('/')[3]
            file = file_name.split('.')[-2]
            table_name=f'saama-gene-training-bhagyam_{file}'
            query = f"SELECT COUNT(*) from {DatabaseName}.{table_name}"
            queryStart = athena_client.start_query_execution(
                QueryString = query,
                QueryExecutionContext = {
                    'Database': DatabaseName
                    }, 
                    ResultConfiguration = { 'OutputLocation': 's3://saama-gene-training-data-bucket/BhagyaM/Landing/INFY/'}
                    )
                    
                    
def create_df(frm_dir,to_dir):
    for file in my_bucket.objects.filter(Prefix=frm_dir):
        file1=file.key
        if(file1.endswith(csv_extn)) is True:
            print(file1)
            fileFolder = file1.split('/')[2]
            data=f's3://{s3_bucket_name}/{file1}'
            print(data)
            print('---------------------------------')
            df=spark.read.csv(data,header='True',inferSchema='True')
            df = df.select([f.col(col).alias(col.replace(' ', '_')) for col in df.columns])
            df = df.select(col(df.columns[0]).cast(StringType()),to_date(col(df.columns[1]),'m/d/yyyy').alias('Date'),\
            col(df.columns[2]).cast(DoubleType()),col(df.columns[3]).cast(DoubleType()),col(df.columns[4]).cast(DoubleType()),\
            col(df.columns[5]).cast(DoubleType()),col(df.columns[6]).cast(DoubleType()),col(df.columns[7]).cast(IntegerType()))
            df.show(2)
            df.printSchema()
            full_window = Window.partitionBy([weekofyear(df.Date),year(df.Date)])
            print('New Dataframe: as below : ')
            New_df=df.select(col(df.columns[0]).alias('script_name'),weekofyear(col(df.columns[1])).alias('week_no'),\
            f.first(col(df.columns[1])).over(full_window).alias('st_date'),\
            f.last(col(df.columns[1])).over(full_window).alias('end_date'),\
            f.first(col(df.columns[2])).over(full_window).alias('open'),\
            f.max(col(df.columns[3])).over(full_window).alias('high'),\
            f.min(col(df.columns[4])).over(full_window).alias('low'),\
            f.last(col(df.columns[5])).over(full_window).alias('close'),\
            f.avg(col(df.columns[6])).over(full_window).alias("adj_close"),\
            f.sum(col(df.columns[7])).over(full_window).alias("volume")).distinct()
            New_df.show(5)
            #Write CSV file with column header (column names)
            Output_path=f's3://{s3_bucket_name}{to_dir}{fileFolder}'
            New_df.write.option("header",True).csv(Output_path)

            
if(check_ind_file()==1):
    #copy_file(Inbound_path,Preprocess_path)
    #cnvrt_xls_csv()
    #move_to_landing(Preprocess_path,Landing_path) 
    #create_crawler()
    #access_athena()
    create_df(Landing_path,Output_path)
else:
    print("Exit the program...")