#Python script with the purpose of loading the content of gold-bucket-dani" 
#on Amazon Redshift


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

AmazonS3_node1730642778825 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://gold-bucket-dani"]}, transformation_ctx="AmazonS3_node1730642778825")

AmazonRedshift_node1730642786564 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1730642778825, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-677276085698-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.btc_merged", "connectionName": "Redshift connection 3", "preactions": "CREATE TABLE IF NOT EXISTS public.btc_merged (date VARCHAR, price DOUBLE PRECISION, indice_google_trend VARCHAR); TRUNCATE TABLE public.btc_merged;"}, transformation_ctx="AmazonRedshift_node1730642786564")

job.commit()