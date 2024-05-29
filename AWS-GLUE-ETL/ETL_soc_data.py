import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1713251468625 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://integrationframework/intermediate/databrewoutput/soc_data/"], "recurse": True}, transformation_ctx="AmazonS3_node1713251468625")

# Script generated for node SQL Query
SqlQuery72 = '''
SELECT 
edw_source_file_date, 
hc1_hcpstatus, 
participantnumber, 
participantfirstname, 
participantlastname, 
participantmiddle, 
dob, 
hc1_startdate, 
hc1_enddate, 
hc1_hcpcode, 
cast(soc_amount as string) as soc_amount, 
immigration_status, 
eligibilitytype, 
center, 
cast(cap_amount as string) as cap_amount, 
dhcs_dual_status, 
source
FROM myDataSource;
'''
SQLQuery_node1713251629556 = sparkSqlQuery(glueContext, query = SqlQuery72, mapping = {"myDataSource":AmazonS3_node1713251468625}, transformation_ctx = "SQLQuery_node1713251629556")

# Script generated for node Change Schema
ChangeSchema_node1713252290905 = ApplyMapping.apply(frame=SQLQuery_node1713251629556, mappings=[("edw_source_file_date", "string", "edw_source_file_date", "date"), ("hc1_hcpstatus", "string", "hc1_hcpstatus", "long"), ("participantnumber", "string", "participantnumber", "string"), ("participantfirstname", "string", "participantfirstname", "string"), ("participantlastname", "string", "participantlastname", "string"), ("participantmiddle", "string", "participantmiddle", "string"), ("dob", "string", "dob", "date"), ("hc1_startdate", "string", "hc1_startdate", "date"), ("hc1_enddate", "string", "hc1_enddate", "date"), ("hc1_hcpcode", "string", "hc1_hcpcode", "long"), ("soc_amount", "string", "soc_amount", "string"), ("immigration_status", "string", "immigration_status", "string"), ("eligibilitytype", "string", "eligibilitytype", "string"), ("center", "string", "center", "string"), ("cap_amount", "string", "cap_amount", "string"), ("dhcs_dual_status", "string", "dhcs_dual_status", "string"), ("source", "string", "source", "string")], transformation_ctx="ChangeSchema_node1713252290905")

# Script generated for node Amazon Redshift
AmazonRedshift_node1714050171056 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1713252290905, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-119391704005-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.soc_data", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS public.soc_data (edw_source_file_date DATE, hc1_hcpstatus BIGINT, participantnumber VARCHAR, participantfirstname VARCHAR, participantlastname VARCHAR, participantmiddle VARCHAR, dob DATE, hc1_startdate DATE, hc1_enddate DATE, hc1_hcpcode BIGINT, soc_amount VARCHAR, immigration_status VARCHAR, eligibilitytype VARCHAR, center VARCHAR, cap_amount VARCHAR, dhcs_dual_status VARCHAR, source VARCHAR);"}, transformation_ctx="AmazonRedshift_node1714050171056")

# Script generated for node PostgreSQL
PostgreSQL_node1713251484566 = glueContext.write_dynamic_frame.from_catalog(frame=ChangeSchema_node1713252290905, database="postgrescatalogdb", table_name="postgres_analytics_soc_data", transformation_ctx="PostgreSQL_node1713251484566")

job.commit()