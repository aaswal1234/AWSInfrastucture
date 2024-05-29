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
AmazonS3_node1713251468625 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://integrationframework/intermediate/databrewoutput/file_comparison/"], "recurse": True}, transformation_ctx="AmazonS3_node1713251468625")

# Script generated for node SQL Query
SqlQuery108 = '''
SELECT * FROM myDataSource
'''
SQLQuery_node1713251629556 = sparkSqlQuery(glueContext, query = SqlQuery108, mapping = {"myDataSource":AmazonS3_node1713251468625}, transformation_ctx = "SQLQuery_node1713251629556")

# Script generated for node Change Schema
ChangeSchema_node1713252290905 = ApplyMapping.apply(frame=SQLQuery_node1713251629556, mappings=[("monthstart", "string", "monthstart", "date"), ("fefd_mcrid", "string", "fefd_mcrid", "string"), ("mmr_mcrid", "string", "mmr_mcrid", "string"), ("sf_mcrid", "string", "sf_mcrid", "string"), ("id834_medicaidid", "string", "id834_medicaidid", "string"), ("sf_medicaidid", "string", "sf_medicaidid", "string"), ("welbe_patientid", "string", "welbe_patientid", "long"), ("sf_eligtype", "string", "sf_eligtype", "string"), ("sf_enrollmentdate", "string", "sf_enrollmentdate", "date"), ("sf_termdate", "string", "sf_termdate", "string"), ("dob", "string", "dob", "date"), ("age", "string", "age", "long"), ("sf_namedob", "string", "sf_namedob", "string"), ("fefd_namedob", "string", "fefd_namedob", "string"), ("mmr_namedob", "string", "mmr_namedob", "string"), ("dob834_namedob", "string", "dob834_namedob", "string"), ("HCPSTATUS", "string", "HCPSTATUS", "long"), ("Status_Problem", "string", "Status_Problem", "string"), ("HCP_Status_Descr", "string", "HCP_Status_Descr", "string"), ("fefd_enrollment_start_dt", "string", "fefd_enrollment_start_dt", "string"), ("fefd_contract", "string", "fefd_contract", "string"), ("sf_contract", "string", "sf_contract", "string"), ("Review_Mcr_contract", "string", "Review_Mcr_contract", "string"), ("reviewlist", "string", "reviewlist", "string")], transformation_ctx="ChangeSchema_node1713252290905")

# Script generated for node PostgreSQL
PostgreSQL_node1713251484566 = glueContext.write_dynamic_frame.from_catalog(frame=ChangeSchema_node1713252290905, database="postgrescatalogdb", table_name="postgres_analytics_filecomparison", transformation_ctx="PostgreSQL_node1713251484566")

# Script generated for node Amazon Redshift
AmazonRedshift_node1714050238547 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1713252290905, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-119391704005-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.filecomparison", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS public.filecomparison (monthstart DATE, fefd_mcrid VARCHAR, mmr_mcrid VARCHAR, sf_mcrid VARCHAR, id834_medicaidid VARCHAR, sf_medicaidid VARCHAR, welbe_patientid BIGINT, sf_eligtype VARCHAR, sf_enrollmentdate DATE, sf_termdate VARCHAR, dob DATE, age BIGINT, sf_namedob VARCHAR, fefd_namedob VARCHAR, mmr_namedob VARCHAR, dob834_namedob VARCHAR, HCPSTATUS BIGINT, Status_Problem VARCHAR, HCP_Status_Descr VARCHAR, fefd_enrollment_start_dt VARCHAR, fefd_contract VARCHAR, sf_contract VARCHAR, Review_Mcr_contract VARCHAR, reviewlist VARCHAR);"}, transformation_ctx="AmazonRedshift_node1714050238547")

job.commit()