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
AmazonS3_node1713251468625 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://integrationframework/intermediate/databrewoutput/prt_mpr_addr_change_dhcs_834/"], "recurse": True}, transformation_ctx="AmazonS3_node1713251468625")

# Script generated for node SQL Query
SqlQuery49 = '''
SELECT * FROM myDataSource
'''
SQLQuery_node1713251629556 = sparkSqlQuery(glueContext, query = SqlQuery49, mapping = {"myDataSource":AmazonS3_node1713251468625}, transformation_ctx = "SQLQuery_node1713251629556")

# Script generated for node Change Schema
ChangeSchema_node1713252290905 = ApplyMapping.apply(frame=SQLQuery_node1713251629556, mappings=[("a_zip", "string", "a_zip", "long"), ("b_zip", "string", "b_zip", "long"), ("concat_a_addr", "string", "concat_a_addr", "string"), ("concat_b_addr", "string", "concat_b_addr", "string"), ("PaceCenter", "string", "PaceCenter", "string"), ("FileType", "string", "FileType", "string"), ("PARTICIPANTNUMBER", "string", "PARTICIPANTNUMBER", "string"), ("PARTICIPANTFIRSTNAME", "string", "PARTICIPANTFIRSTNAME", "string"), ("PARTICIPANTLASTNAME", "string", "PARTICIPANTLASTNAME", "string"), ("PARTICIPANTMIDDLE", "string", "PARTICIPANTMIDDLE", "string"), ("DOB", "string", "DOB", "date"), ("EDW_SOURCE_FILE_DATE", "string", "EDW_SOURCE_FILE_DATE", "date"), ("RES_ADDRESS1", "string", "RES_ADDRESS1", "string"), ("RES_ADDRESS2", "string", "RES_ADDRESS2", "string"), ("RES_CITY", "string", "RES_CITY", "string"), ("RES_STATE", "string", "RES_STATE", "string"), ("RES_ZIP", "string", "RES_ZIP", "long"), ("PHONE", "string", "PHONE", "string"), ("MPR_RES_ADDRESS1", "string", "MPR_RES_ADDRESS1", "string"), ("MPR_RES_ADDRESS2", "string", "MPR_RES_ADDRESS2", "string"), ("MPR_RES_CITY", "string", "MPR_RES_CITY", "string"), ("MPR_RES_STATE", "string", "MPR_RES_STATE", "string"), ("MPR_RES_ZIP", "string", "MPR_RES_ZIP", "long"), ("MPR_PHONE", "string", "MPR_PHONE", "string"), ("INSURANCE_MEDICALELIGIBILITYSTATUS", "string", "INSURANCE_MEDICALELIGIBILITYSTATUS", "string"), ("INSURANCE_FAMECOUNTYID", "string", "INSURANCE_FAMECOUNTYID", "long"), ("HC1_HCPCODE", "string", "HC1_HCPCODE", "long"), ("INSURANCE_AIDCODE", "string", "INSURANCE_AIDCODE", "string"), ("HC1_STARTDATE", "string", "HC1_STARTDATE", "date"), ("HC1_ENDDATE", "string", "HC1_ENDDATE", "date"), ("INSURANCE_CASENUMBER", "string", "INSURANCE_CASENUMBER", "string"), ("INSURANCE_FAMEREDETERMINATIONDATE", "string", "INSURANCE_FAMEREDETERMINATIONDATE", "string"), ("MPR_DOB", "string", "MPR_DOB", "date"), ("HC1_HCPSTATUS", "string", "HC1_HCPSTATUS", "long"), ("HCP_Status_Descr", "string", "HCP_Status_Descr", "string"), ("is_Address_change", "string", "is_Address_change", "string"), ("is_onhold_disenrolled", "string", "is_onhold_disenrolled", "string"), ("Department", "string", "Department", "string"), ("Entity", "string", "Entity", "string"), ("Contract", "string", "Contract", "string"), ("County_Code", "string", "County_Code", "long"), ("HCP_Code", "string", "HCP_Code", "long"), ("Zipcode", "string", "Zipcode", "long"), ("Pace_Covered_Zip", "string", "Pace_Covered_Zip", "string"), ("MPR_Pace_Covered_Zip", "string", "MPR_Pace_Covered_Zip", "string"), ("HCPStatusCategory", "string", "HCPStatusCategory", "string"), ("New_Entity", "string", "New_Entity", "string"), ("MPR_Entity", "string", "MPR_Entity", "string"), ("Entity_Change_Zipcode", "string", "Entity_Change_Zipcode", "string")], transformation_ctx="ChangeSchema_node1713252290905")

# Script generated for node Amazon Redshift
AmazonRedshift_node1714045082094 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1713252290905, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-119391704005-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.prt_mpr_addr_change_dhcs_834", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS public.prt_mpr_addr_change_dhcs_834 (a_zip BIGINT, b_zip BIGINT, concat_a_addr VARCHAR, concat_b_addr VARCHAR, PaceCenter VARCHAR, FileType VARCHAR, PARTICIPANTNUMBER VARCHAR, PARTICIPANTFIRSTNAME VARCHAR, PARTICIPANTLASTNAME VARCHAR, PARTICIPANTMIDDLE VARCHAR, DOB DATE, EDW_SOURCE_FILE_DATE DATE, RES_ADDRESS1 VARCHAR, RES_ADDRESS2 VARCHAR, RES_CITY VARCHAR, RES_STATE VARCHAR, RES_ZIP BIGINT, PHONE VARCHAR, MPR_RES_ADDRESS1 VARCHAR, MPR_RES_ADDRESS2 VARCHAR, MPR_RES_CITY VARCHAR, MPR_RES_STATE VARCHAR, MPR_RES_ZIP BIGINT, MPR_PHONE VARCHAR, INSURANCE_MEDICALELIGIBILITYSTATUS VARCHAR, INSURANCE_FAMECOUNTYID BIGINT, HC1_HCPCODE BIGINT, INSURANCE_AIDCODE VARCHAR, HC1_STARTDATE DATE, HC1_ENDDATE DATE, INSURANCE_CASENUMBER VARCHAR, INSURANCE_FAMEREDETERMINATIONDATE VARCHAR, MPR_DOB DATE, HC1_HCPSTATUS BIGINT, HCP_Status_Descr VARCHAR, is_Address_change VARCHAR, is_onhold_disenrolled VARCHAR, Department VARCHAR, Entity VARCHAR, Contract VARCHAR, County_Code BIGINT, HCP_Code BIGINT, Zipcode BIGINT, Pace_Covered_Zip VARCHAR, MPR_Pace_Covered_Zip VARCHAR, HCPStatusCategory VARCHAR, New_Entity VARCHAR, MPR_Entity VARCHAR, Entity_Change_Zipcode VARCHAR);"}, transformation_ctx="AmazonRedshift_node1714045082094")

# Script generated for node PostgreSQL
PostgreSQL_node1713251484566 = glueContext.write_dynamic_frame.from_catalog(frame=ChangeSchema_node1713252290905, database="postgrescatalogdb", table_name="postgres_analytics_prt_mpr_addr_change_dhcs_834", transformation_ctx="PostgreSQL_node1713251484566")

job.commit()