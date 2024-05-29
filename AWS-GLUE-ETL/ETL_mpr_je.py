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
AmazonS3_node1713251468625 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://integrationframework/intermediate/databrewoutput/mpr_je/"], "recurse": True}, transformation_ctx="AmazonS3_node1713251468625")

# Script generated for node SQL Query
SqlQuery128 = '''
select * from myDataSource
'''
SQLQuery_node1713251629556 = sparkSqlQuery(glueContext, query = SqlQuery128, mapping = {"myDataSource":AmazonS3_node1713251468625}, transformation_ctx = "SQLQuery_node1713251629556")

# Script generated for node Change Schema
ChangeSchema_node1713252290905 = ApplyMapping.apply(frame=SQLQuery_node1713251629556, mappings=[("MEMBERID", "string", "MEMBERID", "bigint"), ("MEDICAIDID", "string", "MEDICAIDID", "string"), ("MEDICAREMBI", "string", "MEDICAREMBI", "string"), ("ParticipantName", "string", "ParticipantName", "string"), ("MPR_MCD_A_EffDate", "string", "MPR_MCD_A_EffDate", "string"), ("MPR_MCD_B_EffDate", "string", "MPR_MCD_B_EffDate", "string"), ("MPR_MCD_D_EffDate", "string", "MPR_MCD_D_EffDate", "string"), ("MMR_A_PayStartDate", "string", "MMR_A_PayStartDate", "timestamp"), ("MMR_A_PayEndDate", "string", "MMR_A_PayEndDate", "timestamp"), ("MMR_B_PayStartDate", "string", "MMR_B_PayStartDate", "timestamp"), ("MMR_B_PayEndDate", "string", "MMR_B_PayEndDate", "timestamp"), ("MMR_D_PayStartDate", "string", "MMR_D_PayStartDate", "timestamp"), ("MMR_D_PayEndDate", "string", "MMR_D_PayEndDate", "timestamp"), ("EligibilityType", "string", "EligibilityType", "string"), ("EligibilityTypeOvrd", "string", "EligibilityTypeOvrd", "string"), ("AR_ELIGIBILITY_TYPE", "string", "AR_ELIGIBILITY_TYPE", "string"), ("EligibilityTypeMMR", "string", "EligibilityTypeMMR", "string"), ("EligibilityType834", "string", "EligibilityType834", "string"), ("EligibilityTypeClaims", "string", "EligibilityTypeClaims", "string"), ("Prim_Cap_Medi_Cal_Only", "string", "Prim_Cap_Medi_Cal_Only", "string"), ("Prim_Cap_Dual", "string", "Prim_Cap_Dual", "string"), ("Dual_State_Only", "string", "Dual_State_Only", "string"), ("Medi_Cal_Only_State_Only", "string", "Medi_Cal_Only_State_Only", "string"), ("DHCS_IMMIGRATION_STATUS", "string", "DHCS_IMMIGRATION_STATUS", "date"), ("PaceCenter", "string", "PaceCenter", "string"), ("CMSCONTRACTNUMBER", "string", "CMSCONTRACTNUMBER", "string"), ("PaceLocation", "string", "PaceLocation", "string"), ("PAC_ABBR", "string", "PAC_ABBR", "string"), ("PACEENROLLMENTDATE", "string", "PACEENROLLMENTDATE", "date"), ("PACETERMINATIONNOTIFICATIONDATE", "string", "PACETERMINATIONNOTIFICATIONDATE", "string"), ("PACEENROLLMENTMONTHS", "string", "PACEENROLLMENTMONTHS", "string"), ("DOB", "string", "DOB", "date"), ("MEDICAREESRD", "string", "MEDICAREESRD", "string"), ("YearMonth", "string", "YearMonth", "bigint"), ("MonthStart", "string", "MonthStart", "date"), ("MonthEnd", "string", "MonthEnd", "date"), ("ActiveFlag", "string", "ActiveFlag", "bigint"), ("HC1_PAYMENTMEDICAREPRTA", "string", "HC1_PAYMENTMEDICAREPRTA", "bigint"), ("HC1_PAYMENTMEDICAREPRTB", "string", "HC1_PAYMENTMEDICAREPRTB", "bigint"), ("HC1_PAYMENTMEDICAREPRTD", "string", "HC1_PAYMENTMEDICAREPRTD", "bigint"), ("AR_ELIGIBILITY_LAST_UPDATED", "string", "AR_ELIGIBILITY_LAST_UPDATED", "string"), ("FEFDContractNumber", "string", "FEFDContractNumber", "string"), ("FEFDStatus", "string", "FEFDStatus", "string"), ("EligibilityTypeMPR", "string", "EligibilityTypeMPR", "string")], transformation_ctx="ChangeSchema_node1713252290905")

# Script generated for node Amazon Redshift
AmazonRedshift_node1714050136337 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1713252290905, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-119391704005-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.mpr_je", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS public.mpr_je (MEMBERID VARCHAR, MEDICAIDID VARCHAR, MEDICAREMBI VARCHAR, ParticipantName VARCHAR, MPR_MCD_A_EffDate VARCHAR, MPR_MCD_B_EffDate VARCHAR, MPR_MCD_D_EffDate VARCHAR, MMR_A_PayStartDate TIMESTAMP, MMR_A_PayEndDate TIMESTAMP, MMR_B_PayStartDate TIMESTAMP, MMR_B_PayEndDate TIMESTAMP, MMR_D_PayStartDate TIMESTAMP, MMR_D_PayEndDate TIMESTAMP, EligibilityType VARCHAR, EligibilityTypeOvrd VARCHAR, AR_ELIGIBILITY_TYPE VARCHAR, EligibilityTypeMMR VARCHAR, EligibilityType834 VARCHAR, EligibilityTypeClaims VARCHAR, Prim_Cap_Medi_Cal_Only VARCHAR, Prim_Cap_Dual VARCHAR, Dual_State_Only VARCHAR, Medi_Cal_Only_State_Only VARCHAR, DHCS_IMMIGRATION_STATUS DATE, PaceCenter VARCHAR, CMSCONTRACTNUMBER VARCHAR, PaceLocation VARCHAR, PAC_ABBR VARCHAR, PACEENROLLMENTDATE DATE, PACETERMINATIONNOTIFICATIONDATE VARCHAR, PACEENROLLMENTMONTHS VARCHAR, DOB DATE, MEDICAREESRD VARCHAR, YearMonth VARCHAR, MonthStart DATE, MonthEnd DATE, ActiveFlag VARCHAR, HC1_PAYMENTMEDICAREPRTA VARCHAR, HC1_PAYMENTMEDICAREPRTB VARCHAR, HC1_PAYMENTMEDICAREPRTD VARCHAR, AR_ELIGIBILITY_LAST_UPDATED VARCHAR, FEFDContractNumber VARCHAR, FEFDStatus VARCHAR, EligibilityTypeMPR VARCHAR);"}, transformation_ctx="AmazonRedshift_node1714050136337")

# Script generated for node PostgreSQL
PostgreSQL_node1713251484566 = glueContext.write_dynamic_frame.from_catalog(frame=ChangeSchema_node1713252290905, database="postgrescatalogdb", table_name="postgres_analytics_mpr_je", transformation_ctx="PostgreSQL_node1713251484566")

job.commit()