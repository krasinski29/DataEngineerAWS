import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1736716843913 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1736716843913")

# Script generated for node Customer Trusted
CustomerTrusted_node1736716870404 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1736716870404")

# Script generated for node Join
Join_node1736716889947 = Join.apply(frame1=CustomerTrusted_node1736716870404, frame2=AccelerometerLanding_node1736716843913, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1736716889947")

# Script generated for node Drop Fields
SqlQuery0 = '''
select user, timestamp, x, y, z from myDataSource

'''
DropFields_node1736717886739 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1736716889947}, transformation_ctx = "DropFields_node1736717886739")

# Script generated for node Acceleromete Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1736717886739, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1736714919462", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometeTrusted_node1736716933335 = glueContext.getSink(path="s3://krasinski-udacity-1/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometeTrusted_node1736716933335")
AccelerometeTrusted_node1736716933335.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometeTrusted_node1736716933335.setFormat("json")
AccelerometeTrusted_node1736716933335.writeFrame(DropFields_node1736717886739)
job.commit()