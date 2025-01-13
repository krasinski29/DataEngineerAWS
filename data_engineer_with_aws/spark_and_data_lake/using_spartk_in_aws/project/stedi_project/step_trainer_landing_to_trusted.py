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

# Script generated for node Customer Curated
CustomerCurated_node1736729695271 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1736729695271")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1736729714999 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1736729714999")

# Script generated for node Join
Join_node1736729735934 = Join.apply(frame1=StepTrainerLanding_node1736729714999, frame2=CustomerCurated_node1736729695271, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1736729735934")

# Script generated for node SQL Query
SqlQuery0 = '''
select sensorReadingTime, serialNumber, distanceFromObject
from myDataSource

'''
SQLQuery_node1736729847441 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1736729735934}, transformation_ctx = "SQLQuery_node1736729847441")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1736729847441, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1736729681407", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1736729909083 = glueContext.getSink(path="s3://krasinski-udacity-1/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1736729909083")
AmazonS3_node1736729909083.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1736729909083.setFormat("json")
AmazonS3_node1736729909083.writeFrame(SQLQuery_node1736729847441)
job.commit()