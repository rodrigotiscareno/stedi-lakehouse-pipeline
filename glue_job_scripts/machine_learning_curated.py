import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-human-balance",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1674006066244 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-human-balance",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1674006066244",
)

# Script generated for node Join on Timestamp
JoinonTimestamp_node2 = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=AccelerometerTrusted_node1674006066244,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="JoinonTimestamp_node2",
)

# Script generated for node Drop Fields
DropFields_node1674006712462 = DropFields.apply(
    frame=JoinonTimestamp_node2,
    paths=["timestamp"],
    transformation_ctx="DropFields_node1674006712462",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1674006712462,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance-analytics/curated/machine_learning/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()
