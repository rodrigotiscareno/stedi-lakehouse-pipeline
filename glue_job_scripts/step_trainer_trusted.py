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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics/raw-data/step_trainer/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1674006066244 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-human-balance",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1674006066244",
)

# Script generated for node Join on Serial Number
JoinonSerialNumber_node2 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerCurated_node1674006066244,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="JoinonSerialNumber_node2",
)

# Script generated for node Drop Fields
DropFields_node1674006712462 = DropFields.apply(
    frame=JoinonSerialNumber_node2,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1674006712462",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1674006712462,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance-analytics/trusted/step_trainer/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
