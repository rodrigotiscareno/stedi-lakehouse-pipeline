import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing Zone
CustomerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics/raw-data/customers/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLandingZone_node1",
)

# Script generated for node Opt-in Filter
OptinFilter_node2 = Filter.apply(
    frame=CustomerLandingZone_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="OptinFilter_node2",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1673999671272 = glueContext.write_dynamic_frame.from_options(
    frame=OptinFilter_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance-analytics/trusted/customers/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrustedZone_node1673999671272",
)

job.commit()
