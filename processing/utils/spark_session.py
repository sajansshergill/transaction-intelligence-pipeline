"""
spark_session.py — SparkSession factory supporting local dev and AWS EMR.

Set SPARK_ENV=emr to activate EMR configuration.
Defaults to local[*] for development.
"""

import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "ConnectedCommerce") -> SparkSession:
    env = os.getenv("SPARK_ENV", "local")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", os.getenv("S3_CURATED_PREFIX", "s3://connected-commerce-lake/curated"))
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "32")
    )

    if env == "local":
        builder = (
            builder
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.catalog.glue_catalog.warehouse", "/tmp/iceberg_warehouse")
            # Use local filesystem catalog in dev
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.type", "hadoop")
        )
    elif env == "emr":
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark