#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lower, regexp_replace, col

# Set AWS credentials as environment variables - DO NOT hardcode in production!
os.environ["AWS_ACCESS_KEY_ID"] =   # SECURITY RISK! Use environment variables or credential providers
os.environ["AWS_SECRET_ACCESS_KEY"] = '  # SECURITY RISK!
os.environ["AWS_DEFAULT_REGION"] = 'eu-north-1'
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Full path to JAR files - replace with your actual path
jar_dir = os.path.abspath("C:/Users/82107/jars")  # Change this to your JARs directory
jar_files = [
    os.path.join(jar_dir, "hadoop-aws-3.3.1.jar"),
    os.path.join(jar_dir, "hadoop-common-3.3.1.jar"),
    os.path.join(jar_dir, "aws-java-sdk-bundle-1.11.901.jar"),
    os.path.join(jar_dir, "postgresql-42.2.5.jar")  
]
jars_path = ",".join(jar_files)

spark = SparkSession.builder \
    .appName("YouTube Trend Pipeline") \
    .config("spark.master", "local[*]") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.local.dir", "C:/Users/82107/tmp_spark") \
    .config("spark.jars", jars_path) \
    .config("spark.driver.extraClassPath", jars_path) \
    .config("spark.executor.extraClassPath", jars_path) \
    .config("spark.files.fetchTimeout", "600") \
    .config("spark.rpc.askTimeout", "600") \
    .config("spark.driver.allowMultipleContexts", "true") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()




spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Test S3 connection before reading large file
bucket_name = 'youtubebucket145'
try:
    # List files in the bucket as a test
    test_list = spark._jsc.hadoopConfiguration().get("fs.s3a.impl")
    print(f"S3A implementation: {test_list}")
    
    # Try to read a small test file or list bucket content
    test_path = f"s3a://{bucket_name}/"
    print(f"Testing access to: {test_path}")
    test_df = spark.read.text(test_path)
    count = test_df.count()
    print(f"S3 connection successful, found {count} entries")
except Exception as e:
    print(f"S3 connection test failed: {e}")
    import traceback
    traceback.print_exc()

# Try different S3 path formats if needed
file_path_options = [
    f"s3a://{bucket_name}/uploads/youtube_trend.csv",  # Original
    f"s3://{bucket_name}/uploads/youtube_trend.csv",   # Without 'a'
    f"s3a://{bucket_name}.s3.{os.environ['AWS_DEFAULT_REGION']}.amazonaws.com/uploads/youtube_trend.csv"  # Full endpoint
]

# Try reading with each path format
df = None
for path in file_path_options:
    try:
        print(f"Attempting to read from: {path}")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        print(f"Successfully read data from: {path}")
        break  # Exit loop if successful
    except Exception as e:
        print(f"Failed to read from {path}: {e}")

if df is None:
    raise Exception("Could not read data from any of the S3 paths")

# Rest of your code remains the same
# Clean and preprocess text columns
text_columns = [field.name for field in df.schema.fields if str(field.dataType) == 'StringType']
for column in text_columns:
    df = df.withColumn(column, lower(regexp_replace(col(column), r'[^ -\x7F]+', '')))
df = df.drop("channel_localized_title", "channel_localized_description").na.drop("any")

# Basic video info
df = df.withColumn("duration_seconds",
        (F.coalesce(F.regexp_extract(F.col("video_duration"), r'(\d+)H', 1).cast("int"), F.lit(0)) * 3600) +
        (F.coalesce(F.regexp_extract(F.col("video_duration"), r'(\d+)M', 1).cast("int"), F.lit(0)) * 60) +
        (F.coalesce(F.regexp_extract(F.col("video_duration"), r'(\d+)S', 1).cast("int"), F.lit(0)))
    ) \
    .withColumn("video_type", F.when(F.col("duration_seconds") <= 60, "Shorts").otherwise("Long-Form")) \
    .withColumn("video_published_at_ts", F.to_timestamp(F.col("video_published_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("video_published_date", F.to_date(F.col("video_published_at_ts"))) \
    .withColumn("video_published_time", F.date_format(F.col("video_published_at_ts"), "HH:mm:ss"))

# Text feature extraction
df = df.withColumn("tags_word_count",
            F.when(F.col("video_tags").isNull() | (F.trim(F.col("video_tags")) == ""), 0)
            .otherwise(F.size(F.split(F.regexp_replace(F.col("video_tags"), r"\s*,\s*", ","), ",")))
        )

df = df.withColumn("words_array",
        F.when(F.col("video_description").isNull() | (F.trim(F.col("video_description")) == ""), F.array())
        .otherwise(F.split(F.regexp_replace(F.lower(F.trim(F.col("video_description"))), r"\s+", " "), " "))
    ) \
    .withColumn("description_word_count", F.size(F.col("words_array"))) \
    .drop("words_array")

# Trending and geographic features
trending_dates_df = df.groupBy("video_id") \
    .agg(F.collect_set("video_trending__date").alias("trending_dates_array"),
         F.countDistinct("video_trending__date").alias("trending_days_count"))

trending_countries_df = df.groupBy("video_id") \
    .agg(F.collect_set("video_trending_country").alias("countries_trended_in"),
         F.countDistinct("video_trending_country").alias("num_countries_trended_in"))

countries_per_date_df = df.groupBy("video_trending__date") \
    .agg(F.countDistinct("video_trending_country").alias("num_countries_trending_per_date"))

country_trending_df = df.groupBy("video_id", "video_trending_country") \
    .agg(F.countDistinct("video_trending__date").alias("country_trending_days"))

# Final join
df_final = df \
    .join(trending_dates_df, ["video_id"], "left") \
    .join(trending_countries_df, ["video_id"], "left") \
    .join(country_trending_df, ["video_id", "video_trending_country"], "left") \
    .join(countries_per_date_df, ["video_trending__date"], "left") \
    .na.drop("any")

# Flag Shorts videos
df_final = df_final.withColumn("is_shorts",
        F.when((F.col("duration_seconds") <= 60) | (F.lower(F.col("video_title")).contains("#shorts")), 1).otherwise(0)
    )

# Show final output
df_final.show(20)

# Save to PostgreSQL
try:
    df_final.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/youtube") \
        .option("dbtable", "youtube_fin2") \
        .option("user", "postgres") \
        .option("password", "5526") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    print("Successfully saved data to PostgreSQL")
except Exception as e:
    print(f"Error saving to PostgreSQL: {e}")
    import traceback
    traceback.print_exc()

