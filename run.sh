#!/bin/bash

# Set environment variables
source ./setup_env.sh

# Run Spark Job
"$SPARK_HOME/bin/spark-submit" \
  --master local[*] \
  --name "YouTube Trend Pipeline" \
  --jars "/c/Users/82107/jars/hadoop-aws-3.3.1.jar,/c/Users/82107/jars/hadoop-common-3.3.1.jar,/c/Users/82107/jars/aws-java-sdk-bundle-1.11.901.jar,/c/Users/82107/jars/postgresql-42.2.5.jar" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID" \
  --conf "spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY" \
  --conf "spark.hadoop.fs.s3a.endpoint=s3.eu-north-1.amazonaws.com" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  ./run_pipeline.py
