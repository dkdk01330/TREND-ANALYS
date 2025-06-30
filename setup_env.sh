#!/bin/bash

#AWS Credentials and Region
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_DEFAULT_REGION="eu-north-1"

#JARs (for Hadoop AWS, PostgreSQL)
export JARS="/c/Users/82107/jars/hadoop-aws-3.3.1.jar,/c/Users/82107/jars/hadoop-common-3.3.1.jar,/c/Users/82107/jars/aws-java-sdk-bundle-1.11.901.jar,/c/Users/82107/jars/postgresql-42.2.5.jar"

# Java & Spark Paths
export JAVA_HOME="/c/Program Files/Java/jdk1.8.0_202"
export SPARK_HOME="/c/spark-3.5.5-bin-hadoop3"
export PATH="$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH"
