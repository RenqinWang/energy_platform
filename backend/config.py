"""
Backend configuration for Energy Platform API
"""
import os

# Spark Configuration
SPARK_HOME = "/home/student/spark-3.5.7-bin-hadoop3"
JAVA_HOME = "/usr/lib/jvm/java-17-openjdk-amd64"

# HDFS Configuration
HDFS_NAMENODE = "hdfs://node1:9000"
HDFS_LAKE_PATH = f"{HDFS_NAMENODE}/lake"

# Delta Lake Paths
SILVER_CHILLER_STATUS_PATH = f"{HDFS_LAKE_PATH}/silver/silver_chiller_status"
GOLD_SUPPLY_CURVE_PATH = f"{HDFS_LAKE_PATH}/gold/gold_supply_curve_hourly"
GOLD_DAILY_REPORT_PATH = f"{HDFS_LAKE_PATH}/gold/gold_report_daily"

# API Configuration
API_TITLE = "Energy Platform API"
API_VERSION = "1.0.0"
API_DESCRIPTION = "REST API for querying energy platform data from Delta Lake"

# CORS Configuration
CORS_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:8080",
    "http://115.120.208.241:8080",
    "http://115.120.208.241:8001",
    "*",  # Allow all origins for development
]

# Set environment variables
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["SPARK_HOME"] = SPARK_HOME
