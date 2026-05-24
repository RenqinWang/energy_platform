"""
Backend configuration for Energy Platform API
"""
import os

# Spark Configuration
SPARK_HOME = "/home/student/spark-3.5.7-bin-hadoop3"
JAVA_HOME = "/usr/lib/jvm/java-17-openjdk-amd64"

# HDFS Configuration
HDFS_NAMENODE = "hdfs://node1:9000"
HDFS_LAKE_BASE_PATH = f"{HDFS_NAMENODE}/lake"

# Data mode:
# - full: frozen full-batch lake snapshot under /lake/full
# - stream: simulated streaming/micro-batch lake under /lake/stream
DATA_MODE = os.getenv("ENERGY_DATA_MODE", "full").strip().lower()
if DATA_MODE not in {"full", "stream"}:
    DATA_MODE = "full"

HDFS_LAKE_PATH = os.getenv("HDFS_LAKE_PATH", f"{HDFS_LAKE_BASE_PATH}/{DATA_MODE}")

# Delta Lake Paths
SILVER_CHILLER_STATUS_PATH = f"{HDFS_LAKE_PATH}/silver/silver_chiller_status"
GOLD_SUPPLY_CURVE_PATH = f"{HDFS_LAKE_PATH}/gold/gold_supply_curve_hourly"
GOLD_DAILY_REPORT_PATH = f"{HDFS_LAKE_PATH}/gold/gold_report_daily"
GOLD_WEEKLY_REPORT_PATH = f"{HDFS_LAKE_PATH}/gold/gold_report_weekly"
GOLD_MONTHLY_REPORT_PATH = f"{HDFS_LAKE_PATH}/gold/gold_report_monthly"
GOLD_FORECAST_PATH = f"{HDFS_LAKE_PATH}/gold/gold_forecast_supply"
GOLD_ADVICE_PATH = f"{HDFS_LAKE_PATH}/gold/gold_operation_advice"
GOLD_REVENUE_FORECAST_PATH = f"{HDFS_LAKE_PATH}/gold/gold_revenue_forecast"
GOLD_DATA_QUALITY_PATH = f"{HDFS_LAKE_PATH}/gold/gold_data_quality"
GOLD_SYSTEM_SUPPLY_CURVE_PATH = f"{HDFS_LAKE_PATH}/gold/gold_system_supply_hourly"
GOLD_SYSTEM_DAILY_REPORT_PATH = f"{HDFS_LAKE_PATH}/gold/gold_system_report_daily"
GOLD_SYSTEM_WEEKLY_REPORT_PATH = f"{HDFS_LAKE_PATH}/gold/gold_system_report_weekly"
GOLD_SYSTEM_MONTHLY_REPORT_PATH = f"{HDFS_LAKE_PATH}/gold/gold_system_report_monthly"
GOLD_SYSTEM_FORECAST_PATH = f"{HDFS_LAKE_PATH}/gold/gold_system_forecast_supply"
GOLD_SYSTEM_FORECAST_METRICS_PATH = f"{HDFS_LAKE_PATH}/gold/gold_system_forecast_metrics"
GOLD_SYSTEM_REVENUE_FORECAST_PATH = f"{HDFS_LAKE_PATH}/gold/gold_system_revenue_forecast"

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
