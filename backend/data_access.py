"""
Data Access Layer for querying Delta Lake tables using PySpark
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from typing import Optional, List, Dict, Any
import config


class DataAccessLayer:
    """Singleton class for managing Spark session and data queries"""

    _instance = None
    _spark = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataAccessLayer, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._spark is None:
            self._initialize_spark()

    def _initialize_spark(self):
        """Initialize Spark session with Delta Lake support"""
        self._spark = SparkSession.builder \
            .appName("EnergyPlatformAPI") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.defaultFS", config.HDFS_NAMENODE) \
            .getOrCreate()

        self._spark.sparkContext.setLogLevel("WARN")

    def get_spark(self) -> SparkSession:
        """Get Spark session"""
        return self._spark

    def query_supply_curve(
        self,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Query hourly supply curve data

        Args:
            station_id: Filter by station ID
            equipment_id: Filter by equipment ID
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            limit: Maximum number of records to return

        Returns:
            List of dictionaries containing supply curve data
        """
        df = self._spark.read.format("delta").load(config.GOLD_SUPPLY_CURVE_PATH)

        # Apply filters
        if station_id:
            df = df.filter(col("station_id") == station_id)
        if equipment_id:
            df = df.filter(col("equipment_id") == equipment_id)
        if start_date:
            df = df.filter(to_date(col("stat_hour")) >= start_date)
        if end_date:
            df = df.filter(to_date(col("stat_hour")) <= end_date)

        # Order by time descending and limit
        df = df.orderBy(col("stat_hour").desc()).limit(limit)

        # Convert to list of dictionaries and convert date types to strings
        result = []
        for row in df.collect():
            row_dict = row.asDict()
            # Convert date/datetime objects to strings
            for key, value in row_dict.items():
                if hasattr(value, 'isoformat'):
                    row_dict[key] = value.isoformat()
            result.append(row_dict)

        return result

    def query_daily_report(
        self,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Query daily report data

        Args:
            station_id: Filter by station ID
            equipment_id: Filter by equipment ID
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            limit: Maximum number of records to return

        Returns:
            List of dictionaries containing daily report data
        """
        df = self._spark.read.format("delta").load(config.GOLD_DAILY_REPORT_PATH)

        # Apply filters
        if station_id:
            df = df.filter(col("station_id") == station_id)
        if equipment_id:
            df = df.filter(col("equipment_id") == equipment_id)
        if start_date:
            df = df.filter(col("stat_date") >= start_date)
        if end_date:
            df = df.filter(col("stat_date") <= end_date)

        # Order by date descending and limit
        df = df.orderBy(col("stat_date").desc()).limit(limit)

        # Convert to list of dictionaries and convert date types to strings
        result = []
        for row in df.collect():
            row_dict = row.asDict()
            # Convert date/datetime objects to strings
            for key, value in row_dict.items():
                if hasattr(value, 'isoformat'):
                    row_dict[key] = value.isoformat()
            result.append(row_dict)

        return result

    def query_equipment_status(
        self,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Query equipment status data (minute-level)

        Args:
            station_id: Filter by station ID
            equipment_id: Filter by equipment ID
            start_time: Start time (YYYY-MM-DD HH:mm:ss format)
            end_time: End time (YYYY-MM-DD HH:mm:ss format)
            limit: Maximum number of records to return

        Returns:
            List of dictionaries containing equipment status data
        """
        df = self._spark.read.format("delta").load(config.SILVER_CHILLER_STATUS_PATH)

        # Apply filters
        if station_id:
            df = df.filter(col("station_id") == station_id)
        if equipment_id:
            df = df.filter(col("equipment_id") == equipment_id)
        if start_time:
            df = df.filter(col("stat_time") >= start_time)
        if end_time:
            df = df.filter(col("stat_time") <= end_time)

        # Order by time descending and limit
        df = df.orderBy(col("stat_time").desc()).limit(limit)

        # Convert to list of dictionaries and convert date types to strings
        result = []
        for row in df.collect():
            row_dict = row.asDict()
            # Convert date/datetime objects to strings
            for key, value in row_dict.items():
                if hasattr(value, 'isoformat'):
                    row_dict[key] = value.isoformat()
            result.append(row_dict)

        return result

    def get_station_list(self) -> List[str]:
        """Get list of all station IDs"""
        df = self._spark.read.format("delta").load(config.GOLD_DAILY_REPORT_PATH)
        stations = df.select("station_id").distinct().collect()
        return [row.station_id for row in stations]

    def get_equipment_list(self, station_id: Optional[str] = None) -> List[str]:
        """Get list of equipment IDs, optionally filtered by station"""
        df = self._spark.read.format("delta").load(config.GOLD_DAILY_REPORT_PATH)
        if station_id:
            df = df.filter(col("station_id") == station_id)
        equipment = df.select("equipment_id").distinct().collect()
        return [row.equipment_id for row in equipment]

    def close(self):
        """Close Spark session"""
        if self._spark:
            self._spark.stop()
            self._spark = None


# Global instance
dal = DataAccessLayer()
