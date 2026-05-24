"""
Data Access Layer for querying Delta Lake tables using PySpark
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from typing import Optional, List, Dict, Any
import config
import math


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

    @staticmethod
    def _collect_rows(df) -> List[Dict[str, Any]]:
        """Collect a Spark DataFrame and convert date/datetime values to strings."""
        result = []
        for row in df.collect():
            row_dict = row.asDict()
            for key, value in row_dict.items():
                if isinstance(value, float) and not math.isfinite(value):
                    row_dict[key] = None
                    continue
                if hasattr(value, 'isoformat'):
                    row_dict[key] = value.isoformat()
            result.append(row_dict)
        return result

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

    def get_system_equipment_list(
        self,
        system_type: Optional[str] = None,
        station_id: Optional[str] = None
    ) -> List[str]:
        """Get list of equipment IDs from the unified system Gold report."""
        df = self._spark.read.format("delta").load(config.GOLD_SYSTEM_DAILY_REPORT_PATH)
        if station_id:
            df = df.filter(col("station_id") == station_id)
        if system_type:
            df = df.filter(col("system_type") == system_type)
        equipment = df.select("equipment_id").distinct().orderBy("equipment_id").collect()
        return [row.equipment_id for row in equipment]

    def query_system_supply_curve(
        self,
        system_type: Optional[str] = None,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query unified hourly supply curve data for chiller/heating/CCHP."""
        df = self._spark.read.format("delta").load(config.GOLD_SYSTEM_SUPPLY_CURVE_PATH)
        if system_type:
            df = df.filter(col("system_type") == system_type)
        if station_id:
            df = df.filter(col("station_id") == station_id)
        if equipment_id:
            df = df.filter(col("equipment_id") == equipment_id)
        if start_date:
            df = df.filter(to_date(col("stat_hour")) >= start_date)
        if end_date:
            df = df.filter(to_date(col("stat_hour")) <= end_date)
        return self._collect_rows(df.orderBy(col("stat_hour").desc()).limit(limit))

    def query_system_daily_report(
        self,
        system_type: Optional[str] = None,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query unified daily system report data."""
        df = self._spark.read.format("delta").load(config.GOLD_SYSTEM_DAILY_REPORT_PATH)
        if system_type:
            df = df.filter(col("system_type") == system_type)
        if station_id:
            df = df.filter(col("station_id") == station_id)
        if equipment_id:
            df = df.filter(col("equipment_id") == equipment_id)
        if start_date:
            df = df.filter(col("stat_date") >= start_date)
        if end_date:
            df = df.filter(col("stat_date") <= end_date)
        return self._collect_rows(df.orderBy(col("stat_date").desc()).limit(limit))

    def query_system_weekly_report(
        self,
        system_type: Optional[str] = None,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        start_week: Optional[str] = None,
        end_week: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query unified weekly system report data."""
        df = self._spark.read.format("delta").load(config.GOLD_SYSTEM_WEEKLY_REPORT_PATH)
        if system_type:
            df = df.filter(col("system_type") == system_type)
        if station_id:
            df = df.filter(col("station_id") == station_id)
        if equipment_id:
            df = df.filter(col("equipment_id") == equipment_id)
        if start_week:
            df = df.filter(col("stat_week_str") >= start_week)
        if end_week:
            df = df.filter(col("stat_week_str") <= end_week)
        return self._collect_rows(df.orderBy(col("week_start_date").desc()).limit(limit))

    def query_system_monthly_report(
        self,
        system_type: Optional[str] = None,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query unified monthly system report data."""
        df = self._spark.read.format("delta").load(config.GOLD_SYSTEM_MONTHLY_REPORT_PATH)
        if system_type:
            df = df.filter(col("system_type") == system_type)
        if station_id:
            df = df.filter(col("station_id") == station_id)
        if equipment_id:
            df = df.filter(col("equipment_id") == equipment_id)
        if start_month:
            df = df.filter(col("stat_month_str") >= start_month)
        if end_month:
            df = df.filter(col("stat_month_str") <= end_month)
        return self._collect_rows(df.orderBy(col("month_start_date").desc()).limit(limit))

    def query_system_forecast(
        self,
        system_type: Optional[str] = None,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query unified 24-hour supply forecasts for all systems."""
        df = self._spark.read.format("delta").load(config.GOLD_SYSTEM_FORECAST_PATH)
        if system_type:
            df = df.filter(col("system_type") == system_type)
        if station_id:
            df = df.filter(col("station_id") == station_id)
        if equipment_id:
            df = df.filter(col("equipment_id") == equipment_id)
        return self._collect_rows(df.orderBy(col("target_hour").asc()).limit(limit))

    def query_system_forecast_metrics(
        self,
        system_type: Optional[str] = None,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query model explanation and evaluation metrics for system forecasts."""
        df = self._spark.read.format("delta").load(config.GOLD_SYSTEM_FORECAST_METRICS_PATH)
        if system_type:
            df = df.filter(col("system_type") == system_type)
        if station_id:
            df = df.filter(col("station_id") == station_id)
        if equipment_id:
            df = df.filter(col("equipment_id") == equipment_id)
        return self._collect_rows(df.orderBy(col("equipment_id").asc()).limit(limit))

    def query_system_revenue_forecast(
        self,
        system_type: Optional[str] = None,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        forecast_date: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query unified revenue forecasts for all systems."""
        df = self._spark.read.format("delta").load(config.GOLD_SYSTEM_REVENUE_FORECAST_PATH)
        if system_type:
            df = df.filter(col("system_type") == system_type)
        if station_id:
            df = df.filter(col("station_id") == station_id)
        if equipment_id:
            df = df.filter(col("equipment_id") == equipment_id)
        if forecast_date:
            df = df.filter(to_date(col("forecast_date")) == forecast_date)
        return self._collect_rows(df.orderBy(col("target_hour").asc()).limit(limit))

    def query_forecast(
        self,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        hours: int = 24,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Query forecast data

        Args:
            station_id: Filter by station ID
            equipment_id: Filter by equipment ID
            hours: Number of hours to forecast (default 24)
            limit: Maximum number of records to return

        Returns:
            List of dictionaries containing forecast data
        """
        try:
            df = self._spark.read.format("delta").load(config.GOLD_FORECAST_PATH)

            # Apply filters
            if station_id:
                df = df.filter(col("station_id") == station_id)
            if equipment_id:
                df = df.filter(col("equipment_id") == equipment_id)

            # Order by target hour and limit
            df = df.orderBy(col("target_hour").asc()).limit(limit)

            # Convert to list of dictionaries
            result = []
            for row in df.collect():
                row_dict = row.asDict()
                # Convert date/datetime objects to strings
                for key, value in row_dict.items():
                    if hasattr(value, 'isoformat'):
                        row_dict[key] = value.isoformat()
                result.append(row_dict)

            return result

        except Exception as e:
            print(f"Error querying forecast: {e}")
            return []

    def query_advice(
        self,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        risk_level: Optional[str] = None,
        advice_type: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Query operation advice data

        Args:
            station_id: Filter by station ID
            equipment_id: Filter by equipment ID
            risk_level: Filter by risk level (low, medium, high)
            advice_type: Filter by advice type (load_change, anomaly, efficiency, economic)
            limit: Maximum number of records to return

        Returns:
            List of dictionaries containing advice data
        """
        try:
            df = self._spark.read.format("delta").load(config.GOLD_ADVICE_PATH)

            # Apply filters
            if station_id:
                df = df.filter(col("station_id") == station_id)
            if equipment_id:
                df = df.filter(col("equipment_id") == equipment_id)
            if risk_level:
                df = df.filter(col("risk_level") == risk_level)
            if advice_type:
                df = df.filter(col("advice_type") == advice_type)

            # Filter active advice only
            df = df.filter(col("is_active") == True)

            # Order by advice time descending and limit
            df = df.orderBy(col("advice_time").desc()).limit(limit)

            # Convert to list of dictionaries
            result = []
            for row in df.collect():
                row_dict = row.asDict()
                # Convert date/datetime objects to strings
                for key, value in row_dict.items():
                    if hasattr(value, 'isoformat'):
                        row_dict[key] = value.isoformat()
                result.append(row_dict)

            return result

        except Exception as e:
            print(f"Error querying advice: {e}")
            return []

    def query_weekly_report(
        self,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        start_week: Optional[str] = None,
        end_week: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Query weekly report data

        Args:
            station_id: Filter by station ID
            equipment_id: Filter by equipment ID
            start_week: Start week (format: 2018-W30)
            end_week: End week (format: 2018-W35)
            limit: Maximum number of records to return

        Returns:
            List of dictionaries containing weekly report data
        """
        try:
            df = self._spark.read.format("delta").load(config.GOLD_WEEKLY_REPORT_PATH)

            # Apply filters
            if station_id:
                df = df.filter(col("station_id") == station_id)
            if equipment_id:
                df = df.filter(col("equipment_id") == equipment_id)
            if start_week:
                df = df.filter(col("stat_week_str") >= start_week)
            if end_week:
                df = df.filter(col("stat_week_str") <= end_week)

            # Order by week descending and limit
            df = df.orderBy(col("week_start_date").desc()).limit(limit)

            # Convert to list of dictionaries
            result = []
            for row in df.collect():
                row_dict = row.asDict()
                # Convert date/datetime objects to strings
                for key, value in row_dict.items():
                    if hasattr(value, 'isoformat'):
                        row_dict[key] = value.isoformat()
                result.append(row_dict)

            return result

        except Exception as e:
            print(f"Error querying weekly report: {e}")
            return []

    def query_monthly_report(
        self,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Query monthly report data

        Args:
            station_id: Filter by station ID
            equipment_id: Filter by equipment ID
            start_month: Start month (format: 2018-07)
            end_month: End month (format: 2018-09)
            limit: Maximum number of records to return

        Returns:
            List of dictionaries containing monthly report data
        """
        try:
            df = self._spark.read.format("delta").load(config.GOLD_MONTHLY_REPORT_PATH)

            # Apply filters
            if station_id:
                df = df.filter(col("station_id") == station_id)
            if equipment_id:
                df = df.filter(col("equipment_id") == equipment_id)
            if start_month:
                df = df.filter(col("stat_month_str") >= start_month)
            if end_month:
                df = df.filter(col("stat_month_str") <= end_month)

            # Order by month descending and limit
            df = df.orderBy(col("month_start_date").desc()).limit(limit)

            # Convert to list of dictionaries
            result = []
            for row in df.collect():
                row_dict = row.asDict()
                # Convert date/datetime objects to strings
                for key, value in row_dict.items():
                    if hasattr(value, 'isoformat'):
                        row_dict[key] = value.isoformat()
                result.append(row_dict)

            return result

        except Exception as e:
            print(f"Error querying monthly report: {e}")
            return []

    def query_revenue_forecast(
        self,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        forecast_date: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Query revenue forecast data

        Args:
            station_id: Filter by station ID
            equipment_id: Filter by equipment ID
            forecast_date: Filter by forecast date (format: 2018-07-30)
            limit: Maximum number of records to return

        Returns:
            List of dictionaries containing revenue forecast data
        """
        try:
            df = self._spark.read.format("delta").load(config.GOLD_REVENUE_FORECAST_PATH)

            # Apply filters
            if station_id:
                df = df.filter(col("station_id") == station_id)
            if equipment_id:
                df = df.filter(col("equipment_id") == equipment_id)
            if forecast_date:
                df = df.filter(to_date(col("forecast_date")) == forecast_date)

            # Order by target hour and limit
            df = df.orderBy(col("target_hour").asc()).limit(limit)

            # Convert to list of dictionaries
            result = []
            for row in df.collect():
                row_dict = row.asDict()
                # Convert date/datetime objects to strings
                for key, value in row_dict.items():
                    if hasattr(value, 'isoformat'):
                        row_dict[key] = value.isoformat()
                result.append(row_dict)

            return result

        except Exception as e:
            print(f"Error querying revenue forecast: {e}")
            return []

    def query_data_quality(
        self,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        quality_flag: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Query data quality scores

        Args:
            station_id: Filter by station ID
            equipment_id: Filter by equipment ID
            start_date: Start date (format: YYYY-MM-DD)
            end_date: End date (format: YYYY-MM-DD)
            quality_flag: Filter by quality flag (good, warning, poor)
            limit: Maximum number of records to return

        Returns:
            List of dictionaries containing data quality scores
        """
        try:
            df = self._spark.read.format("delta").load(config.GOLD_DATA_QUALITY_PATH)

            # Apply filters
            if station_id:
                df = df.filter(col("station_id") == station_id)
            if equipment_id:
                df = df.filter(col("equipment_id") == equipment_id)
            if start_date:
                df = df.filter(to_date(col("stat_date")) >= start_date)
            if end_date:
                df = df.filter(to_date(col("stat_date")) <= end_date)
            if quality_flag:
                df = df.filter(col("quality_flag") == quality_flag)

            # Order by date descending and limit
            df = df.orderBy(col("stat_date").desc()).limit(limit)

            # Convert to list of dictionaries
            result = []
            for row in df.collect():
                row_dict = row.asDict()
                # Convert date/datetime objects to strings
                for key, value in row_dict.items():
                    if hasattr(value, 'isoformat'):
                        row_dict[key] = value.isoformat()
                result.append(row_dict)

            return result

        except Exception as e:
            print(f"Error querying data quality: {e}")
            return []

    def close(self):
        """Close Spark session"""
        if self._spark:
            self._spark.stop()
            self._spark = None


# Global instance
dal = DataAccessLayer()
