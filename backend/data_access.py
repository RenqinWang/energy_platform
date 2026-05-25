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
        try:
            df = self._spark.read.format("delta").load(config.GOLD_DAILY_REPORT_PATH)
        except Exception:
            df = self._spark.read.format("delta").load(config.GOLD_SYSTEM_DAILY_REPORT_PATH)
        stations = df.select("station_id").distinct().collect()
        return [row.station_id for row in stations]

    def get_equipment_list(self, station_id: Optional[str] = None) -> List[str]:
        """Get list of equipment IDs, optionally filtered by station"""
        try:
            df = self._spark.read.format("delta").load(config.GOLD_DAILY_REPORT_PATH)
        except Exception:
            df = self._spark.read.format("delta").load(config.GOLD_SYSTEM_DAILY_REPORT_PATH)
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

    def query_dashboard_dates(self, station_id: Optional[str] = None) -> List[str]:
        """Return dates that have dashboard-ready Gold daily report data."""
        df = self._spark.read.format("delta").load(config.GOLD_SYSTEM_DAILY_REPORT_PATH)
        if station_id:
            df = df.filter(col("station_id") == station_id)
        rows = df.select("stat_date").distinct().orderBy(col("stat_date").asc()).collect()
        return [str(row["stat_date"]) for row in rows if row["stat_date"] is not None]

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
        try:
            df = self._spark.read.format("delta").load(config.GOLD_SYSTEM_FORECAST_METRICS_PATH)
            if system_type:
                df = df.filter(col("system_type") == system_type)
            if station_id:
                df = df.filter(col("station_id") == station_id)
            if equipment_id:
                df = df.filter(col("equipment_id") == equipment_id)
            return self._collect_rows(df.orderBy(col("equipment_id").asc()).limit(limit))
        except Exception:
            forecast_rows = self.query_system_forecast(
                system_type=system_type,
                station_id=station_id,
                equipment_id=equipment_id,
                limit=10000,
            )
            metrics = []
            seen = set()
            for row in forecast_rows:
                key = (row.get("station_id"), row.get("system_type"), row.get("equipment_id"))
                if key in seen:
                    continue
                seen.add(key)
                model_version = row.get("model_version") or "seasonal_hour_profile_v1"
                metrics.append({
                    "station_id": row.get("station_id"),
                    "system_type": row.get("system_type"),
                    "equipment_id": row.get("equipment_id"),
                    "model_version": model_version,
                    "algorithm": row.get("algorithm") or "seasonal_hour_profile",
                    "target_col": "supply_kwh",
                    "feature_list": row.get("feature_set") or "system_type,equipment_id,month,date_type,hour_of_day,historical_hour_profile,recent_24h_supply,recent_24h_operation_rate",
                    "feature_design": "流式模式使用季节、日期类型、小时段、系统类型、设备ID、历史同小时供能画像，以及最近24小时供能量和运行率作为解释特征。",
                    "window_design": "Kafka 数据先落 Bronze，再由微批增量刷新 Gold 小时表；预测取当前已到达的小时历史构造画像，并滚动生成未来24小时目标点。",
                    "model_reason": "流式样本初期较少，优先使用可解释的季节-日期-时段画像基线保证连续展示；全量模式下可运行随机森林训练脚本产出正式训练评估指标。",
                    "training_method": "每次微批后基于当前 Gold 小时表重建画像，按系统和设备分组统计历史小时模式。",
                    "evaluation_method": "画像基线不强行给出离线测试分数；页面保留 MAE/RMSE/R2/MAPE 为空，避免把非训练结果伪装为模型评估。",
                    "train_start": None,
                    "train_end": None,
                    "test_start": None,
                    "test_end": None,
                    "train_row_count": row.get("profile_sample_count"),
                    "test_row_count": None,
                    "mae": None,
                    "rmse": None,
                    "r2": None,
                    "mape": None,
                    "residual_std": None,
                    "top_features": "[{\"feature\":\"hour_of_day\",\"importance\":0.35},{\"feature\":\"historical_hour_profile\",\"importance\":0.30},{\"feature\":\"recent_24h_operation_rate\",\"importance\":0.20}]",
                    "result_summary": "当前为流式微批画像基线结果，用于验证实时链路和连续展示；若需要正式训练评估，请在流式样本积累后运行统一趋势预测训练任务。",
                    "dt": row.get("dt"),
                })
                if len(metrics) >= limit:
                    break
            return metrics

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

    def query_price_history(
        self,
        station_id: Optional[str] = None,
        price_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query Silver price dimension history."""
        df = self._spark.read.format("delta").load(config.SILVER_PRICE_DIM_PATH)
        if station_id:
            df = df.filter(col("station_code") == station_id)
        if price_type:
            df = df.filter(col("price_type") == price_type)
        if start_date:
            df = df.filter(col("effective_date") >= start_date)
        if end_date:
            df = df.filter(col("effective_date") <= end_date)
        return self._collect_rows(df.orderBy(col("effective_date").desc(), col("price_type").asc()).limit(limit))

    def query_operation_advice(
        self,
        system_type: Optional[str] = None,
        station_id: Optional[str] = None,
        equipment_id: Optional[str] = None,
        risk_level: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query unified operation advice with graceful fallback to legacy advice table."""
        rows = self.query_advice(
            station_id=station_id,
            equipment_id=equipment_id,
            risk_level=risk_level,
            limit=limit
        )
        if system_type:
            rows = [
                row for row in rows
                if row.get("system_type") == system_type
                or (system_type == "chiller" and str(row.get("equipment_id", "")).startswith("chiller_"))
                or (system_type == "heating" and str(row.get("equipment_id", "")).startswith("heating_"))
                or (system_type == "cchp" and str(row.get("equipment_id", "")) == "cchp_system")
            ]

        for row in rows:
            equipment = str(row.get("equipment_id", ""))
            if not row.get("system_type"):
                if equipment.startswith("chiller_"):
                    row["system_type"] = "chiller"
                elif equipment.startswith("heating_"):
                    row["system_type"] = "heating"
                elif equipment == "cchp_system":
                    row["system_type"] = "cchp"
        return rows[:limit]

    @staticmethod
    def _number(value: Any) -> float:
        return float(value) if isinstance(value, (int, float)) and math.isfinite(float(value)) else 0.0

    @staticmethod
    def _system_status(record: Dict[str, Any]) -> str:
        supply = DataAccessLayer._number(record.get("supply_kwh")) \
            or DataAccessLayer._number(record.get("cooling_supply_kwh")) \
            + DataAccessLayer._number(record.get("heating_supply_kwh")) \
            + DataAccessLayer._number(record.get("electric_supply_kwh"))
        energy = DataAccessLayer._number(record.get("energy_consumption_kwh"))
        operation = DataAccessLayer._number(record.get("operation_rate"))
        records = DataAccessLayer._number(record.get("record_count"))
        if records <= 0:
            return "nodata"
        if supply <= 0 and energy <= 0 and operation <= 0:
            return "stopped"
        if records < 3:
            return "warning"
        return "running"

    def get_dashboard_summary(
        self,
        station_id: Optional[str] = None,
        stat_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build a compact dashboard payload from Gold/Silver query results."""
        effective_start = start_date or stat_date
        effective_end = end_date or stat_date or start_date
        hourly = self.query_system_supply_curve(
            station_id=station_id,
            start_date=effective_start,
            end_date=effective_end,
            limit=5000,
        )
        daily = self.query_system_daily_report(
            station_id=station_id,
            start_date=effective_start,
            end_date=effective_end,
            limit=500,
        )
        forecast = self.query_system_forecast(station_id=station_id, limit=10000)
        revenue = self.query_system_revenue_forecast(station_id=station_id, limit=10000)

        latest_by_equipment: Dict[str, Dict[str, Any]] = {}
        for row in hourly:
            key = f"{row.get('system_type')}:{row.get('equipment_id')}"
            if key not in latest_by_equipment:
                latest_by_equipment[key] = row

        equipment = []
        for row in latest_by_equipment.values():
            supply = self._number(row.get("supply_kwh")) \
                or self._number(row.get("cooling_supply_kwh")) \
                + self._number(row.get("heating_supply_kwh")) \
                + self._number(row.get("electric_supply_kwh"))
            energy = self._number(row.get("energy_consumption_kwh"))
            equipment.append({
                "station_id": row.get("station_id"),
                "system_type": row.get("system_type"),
                "equipment_id": row.get("equipment_id"),
                "status": self._system_status(row),
                "supply": supply,
                "cooling": self._number(row.get("cooling_supply_kwh")),
                "heating": self._number(row.get("heating_supply_kwh")),
                "electric": self._number(row.get("electric_supply_kwh")),
                "energy": energy,
                "efficiency": supply / energy if energy > 0 else None,
                "stat_hour": row.get("stat_hour"),
            })

        systems: Dict[str, Dict[str, Any]] = {}
        for item in equipment:
            system_type = item.get("system_type") or "unknown"
            current = systems.setdefault(system_type, {
                "system_type": system_type,
                "equipment_count": 0,
                "running_count": 0,
                "warning_count": 0,
                "total_supply": 0.0,
                "total_cooling": 0.0,
                "total_heating": 0.0,
                "total_electric": 0.0,
                "total_energy": 0.0,
            })
            current["equipment_count"] += 1
            if item["status"] == "running":
                current["running_count"] += 1
            if item["status"] == "warning":
                current["warning_count"] += 1
            current["total_supply"] += item["supply"]
            current["total_cooling"] += item["cooling"]
            current["total_heating"] += item["heating"]
            current["total_electric"] += item["electric"]
            current["total_energy"] += item["energy"]

        is_range_view = bool(effective_start and effective_end and effective_start != effective_end)
        trend_points: Dict[str, Dict[str, Any]] = {}
        if is_range_view:
            for row in daily:
                day = str(row.get("stat_date") or "")
                if not day:
                    continue
                point = trend_points.setdefault(day, {"time": day, "history": 0.0})
                point["history"] += self._number(row.get("total_supply_kwh")) \
                    or self._number(row.get("total_cooling_supply_kwh")) \
                    + self._number(row.get("total_heating_supply_kwh")) \
                    + self._number(row.get("total_electric_supply_kwh"))
        else:
            for row in hourly:
                hour = row.get("stat_hour")
                if not hour:
                    continue
                point = trend_points.setdefault(hour, {"time": hour, "history": 0.0})
                point["history"] += self._number(row.get("supply_kwh")) \
                    or self._number(row.get("cooling_supply_kwh")) \
                    + self._number(row.get("heating_supply_kwh")) \
                    + self._number(row.get("electric_supply_kwh"))

        forecast_by_offset: Dict[Any, Dict[str, Any]] = {}
        for row in forecast:
            offset = row.get("forecast_hour_offset")
            key = int(offset) if isinstance(offset, (int, float)) and offset else row.get("target_hour")
            point = forecast_by_offset.setdefault(key, {
                "offset": key,
                "time": row.get("target_hour"),
                "forecast": 0.0,
                "lower": 0.0,
                "upper": 0.0,
            })
            point["forecast"] += self._number(row.get("predicted_supply_kwh"))
            point["lower"] += self._number(row.get("confidence_lower"))
            point["upper"] += self._number(row.get("confidence_upper"))
            if row.get("target_hour") and (not point.get("time") or str(row.get("target_hour")) < str(point.get("time"))):
                point["time"] = row.get("target_hour")

        forecast_points = [
            {k: v for k, v in item.items() if k != "offset"}
            for item in sorted(forecast_by_offset.values(), key=lambda value: value["offset"])[:24]
        ]

        # `gold_system_revenue_forecast` contains one 24-hour horizon per equipment.
        # The revenue page defaults to chiller_10, so the dashboard KPI uses the
        # same scope and keeps the all-system total as secondary context.
        all_revenue_total = sum(self._number(row.get("predicted_profit")) for row in revenue)
        revenue_scope = "chiller_10"
        scoped_revenue = [
            row for row in revenue
            if row.get("system_type") == "chiller" and row.get("equipment_id") == "chiller_10"
        ]
        if not scoped_revenue:
            revenue_scope = "chiller"
            scoped_revenue = [row for row in revenue if row.get("system_type") == "chiller"]
        if not scoped_revenue:
            revenue_scope = "all_systems"
            scoped_revenue = revenue
        revenue_total = sum(self._number(row.get("predicted_profit")) for row in scoped_revenue)
        today_supply = sum(
            self._number(row.get("total_supply_kwh")) or self._number(row.get("total_cooling_supply_kwh"))
            for row in daily[:80]
        )
        total_energy = sum(item["energy"] for item in equipment)
        total_supply = sum(item["supply"] for item in equipment)
        latest_data_time = max([str(item.get("stat_hour")) for item in equipment if item.get("stat_hour")] or [""])
        advice = self._build_dashboard_advice(
            equipment=equipment,
            systems=list(systems.values()),
            start_date=effective_start,
            end_date=effective_end,
            latest_data_time=latest_data_time,
            revenue_total=revenue_total,
        )

        return {
            "station_id": station_id or "ALL",
            "stat_date": stat_date,
            "start_date": effective_start,
            "end_date": effective_end,
            "generated_at": self._collect_rows(self._spark.sql("select current_timestamp() as ts"))[0]["ts"],
            "latest_data_time": latest_data_time,
            "kpis": {
                "today_total_supply": today_supply,
                "current_total_supply": total_supply,
                "current_total_energy": total_energy,
                "forecast_profit_24h": revenue_total,
                "forecast_profit_scope": revenue_scope,
                "forecast_profit_all_systems": all_revenue_total,
                "running_equipment": sum(1 for item in equipment if item["status"] == "running"),
                "warning_equipment": sum(1 for item in equipment if item["status"] == "warning"),
                "avg_efficiency": total_supply / total_energy if total_energy > 0 else None,
            },
            "systems": list(systems.values()),
            "equipment": equipment,
            "trend": sorted(trend_points.values(), key=lambda item: item["time"])[-30 if is_range_view else -24:],
            "forecast": forecast_points,
            "advice": advice,
        }

    def _build_dashboard_advice(
        self,
        equipment: List[Dict[str, Any]],
        systems: List[Dict[str, Any]],
        start_date: Optional[str],
        end_date: Optional[str],
        latest_data_time: str,
        revenue_total: float,
    ) -> List[Dict[str, Any]]:
        """Build date-window-specific dashboard advice from the same rows shown on screen."""
        window_text = start_date if start_date == end_date else f"{start_date or '-'} 至 {end_date or '-'}"
        advice_time = latest_data_time or end_date or start_date or ""
        advices: List[Dict[str, Any]] = []

        if not equipment:
            return [{
                "system_type": None,
                "equipment_id": "系统",
                "advice_time": advice_time,
                "advice_type": "data_window",
                "risk_level": "medium",
                "advice_text": f"{window_text} 没有可用于大屏展示的 Gold 小时数据，请切换到有数据的日期或使用最近7天/最近30天窗口。",
            }]

        label_map = {"chiller": "冷机", "heating": "热机", "cchp": "冷热电三联供"}

        for system in systems:
            system_type = system.get("system_type")
            label = label_map.get(system_type, system_type or "系统")
            running = int(system.get("running_count") or 0)
            supply = self._number(system.get("total_supply"))
            energy = self._number(system.get("total_energy"))
            if running == 0 and supply <= 0:
                advices.append({
                    "system_type": system_type,
                    "equipment_id": f"{label}系统",
                    "advice_time": advice_time,
                    "advice_type": "operation_status",
                    "risk_level": "low",
                    "advice_text": f"{window_text} 最新小时 {label}系统未运行或无供能输出，若业务侧存在用能需求，应检查调度计划和上游数据是否同步。",
                })
            elif energy > 0 and supply / energy < 1.0:
                advices.append({
                    "system_type": system_type,
                    "equipment_id": f"{label}系统",
                    "advice_time": advice_time,
                    "advice_type": "efficiency",
                    "risk_level": "medium",
                    "advice_text": f"{window_text} 最新小时 {label}系统供能/能耗比低于 1.0，建议核查负荷率、燃气/电耗计量和设备运行效率。",
                })

        running_equipment = [item for item in equipment if item.get("status") == "running"]
        for item in sorted(running_equipment, key=lambda row: self._number(row.get("supply")), reverse=True)[:3]:
            efficiency = item.get("efficiency")
            advice_text = (
                f"{window_text} 最新小时 {item.get('equipment_id')} 正在承担主要供能，"
                f"供能 {self._number(item.get('supply')):.1f} kWh。"
            )
            if isinstance(efficiency, (int, float)) and math.isfinite(float(efficiency)):
                advice_text += f" 当前效率约 {float(efficiency):.2f}，建议结合负荷预测维持在高效区间。"
            advices.append({
                "system_type": item.get("system_type"),
                "equipment_id": item.get("equipment_id"),
                "advice_time": advice_time,
                "advice_type": "dispatch",
                "risk_level": "low",
                "advice_text": advice_text,
            })

        if revenue_total < 0:
            advices.append({
                "system_type": None,
                "equipment_id": "收益预测",
                "advice_time": advice_time,
                "advice_type": "economic",
                "risk_level": "medium",
                "advice_text": f"未来24小时收益预测为 {revenue_total:.1f} 元，当前成本口径下为负，建议优先复核热机/三联供燃气成本和供能价格匹配关系。",
            })

        if not advices:
            advices.append({
                "system_type": None,
                "equipment_id": "系统",
                "advice_time": advice_time,
                "advice_type": "normal",
                "risk_level": "low",
                "advice_text": f"{window_text} 最新小时系统运行状态平稳，未发现明显供能中断或效率异常。",
            })

        return advices[:8]

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
