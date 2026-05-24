#!/usr/bin/env python3
"""Generate unified Gold tables for chiller, heating, and CCHP systems.

The existing chiller Gold tables are high quality and are kept as-is. This
script builds system-wide Gold tables for UI/reporting requirements:

- gold_system_supply_hourly
- gold_system_report_daily
- gold_system_report_weekly
- gold_system_report_monthly
- gold_system_forecast_supply
- gold_system_revenue_forecast
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    current_timestamp,
    date_format,
    date_sub,
    dayofweek,
    explode,
    expr,
    from_unixtime,
    hour,
    last_day,
    least,
    lit,
    max as spark_max,
    min as spark_min,
    month,
    regexp_extract,
    sequence,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trunc,
    unix_timestamp,
    weekofyear,
    when,
    year,
)
from pyspark.sql.window import Window


HDFS_ROOT = "hdfs://node1:9000/lake"
SILVER_POINT_FACT = f"{HDFS_ROOT}/silver/silver_point_fact"
SILVER_PRICE_DIM = f"{HDFS_ROOT}/silver/silver_price_dim"
GOLD_CHILLER_HOURLY = f"{HDFS_ROOT}/gold/gold_supply_curve_hourly"

GOLD_SYSTEM_HOURLY = f"{HDFS_ROOT}/gold/gold_system_supply_hourly"
GOLD_SYSTEM_DAILY = f"{HDFS_ROOT}/gold/gold_system_report_daily"
GOLD_SYSTEM_WEEKLY = f"{HDFS_ROOT}/gold/gold_system_report_weekly"
GOLD_SYSTEM_MONTHLY = f"{HDFS_ROOT}/gold/gold_system_report_monthly"
GOLD_SYSTEM_FORECAST = f"{HDFS_ROOT}/gold/gold_system_forecast_supply"
GOLD_SYSTEM_REVENUE_FORECAST = f"{HDFS_ROOT}/gold/gold_system_revenue_forecast"

GAS_KWH_PER_M3 = 9.7
BOILER_EFFICIENCY = 0.9
HEATING_NOMINAL_KW = 1000.0

HOURLY_COLUMNS = [
    "station_id",
    "system_type",
    "equipment_id",
    "stat_hour",
    "avg_supply_temp",
    "max_supply_temp",
    "min_supply_temp",
    "avg_return_temp",
    "avg_pressure",
    "max_pressure",
    "min_pressure",
    "avg_flow",
    "max_flow",
    "min_flow",
    "avg_power",
    "max_power",
    "min_power",
    "energy_consumption_kwh",
    "energy_input_kwh",
    "cooling_capacity_kw",
    "supply_kwh",
    "cooling_supply_kwh",
    "heating_supply_kwh",
    "electric_supply_kwh",
    "runtime_hours",
    "cumulative_runtime_hours",
    "start_count",
    "running_sample_count",
    "run_minutes",
    "operation_rate",
    "record_count",
    "supply_price_type",
    "metric_quality",
    "dt",
    "created_at",
    "updated_at",
]


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("GenerateSystemGold")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.hadoop.fs.defaultFS", "hdfs://node1:9000")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_price_map(spark):
    price_df = spark.read.format("delta").load(SILVER_PRICE_DIM)
    rows = (
        price_df.groupBy("price_type")
        .agg(avg("price").alias("avg_price"))
        .collect()
    )
    prices = {row["price_type"]: float(row["avg_price"]) for row in rows if row["avg_price"] is not None}
    return {
        "electricity": prices.get("electricity", 0.8),
        "cooling": prices.get("cooling", 0.3),
        "heating": prices.get("heating", 0.35),
    }


def select_hourly(df):
    return df.select(*HOURLY_COLUMNS)


def load_chiller_hourly(spark):
    print("Loading chiller hourly Gold table...")
    df = spark.read.format("delta").load(GOLD_CHILLER_HOURLY)

    return select_hourly(
        df.withColumn("system_type", lit("chiller"))
        .withColumn("supply_kwh", col("cooling_supply_kwh"))
        .withColumn("heating_supply_kwh", lit(0.0))
        .withColumn("electric_supply_kwh", lit(0.0))
        .withColumn("energy_input_kwh", col("energy_consumption_kwh"))
        .withColumn("supply_price_type", lit("cooling"))
        .withColumn("metric_quality", lit("chiller_status_derived"))
    )


def load_fact(spark):
    print("Loading Silver point fact...")
    return (
        spark.read.format("delta")
        .load(SILVER_POINT_FACT)
        .filter(col("system_type").isin("boiler", "burner", "cchp", "generator"))
        .withColumn("event_ts", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX"))
        .filter(col("event_ts").isNotNull())
        .withColumn("stat_hour", date_format(col("event_ts"), "yyyy-MM-dd HH:00:00"))
        .withColumn("dt", to_date(col("event_ts")))
        .withColumn("value_clean", when(col("value") >= 0, col("value")))
    )


def hourly_delta(df, output_col, max_delta):
    hourly_end = (
        df.groupBy("station_id", "point_code", "stat_hour", "dt")
        .agg(spark_max("value_clean").alias("hour_end_value"))
    )
    window_spec = Window.partitionBy("station_id", "point_code").orderBy("stat_hour")
    return (
        hourly_end.withColumn("prev_hour_end_value", expr("lag(hour_end_value) over (partition by station_id, point_code order by stat_hour)"))
        .withColumn("raw_delta", col("hour_end_value") - col("prev_hour_end_value"))
        .withColumn(
            output_col,
            when((col("raw_delta") >= 0) & (col("raw_delta") <= lit(max_delta)), col("raw_delta")),
        )
    )


def build_heating_hourly(fact):
    print("Building heating hourly table from boiler/burner details...")

    boiler = (
        fact.filter((col("system_type") == "boiler") & col("equipment_id").rlike("^boiler_[0-9]+$"))
        .withColumn("equipment_num", regexp_extract(col("equipment_id"), r"([0-9]+)$", 1))
    )
    boiler_status = (
        boiler.filter(~col("point_code").contains("GAS_TOTAL"))
        .groupBy("station_id", "equipment_num", "stat_hour", "dt")
        .agg(
            avg(when((col("measure_role") == "supply_temp") & col("value_clean").between(0, 120), col("value_clean"))).alias("avg_supply_temp"),
            spark_max(when((col("measure_role") == "supply_temp") & col("value_clean").between(0, 120), col("value_clean"))).alias("max_supply_temp"),
            spark_min(when((col("measure_role") == "supply_temp") & col("value_clean").between(0, 120), col("value_clean"))).alias("min_supply_temp"),
            avg(when((col("measure_role") == "return_temp") & col("value_clean").between(0, 120), col("value_clean"))).alias("avg_return_temp"),
            avg(when((col("measure_role") == "pressure") & col("value_clean").between(0, 10), col("value_clean"))).alias("avg_pressure"),
            spark_max(when((col("measure_role") == "pressure") & col("value_clean").between(0, 10), col("value_clean"))).alias("max_pressure"),
            spark_min(when((col("measure_role") == "pressure") & col("value_clean").between(0, 10), col("value_clean"))).alias("min_pressure"),
            avg(when((col("measure_role") == "flow") & col("value_clean").between(0, 100000), col("value_clean"))).alias("avg_flow"),
            spark_max(when((col("measure_role") == "flow") & col("value_clean").between(0, 100000), col("value_clean"))).alias("max_flow"),
            spark_min(when((col("measure_role") == "flow") & col("value_clean").between(0, 100000), col("value_clean"))).alias("min_flow"),
            count("*").alias("boiler_record_count"),
        )
    )

    burner = (
        fact.filter((col("system_type") == "burner") & col("equipment_id").rlike("^burner_[0-9]+$"))
        .withColumn("equipment_num", regexp_extract(col("equipment_id"), r"([0-9]+)$", 1))
    )
    burner_status = (
        burner.groupBy("station_id", "equipment_num", "stat_hour", "dt")
        .agg(
            avg(when((col("point_code").startswith("Burner_Load_")) & col("value_clean").between(0, 120), col("value_clean"))).alias("avg_load_pct"),
            avg(when((col("point_code").startswith("Op_Burner_")) & col("value_clean").between(0, 1), col("value_clean"))).alias("runtime_hours"),
            spark_sum(when((col("point_code").startswith("Op_Burner_")) & (col("value_clean") > 0), 1).otherwise(0)).alias("running_sample_count"),
            spark_max(when((col("point_code").startswith("BetrStdB_")) & (col("value_clean") >= 0), col("value_clean"))).alias("cumulative_runtime_hours"),
            count("*").alias("burner_record_count"),
        )
        .withColumn("runtime_hours", coalesce(col("runtime_hours"), when(col("avg_load_pct") > 0, least(col("avg_load_pct") / 100.0, lit(1.0))).otherwise(lit(0.0))))
        .withColumn("run_minutes", col("runtime_hours") * 60.0)
        .withColumn("operation_rate", col("runtime_hours") * 100.0)
        .withColumn(
            "allocation_weight",
            when((col("runtime_hours") > 0) & (col("avg_load_pct") > 0), col("runtime_hours") * col("avg_load_pct"))
            .when(col("runtime_hours") > 0, col("runtime_hours"))
            .otherwise(lit(0.0)),
        )
        .withColumn("gas_group", when(col("equipment_num").isin("1", "4"), lit("14")).otherwise(lit("23")))
    )

    gas_delta = (
        hourly_delta(
            boiler.filter(col("point_code").isin("14BOILER_GAS_TOTAL", "23BOILER_GAS_TOTAL")),
            "gas_delta_m3",
            max_delta=200000,
        )
        .withColumn("gas_group", when(col("point_code") == "14BOILER_GAS_TOTAL", lit("14")).otherwise(lit("23")))
        .select("station_id", "gas_group", "stat_hour", "dt", "gas_delta_m3")
        .withColumn("gas_input_kwh", col("gas_delta_m3") * lit(GAS_KWH_PER_M3))
        .withColumn("measured_heating_supply_kwh", col("gas_input_kwh") * lit(BOILER_EFFICIENCY))
    )

    alloc_window = Window.partitionBy("station_id", "gas_group", "stat_hour", "dt")
    heating = (
        burner_status.withColumn("group_weight", spark_sum("allocation_weight").over(alloc_window))
        .withColumn("group_member_count", count("*").over(alloc_window))
        .withColumn(
            "allocation_share",
            when(col("group_weight") > 0, col("allocation_weight") / col("group_weight"))
            .otherwise(lit(1.0) / col("group_member_count")),
        )
        .join(gas_delta, ["station_id", "gas_group", "stat_hour", "dt"], "left")
        .join(boiler_status, ["station_id", "equipment_num", "stat_hour", "dt"], "left")
        .withColumn("measured_supply_alloc", col("measured_heating_supply_kwh") * col("allocation_share"))
        .withColumn("measured_input_alloc", col("gas_input_kwh") * col("allocation_share"))
        .withColumn("estimated_supply_from_load", (coalesce(col("avg_load_pct"), lit(0.0)) / 100.0) * lit(HEATING_NOMINAL_KW) * col("runtime_hours"))
        .withColumn(
            "heating_supply_kwh",
            when(col("measured_supply_alloc").isNotNull(), col("measured_supply_alloc"))
            .when(col("runtime_hours") > 0, coalesce(col("estimated_supply_from_load"), lit(0.0)))
            .otherwise(lit(0.0)),
        )
        .withColumn(
            "energy_consumption_kwh",
            when(col("measured_input_alloc").isNotNull(), col("measured_input_alloc"))
            .when(col("runtime_hours") > 0, coalesce(col("heating_supply_kwh") / lit(BOILER_EFFICIENCY), lit(0.0)))
            .otherwise(lit(0.0)),
        )
        .withColumn("system_type", lit("heating"))
        .withColumn("equipment_id", expr("concat('heating_', equipment_num)"))
        .withColumn("avg_flow", col("avg_flow").cast("double"))
        .withColumn("max_flow", col("max_flow").cast("double"))
        .withColumn("min_flow", col("min_flow").cast("double"))
        .withColumn("avg_power", lit(None).cast("double"))
        .withColumn("max_power", lit(None).cast("double"))
        .withColumn("min_power", lit(None).cast("double"))
        .withColumn("energy_input_kwh", col("energy_consumption_kwh"))
        .withColumn("cooling_capacity_kw", lit(None).cast("double"))
        .withColumn("supply_kwh", col("heating_supply_kwh"))
        .withColumn("cooling_supply_kwh", lit(0.0))
        .withColumn("electric_supply_kwh", lit(0.0))
        .withColumn("start_count", lit(None).cast("double"))
        .withColumn("record_count", coalesce(col("boiler_record_count"), lit(0)) + coalesce(col("burner_record_count"), lit(0)))
        .withColumn("supply_price_type", lit("heating"))
        .withColumn(
            "metric_quality",
            when(col("measured_supply_alloc").isNotNull() & (col("group_weight") > 0), lit("gas_delta_allocated_by_load"))
            .when(col("measured_supply_alloc").isNotNull(), lit("gas_delta_equal_allocated"))
            .otherwise(lit("load_estimated")),
        )
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
    )

    return select_hourly(heating)


def build_cchp_hourly(fact):
    print("Building CCHP hourly table from cumulative heat/cooling/electric points...")

    cchp_cumulative = fact.filter((col("system_type") == "cchp") & col("point_code").isin("FT2005_RL_C", "P9_ADDH"))
    cchp_delta = (
        hourly_delta(cchp_cumulative, "delta_kwh", max_delta=200000)
        .withColumn("metric_name", when(col("point_code") == "FT2005_RL_C", lit("heating")).otherwise(lit("cooling")))
        .groupBy("station_id", "stat_hour", "dt")
        .pivot("metric_name", ["heating", "cooling"])
        .agg(spark_sum("delta_kwh"))
        .withColumnRenamed("heating", "heating_supply_kwh")
        .withColumnRenamed("cooling", "cooling_supply_kwh")
    )

    generator_delta = (
        hourly_delta(
            fact.filter((col("system_type") == "generator") & col("point_code").rlike("^SLG_JZ[0-9]+_DD_YG_ZZ1$")),
            "electric_supply_kwh",
            max_delta=500000,
        )
        .groupBy("station_id", "stat_hour", "dt")
        .agg(spark_sum("electric_supply_kwh").alias("electric_supply_kwh"))
    )

    generator_gas = (
        hourly_delta(
            fact.filter((col("system_type") == "generator") & (col("point_code") == "FDJ_GAS_TOTAL")),
            "gas_delta_m3",
            max_delta=300000,
        )
        .select("station_id", "stat_hour", "dt", "gas_delta_m3")
        .withColumn("energy_consumption_kwh", col("gas_delta_m3") * lit(GAS_KWH_PER_M3))
    )

    cchp_temp_raw = (
        fact.filter(col("system_type") == "cchp")
        .groupBy("station_id", "stat_hour", "dt")
        .agg(
            avg(when((col("point_code") == "TT2005_T") & col("value_clean").between(0, 120), col("value_clean"))).alias("avg_header_supply_temp"),
            spark_max(when((col("point_code") == "TT2005_T") & col("value_clean").between(0, 120), col("value_clean"))).alias("max_header_supply_temp"),
            spark_min(when((col("point_code") == "TT2005_T") & col("value_clean").between(0, 120), col("value_clean"))).alias("min_header_supply_temp"),
            avg(when((col("point_code") == "TT2004_T") & col("value_clean").between(0, 120), col("value_clean"))).alias("avg_header_return_temp"),
            avg(when((col("measure_role") == "supply_temp") & col("value_clean").between(0, 120), col("value_clean"))).alias("avg_role_supply_temp"),
            spark_max(when((col("measure_role") == "supply_temp") & col("value_clean").between(0, 120), col("value_clean"))).alias("max_role_supply_temp"),
            spark_min(when((col("measure_role") == "supply_temp") & col("value_clean").between(0, 120), col("value_clean"))).alias("min_role_supply_temp"),
            avg(when((col("measure_role") == "return_temp") & col("value_clean").between(0, 120), col("value_clean"))).alias("avg_role_return_temp"),
            avg(when((col("measure_role") == "pressure") & col("value_clean").between(0, 10), col("value_clean"))).alias("avg_pressure"),
            spark_max(when((col("measure_role") == "pressure") & col("value_clean").between(0, 10), col("value_clean"))).alias("max_pressure"),
            spark_min(when((col("measure_role") == "pressure") & col("value_clean").between(0, 10), col("value_clean"))).alias("min_pressure"),
            avg(when((col("measure_role") == "flow") & col("value_clean").between(0, 100000), col("value_clean"))).alias("avg_flow"),
            spark_max(when((col("measure_role") == "flow") & col("value_clean").between(0, 100000), col("value_clean"))).alias("max_flow"),
            spark_min(when((col("measure_role") == "flow") & col("value_clean").between(0, 100000), col("value_clean"))).alias("min_flow"),
            count("*").alias("cchp_record_count"),
        )
    )
    cchp_temp = (
        cchp_temp_raw.withColumn("avg_supply_temp", coalesce(col("avg_header_supply_temp"), col("avg_role_supply_temp")))
        .withColumn("max_supply_temp", coalesce(col("max_header_supply_temp"), col("max_role_supply_temp")))
        .withColumn("min_supply_temp", coalesce(col("min_header_supply_temp"), col("min_role_supply_temp")))
        .withColumn("avg_return_temp", coalesce(col("avg_header_return_temp"), col("avg_role_return_temp")))
    )

    generator_run = (
        fact.filter((col("system_type") == "generator") & col("point_code").rlike("^[0-9]+FDJ_RUN$"))
        .groupBy("station_id", "stat_hour", "dt")
        .agg(
            avg(when(col("value_clean").between(0, 1), col("value_clean"))).alias("runtime_hours"),
            spark_sum(when(col("value_clean") > 0, 1).otherwise(0)).alias("running_sample_count"),
            count("*").alias("generator_record_count"),
        )
    )

    cchp = (
        cchp_delta.join(generator_delta, ["station_id", "stat_hour", "dt"], "full")
        .join(generator_gas, ["station_id", "stat_hour", "dt"], "full")
        .join(cchp_temp, ["station_id", "stat_hour", "dt"], "left")
        .join(generator_run, ["station_id", "stat_hour", "dt"], "left")
        .withColumn("heating_supply_kwh", coalesce(col("heating_supply_kwh"), lit(0.0)))
        .withColumn("cooling_supply_kwh", coalesce(col("cooling_supply_kwh"), lit(0.0)))
        .withColumn("electric_supply_kwh", coalesce(col("electric_supply_kwh"), lit(0.0)))
        .withColumn("supply_kwh", col("heating_supply_kwh") + col("cooling_supply_kwh") + col("electric_supply_kwh"))
        .withColumn("energy_consumption_kwh", coalesce(col("energy_consumption_kwh"), when(col("supply_kwh") > 0, col("supply_kwh") / lit(0.8)).otherwise(lit(0.0))))
        .withColumn("energy_input_kwh", col("energy_consumption_kwh"))
        .withColumn("runtime_hours", when(col("supply_kwh") > 0, lit(1.0)).otherwise(coalesce(col("runtime_hours"), lit(0.0))))
        .withColumn("run_minutes", col("runtime_hours") * 60.0)
        .withColumn("operation_rate", col("runtime_hours") * 100.0)
        .withColumn("system_type", lit("cchp"))
        .withColumn("equipment_id", lit("cchp_system"))
        .withColumn("avg_flow", col("avg_flow").cast("double"))
        .withColumn("max_flow", col("max_flow").cast("double"))
        .withColumn("min_flow", col("min_flow").cast("double"))
        .withColumn("avg_power", lit(None).cast("double"))
        .withColumn("max_power", lit(None).cast("double"))
        .withColumn("min_power", lit(None).cast("double"))
        .withColumn("cooling_capacity_kw", col("cooling_supply_kwh"))
        .withColumn("cumulative_runtime_hours", lit(None).cast("double"))
        .withColumn("start_count", lit(None).cast("double"))
        .withColumn("record_count", coalesce(col("cchp_record_count"), lit(0)) + coalesce(col("generator_record_count"), lit(0)))
        .withColumn("supply_price_type", lit("mixed"))
        .withColumn("metric_quality", lit("cumulative_delta"))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
    )

    return select_hourly(cchp)


def build_system_hourly(spark):
    fact = load_fact(spark).cache()
    chiller = load_chiller_hourly(spark)
    heating = build_heating_hourly(fact)
    cchp = build_cchp_hourly(fact)

    hourly = chiller.unionByName(heating).unionByName(cchp)
    hourly = hourly.orderBy("system_type", "equipment_id", "stat_hour")

    print(f"Writing unified hourly table: {GOLD_SYSTEM_HOURLY}")
    hourly.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("system_type", "dt").save(GOLD_SYSTEM_HOURLY)

    verified = spark.read.format("delta").load(GOLD_SYSTEM_HOURLY)
    print("Unified hourly summary:")
    verified.groupBy("system_type").agg(
        count("*").alias("rows"),
        spark_sum("supply_kwh").alias("total_supply_kwh"),
        spark_sum("energy_consumption_kwh").alias("total_energy_kwh"),
    ).orderBy("system_type").show(50, truncate=False)

    fact.unpersist()
    return verified


def add_period_columns(hourly, period):
    base = (
        hourly.withColumn("stat_ts", to_timestamp(col("stat_hour"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("supply_value", coalesce(col("supply_kwh"), lit(0.0)))
        .withColumn("cooling_value", coalesce(col("cooling_supply_kwh"), lit(0.0)))
        .withColumn("heating_value", coalesce(col("heating_supply_kwh"), lit(0.0)))
        .withColumn("electric_value", coalesce(col("electric_supply_kwh"), lit(0.0)))
        .withColumn("energy_value", coalesce(col("energy_consumption_kwh"), lit(0.0)))
        .withColumn("run_minutes_value", coalesce(col("run_minutes"), lit(0.0)))
    )

    if period == "daily":
        return base.withColumn("stat_date", to_date(col("stat_ts"))), ["station_id", "system_type", "equipment_id", "stat_date"], lit(24.0)
    if period == "weekly":
        weekly = (
            base.withColumn("stat_year", year(col("stat_ts")))
            .withColumn("stat_week", weekofyear(col("stat_ts")))
            .withColumn("week_start_date", date_sub(to_date(col("stat_ts")), dayofweek(col("stat_ts")) - 2))
            .withColumn("week_end_date", expr("date_add(week_start_date, 6)"))
            .withColumn("stat_week_str", expr("concat(stat_year, '-W', lpad(stat_week, 2, '0'))"))
        )
        return weekly, ["station_id", "system_type", "equipment_id", "stat_year", "stat_week", "stat_week_str", "week_start_date", "week_end_date"], lit(168.0)

    monthly = (
        base.withColumn("stat_year", year(col("stat_ts")))
        .withColumn("stat_month", month(col("stat_ts")))
        .withColumn("month_start_date", trunc(col("stat_ts"), "month"))
        .withColumn("month_end_date", last_day(col("stat_ts")))
        .withColumn("days_in_month", expr("datediff(month_end_date, month_start_date) + 1"))
        .withColumn("stat_month_str", expr("concat(stat_year, '-', lpad(stat_month, 2, '0'))"))
    )
    return monthly, ["station_id", "system_type", "equipment_id", "stat_year", "stat_month", "stat_month_str", "month_start_date", "month_end_date", "days_in_month"], col("days_in_month") * lit(24.0)


def build_report(spark, hourly, period, prices):
    period_df, group_cols, expected_hours = add_period_columns(hourly, period)

    report = (
        period_df.groupBy(*group_cols)
        .agg(
            spark_max("supply_value").alias("peak_supply_kwh"),
            spark_min(when(col("supply_value") > 0, col("supply_value"))).alias("valley_supply_kwh"),
            spark_sum("supply_value").alias("total_supply_kwh"),
            spark_sum("cooling_value").alias("total_cooling_supply_kwh"),
            spark_sum("heating_value").alias("total_heating_supply_kwh"),
            spark_sum("electric_value").alias("total_electric_supply_kwh"),
            spark_sum("energy_value").alias("total_energy_consumption_kwh"),
            spark_sum("run_minutes_value").alias("total_run_minutes"),
            avg("supply_value").alias("avg_supply_kwh"),
            avg("avg_supply_temp").alias("avg_supply_temp"),
            avg("operation_rate").alias("avg_operation_rate"),
            count("*").alias("hour_count"),
            count(when(col("supply_kwh").isNull(), 1)).alias("missing_supply_hours"),
            count(when(col("energy_consumption_kwh").isNull(), 1)).alias("missing_energy_hours"),
            count(when((col("supply_value") > 0) & ((col("operation_rate") <= 0) | col("operation_rate").isNull()), 1)).alias("anomaly_count"),
        )
    )

    duration_window = Window.partitionBy(*group_cols)
    duration = (
        period_df.withColumn("period_avg_supply", avg("supply_value").over(duration_window))
        .withColumn("is_peak_period", when(col("supply_value") > col("period_avg_supply") * 1.2, 1).otherwise(0))
        .withColumn("is_valley_period", when((col("supply_value") < col("period_avg_supply") * 0.8) & (col("supply_value") > 0), 1).otherwise(0))
        .groupBy(*group_cols)
        .agg(
            spark_sum("is_peak_period").alias("peak_duration_hours"),
            spark_sum("is_valley_period").alias("valley_duration_hours"),
        )
    )

    report = (
        report.join(duration, group_cols, "left")
        .withColumn("peak_valley_ratio", when(col("valley_supply_kwh") > 0, col("peak_supply_kwh") / col("valley_supply_kwh")))
        .withColumn("avg_cop", when(col("total_energy_consumption_kwh") > 0, col("total_supply_kwh") / col("total_energy_consumption_kwh")))
        .withColumn("total_runtime_hours", col("total_run_minutes") / 60.0)
        .withColumn("equipment_utilization_rate", col("total_run_minutes") / (expected_hours * 60.0))
        .withColumn("load_factor", when(col("peak_supply_kwh") > 0, col("avg_supply_kwh") / col("peak_supply_kwh")))
        .withColumn("data_completeness_rate", col("hour_count") / expected_hours)
        .withColumn("total_energy_cost", col("total_energy_consumption_kwh") * lit(prices["electricity"]))
        .withColumn(
            "total_supply_revenue",
            col("total_cooling_supply_kwh") * lit(prices["cooling"])
            + col("total_heating_supply_kwh") * lit(prices["heating"])
            + col("total_electric_supply_kwh") * lit(prices["electricity"]),
        )
        .withColumn("net_profit", col("total_supply_revenue") - col("total_energy_cost"))
        .withColumn("peak_cooling_kwh", col("peak_supply_kwh"))
        .withColumn("valley_cooling_kwh", col("valley_supply_kwh"))
        .withColumn("avg_cooling_supply_kwh", col("avg_supply_kwh"))
        .withColumn("total_cooling_revenue", col("total_supply_revenue"))
        .withColumn("energy_cost", col("total_energy_cost"))
        .withColumn("cooling_revenue", col("total_supply_revenue"))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
    )

    if period == "daily":
        report = (
            report.withColumn("daily_operation_rate", col("equipment_utilization_rate") * 100.0)
            .withColumn("dt", col("stat_date"))
            .select(
                "station_id",
                "system_type",
                "equipment_id",
                "stat_date",
                "peak_supply_kwh",
                "valley_supply_kwh",
                "peak_cooling_kwh",
                "valley_cooling_kwh",
                "peak_valley_ratio",
                "peak_duration_hours",
                "valley_duration_hours",
                "total_supply_kwh",
                "total_cooling_supply_kwh",
                "total_heating_supply_kwh",
                "total_electric_supply_kwh",
                "total_energy_consumption_kwh",
                "total_runtime_hours",
                "total_run_minutes",
                "daily_operation_rate",
                "avg_supply_kwh",
                "avg_cooling_supply_kwh",
                "avg_supply_temp",
                "avg_cop",
                "equipment_utilization_rate",
                "load_factor",
                "total_energy_cost",
                "total_supply_revenue",
                "energy_cost",
                "cooling_revenue",
                "total_cooling_revenue",
                "net_profit",
                "data_completeness_rate",
                "hour_count",
                "missing_supply_hours",
                "missing_energy_hours",
                "anomaly_count",
                "dt",
                "created_at",
                "updated_at",
            )
        )
        output_path = GOLD_SYSTEM_DAILY
    elif period == "weekly":
        report = (
            report.withColumn("dt", col("week_start_date"))
            .select(
                "station_id",
                "system_type",
                "equipment_id",
                "stat_week_str",
                "week_start_date",
                "week_end_date",
                "peak_supply_kwh",
                "valley_supply_kwh",
                "peak_cooling_kwh",
                "valley_cooling_kwh",
                "peak_valley_ratio",
                "peak_duration_hours",
                "valley_duration_hours",
                "total_supply_kwh",
                "total_cooling_supply_kwh",
                "total_heating_supply_kwh",
                "total_electric_supply_kwh",
                "total_energy_consumption_kwh",
                "total_runtime_hours",
                "avg_supply_kwh",
                "avg_cooling_supply_kwh",
                "avg_supply_temp",
                "avg_cop",
                "equipment_utilization_rate",
                "load_factor",
                "total_energy_cost",
                "total_supply_revenue",
                "total_cooling_revenue",
                "net_profit",
                "data_completeness_rate",
                "hour_count",
                "missing_supply_hours",
                "missing_energy_hours",
                "anomaly_count",
                "dt",
                "created_at",
                "updated_at",
            )
        )
        output_path = GOLD_SYSTEM_WEEKLY
    else:
        report = (
            report.withColumn("dt", col("month_start_date"))
            .select(
                "station_id",
                "system_type",
                "equipment_id",
                "stat_month_str",
                "month_start_date",
                "month_end_date",
                "days_in_month",
                "peak_supply_kwh",
                "valley_supply_kwh",
                "peak_cooling_kwh",
                "valley_cooling_kwh",
                "peak_valley_ratio",
                "peak_duration_hours",
                "valley_duration_hours",
                "total_supply_kwh",
                "total_cooling_supply_kwh",
                "total_heating_supply_kwh",
                "total_electric_supply_kwh",
                "total_energy_consumption_kwh",
                "total_runtime_hours",
                "avg_supply_kwh",
                "avg_cooling_supply_kwh",
                "avg_supply_temp",
                "avg_cop",
                "equipment_utilization_rate",
                "load_factor",
                "total_energy_cost",
                "total_supply_revenue",
                "total_cooling_revenue",
                "net_profit",
                "data_completeness_rate",
                "hour_count",
                "missing_supply_hours",
                "missing_energy_hours",
                "anomaly_count",
                "dt",
                "created_at",
                "updated_at",
            )
        )
        output_path = GOLD_SYSTEM_MONTHLY

    print(f"Writing {period} system report: {output_path}")
    report.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("system_type", "dt").save(output_path)

    verified = spark.read.format("delta").load(output_path)
    print(f"{period} report summary:")
    verified.groupBy("system_type").agg(
        count("*").alias("rows"),
        spark_sum("total_supply_kwh").alias("total_supply_kwh"),
    ).orderBy("system_type").show(50, truncate=False)
    return verified


def build_forecast(hourly):
    print("Building baseline 24h system supply forecast...")
    hist = (
        hourly.withColumn("stat_ts", to_timestamp(col("stat_hour"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("hour_of_day", hour(col("stat_ts")))
        .withColumn("day_of_week", dayofweek(col("stat_ts")))
        .withColumn("supply_value", coalesce(col("supply_kwh"), lit(0.0)))
    )

    last_seen = hist.groupBy("station_id", "system_type", "equipment_id").agg(spark_max("stat_ts").alias("last_hour"))
    future = (
        last_seen.withColumn("forecast_hour_offset", explode(sequence(lit(1), lit(24))))
        .withColumn("target_ts", from_unixtime(unix_timestamp(col("last_hour")) + col("forecast_hour_offset") * 3600).cast("timestamp"))
        .withColumn("target_hour", date_format(col("target_ts"), "yyyy-MM-dd HH:00:00"))
        .withColumn("forecast_date", to_date(col("target_ts")))
        .withColumn("hour_of_day", hour(col("target_ts")))
        .withColumn("day_of_week", dayofweek(col("target_ts")))
    )

    profile = (
        hist.groupBy("system_type", "equipment_id", "hour_of_day", "day_of_week")
        .agg(avg("supply_value").alias("profile_supply_kwh"), count("*").alias("profile_sample_count"))
    )
    hour_profile = (
        hist.groupBy("system_type", "equipment_id", "hour_of_day")
        .agg(avg("supply_value").alias("hour_profile_supply_kwh"))
    )
    recent_avg = (
        hist.groupBy("system_type", "equipment_id")
        .agg(avg("supply_value").alias("recent_avg_supply_kwh"))
    )
    mix = (
        hist.groupBy("system_type", "equipment_id")
        .agg(
            spark_sum(coalesce(col("cooling_supply_kwh"), lit(0.0))).alias("mix_cooling"),
            spark_sum(coalesce(col("heating_supply_kwh"), lit(0.0))).alias("mix_heating"),
            spark_sum(coalesce(col("electric_supply_kwh"), lit(0.0))).alias("mix_electric"),
            spark_sum("supply_value").alias("mix_total"),
            spark_sum(coalesce(col("energy_consumption_kwh"), lit(0.0))).alias("mix_energy"),
        )
        .withColumn("cooling_share", when(col("mix_total") > 0, col("mix_cooling") / col("mix_total")).otherwise(lit(0.0)))
        .withColumn("heating_share", when(col("mix_total") > 0, col("mix_heating") / col("mix_total")).otherwise(lit(0.0)))
        .withColumn("electric_share", when(col("mix_total") > 0, col("mix_electric") / col("mix_total")).otherwise(lit(0.0)))
        .withColumn("energy_per_supply", when(col("mix_total") > 0, col("mix_energy") / col("mix_total")).otherwise(lit(1.0)))
    )

    forecast = (
        future.join(profile, ["system_type", "equipment_id", "hour_of_day", "day_of_week"], "left")
        .join(hour_profile, ["system_type", "equipment_id", "hour_of_day"], "left")
        .join(recent_avg, ["system_type", "equipment_id"], "left")
        .join(mix, ["system_type", "equipment_id"], "left")
        .withColumn("predicted_supply_kwh", coalesce(col("profile_supply_kwh"), col("hour_profile_supply_kwh"), col("recent_avg_supply_kwh"), lit(0.0)))
        .withColumn("predicted_cooling_kwh", col("predicted_supply_kwh") * col("cooling_share"))
        .withColumn("predicted_heating_kwh", col("predicted_supply_kwh") * col("heating_share"))
        .withColumn("predicted_electric_kwh", col("predicted_supply_kwh") * col("electric_share"))
        .withColumn("predicted_energy_kwh", col("predicted_supply_kwh") * col("energy_per_supply"))
        .withColumn("confidence_margin", col("predicted_supply_kwh") * when(col("profile_sample_count") >= 8, lit(0.15)).otherwise(lit(0.25)))
        .withColumn("confidence_lower", when(col("predicted_supply_kwh") - col("confidence_margin") > 0, col("predicted_supply_kwh") - col("confidence_margin")).otherwise(lit(0.0)))
        .withColumn("confidence_upper", col("predicted_supply_kwh") + col("confidence_margin"))
        .withColumn("forecast_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("model_version", lit("seasonal_hour_profile_v1"))
        .withColumn("dt", col("forecast_date"))
        .withColumn("created_at", current_timestamp())
        .select(
            "station_id",
            "system_type",
            "equipment_id",
            "forecast_time",
            "target_hour",
            "forecast_hour_offset",
            "predicted_supply_kwh",
            "predicted_cooling_kwh",
            "predicted_heating_kwh",
            "predicted_electric_kwh",
            "predicted_energy_kwh",
            "confidence_lower",
            "confidence_upper",
            "profile_sample_count",
            "model_version",
            "dt",
            "created_at",
        )
    )

    print(f"Writing system forecast: {GOLD_SYSTEM_FORECAST}")
    forecast.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("system_type", "dt").save(GOLD_SYSTEM_FORECAST)
    forecast.groupBy("system_type").agg(count("*").alias("rows"), spark_sum("predicted_supply_kwh").alias("predicted_supply_kwh")).show(50, truncate=False)
    return forecast


def build_revenue_forecast(forecast, prices):
    print("Building system revenue forecast...")
    revenue = (
        forecast.withColumn("forecast_date", to_date(col("target_hour")))
        .withColumn("forecast_hour", hour(to_timestamp(col("target_hour"), "yyyy-MM-dd HH:mm:ss")))
        .withColumn("energy_price", lit(prices["electricity"]))
        .withColumn("cooling_price", lit(prices["cooling"]))
        .withColumn("heating_price", lit(prices["heating"]))
        .withColumn(
            "predicted_supply_revenue",
            col("predicted_cooling_kwh") * col("cooling_price")
            + col("predicted_heating_kwh") * col("heating_price")
            + col("predicted_electric_kwh") * col("energy_price"),
        )
        .withColumn("predicted_energy_cost", col("predicted_energy_kwh") * col("energy_price"))
        .withColumn("predicted_profit", col("predicted_supply_revenue") - col("predicted_energy_cost"))
        .withColumn("profit_margin", when(col("predicted_supply_revenue") > 0, col("predicted_profit") / col("predicted_supply_revenue")).otherwise(lit(0.0)))
        .select(
            "station_id",
            "system_type",
            "equipment_id",
            "forecast_date",
            "target_hour",
            "forecast_hour",
            "predicted_supply_kwh",
            "predicted_cooling_kwh",
            "predicted_heating_kwh",
            "predicted_electric_kwh",
            "predicted_energy_kwh",
            "energy_price",
            "cooling_price",
            "heating_price",
            "predicted_energy_cost",
            "predicted_supply_revenue",
            "predicted_profit",
            "profit_margin",
            "model_version",
            "dt",
            "created_at",
        )
    )

    print(f"Writing system revenue forecast: {GOLD_SYSTEM_REVENUE_FORECAST}")
    revenue.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("system_type", "dt").save(GOLD_SYSTEM_REVENUE_FORECAST)
    revenue.groupBy("system_type").agg(count("*").alias("rows"), spark_sum("predicted_profit").alias("predicted_profit")).show(50, truncate=False)
    return revenue


def main():
    spark = create_spark_session()
    try:
        print("=" * 80)
        print("Unified system Gold generation started")
        print("=" * 80)

        prices = read_price_map(spark)
        print(f"Price map: {prices}")

        hourly = build_system_hourly(spark).cache()
        daily = build_report(spark, hourly, "daily", prices)
        weekly = build_report(spark, hourly, "weekly", prices)
        monthly = build_report(spark, hourly, "monthly", prices)
        forecast = build_forecast(hourly)
        build_revenue_forecast(forecast, prices)

        print("Final Gold counts:")
        for name, df in [
            ("hourly", hourly),
            ("daily", daily),
            ("weekly", weekly),
            ("monthly", monthly),
            ("forecast", forecast),
        ]:
            print(f"{name}: {df.count():,}")

        print("=" * 80)
        print("Unified system Gold generation completed")
        print("=" * 80)
    except Exception as exc:
        print(f"Failed to generate system Gold tables: {exc}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
