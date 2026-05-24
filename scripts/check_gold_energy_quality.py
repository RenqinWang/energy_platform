#!/usr/bin/env python3
"""Check current Gold-layer Delta tables with an emphasis on energy fields."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    count,
    countDistinct,
    date_format,
    expr,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    stddev,
    sum as spark_sum,
    to_date,
    when,
)


LAKE = "hdfs://node1:9000/lake"
TABLES = {
    "silver_chiller_status": f"{LAKE}/silver/silver_chiller_status",
    "gold_supply_curve_hourly": f"{LAKE}/gold/gold_supply_curve_hourly",
    "gold_report_daily": f"{LAKE}/gold/gold_report_daily",
    "gold_report_weekly": f"{LAKE}/gold/gold_report_weekly",
    "gold_report_monthly": f"{LAKE}/gold/gold_report_monthly",
    "gold_forecast_supply": f"{LAKE}/gold/gold_forecast_supply",
    "gold_revenue_forecast": f"{LAKE}/gold/gold_revenue_forecast",
    "gold_data_quality": f"{LAKE}/gold/gold_data_quality",
    "gold_operation_advice": f"{LAKE}/gold/gold_operation_advice",
}

ENERGY_COLUMNS = {
    "gold_supply_curve_hourly": [
        "avg_power",
        "energy_consumption_kwh",
        "cooling_capacity_kw",
        "cooling_supply_kwh",
        "runtime_hours",
        "running_sample_count",
        "run_minutes",
        "operation_rate",
        "record_count",
    ],
    "gold_report_daily": [
        "total_energy_consumption_kwh",
        "total_cooling_supply_kwh",
        "daily_operation_rate",
        "avg_cop",
        "energy_cost",
        "cooling_revenue",
        "net_profit",
        "hour_count",
    ],
    "gold_report_weekly": [
        "total_energy_consumption_kwh",
        "total_cooling_supply_kwh",
        "avg_operation_rate",
        "avg_cop",
        "total_energy_cost",
        "total_cooling_revenue",
        "net_profit",
    ],
    "gold_report_monthly": [
        "total_energy_consumption_kwh",
        "total_cooling_supply_kwh",
        "avg_operation_rate",
        "avg_cop",
        "total_energy_cost",
        "total_cooling_revenue",
        "net_profit",
    ],
    "gold_forecast_supply": [
        "predicted_cooling_kwh",
        "confidence_lower",
        "confidence_upper",
    ],
    "gold_revenue_forecast": [
        "predicted_cooling_kwh",
        "predicted_energy_kwh",
        "predicted_energy_cost",
        "predicted_cooling_revenue",
        "predicted_profit",
        "profit_margin",
        "energy_price",
        "cooling_price",
    ],
    "gold_data_quality": [
        "running_records",
        "missing_running_power_records",
        "missing_running_energy_records",
        "missing_running_cooling_records",
        "power_valid_rate",
        "energy_valid_rate",
        "data_quality_score",
    ],
}


def spark_session():
    spark = (
        SparkSession.builder.appName("CheckGoldEnergyQuality")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.hadoop.fs.defaultFS", "hdfs://node1:9000")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_table(spark, name):
    try:
        return spark.read.format("delta").load(TABLES[name])
    except Exception as exc:
        print(f"TABLE_EXISTS|{name}|false|{str(exc).splitlines()[0]}")
        return None


def print_overview(name, df):
    columns = df.columns
    aggregations = [
        count(lit(1)).alias("rows"),
    ]
    if "station_id" in columns:
        aggregations.append(countDistinct("station_id").alias("stations"))
    if "equipment_id" in columns:
        aggregations.append(countDistinct("equipment_id").alias("equipment"))
    if "dt" in columns:
        aggregations.extend([spark_min("dt").alias("min_dt"), spark_max("dt").alias("max_dt")])
    if "stat_hour" in columns:
        aggregations.extend([spark_min("stat_hour").alias("min_stat_hour"), spark_max("stat_hour").alias("max_stat_hour")])
    if "stat_date" in columns:
        aggregations.extend([spark_min("stat_date").alias("min_stat_date"), spark_max("stat_date").alias("max_stat_date")])
    if "target_hour" in columns:
        aggregations.extend([spark_min("target_hour").alias("min_target_hour"), spark_max("target_hour").alias("max_target_hour")])
    row = df.agg(*aggregations).collect()[0].asDict()
    payload = "|".join(f"{key}={value}" for key, value in row.items())
    print(f"OVERVIEW|{name}|columns={len(columns)}|{payload}")
    print(f"SCHEMA|{name}|{','.join(columns)}")


def print_column_quality(name, df):
    rows = df.count()
    columns = df.columns
    for column_name in ENERGY_COLUMNS.get(name, []):
        if column_name not in columns:
            print(f"COLUMN_MISSING|{name}|{column_name}")
            continue
        row = df.agg(
            count(when(col(column_name).isNotNull(), 1)).alias("non_null"),
            count(when(col(column_name).isNull(), 1)).alias("nulls"),
            count(when(col(column_name) == 0, 1)).alias("zeros"),
            count(when(col(column_name) < 0, 1)).alias("negatives"),
            spark_round(spark_min(column_name), 6).alias("min"),
            spark_round(spark_max(column_name), 6).alias("max"),
            spark_round(avg(column_name), 6).alias("avg"),
            spark_round(stddev(column_name), 6).alias("stddev"),
        ).collect()[0].asDict()
        row["non_null_pct"] = round(row["non_null"] / rows * 100, 4) if rows else 0
        payload = "|".join(f"{key}={value}" for key, value in row.items())
        print(f"COLUMN_QUALITY|{name}|{column_name}|{payload}")


def print_silver_source_checks(silver):
    row = silver.agg(
        count(lit(1)).alias("rows"),
        count(when(col("power").isNotNull(), 1)).alias("power_non_null_rows"),
        count(when(col("power").isNull(), 1)).alias("power_null_rows"),
        count(when(col("power") == 0, 1)).alias("power_zero_rows"),
        count(when(col("power") < 0, 1)).alias("power_negative_rows"),
        spark_round(spark_min("power"), 6).alias("power_min"),
        spark_round(spark_max("power"), 6).alias("power_max"),
        spark_round(avg("power"), 6).alias("power_avg"),
        count(when(col("run_flag").isNotNull(), 1)).alias("run_flag_non_null_rows"),
        count(when(col("run_flag").isNull(), 1)).alias("run_flag_null_rows"),
        count(when(col("run_flag") < 0, 1)).alias("run_flag_negative_rows"),
        count(when(col("run_flag") > 0, 1)).alias("run_flag_positive_rows"),
    ).collect()[0].asDict()
    row["power_non_null_pct"] = round(row["power_non_null_rows"] / row["rows"] * 100, 4) if row["rows"] else 0
    row["run_flag_positive_pct"] = round(row["run_flag_positive_rows"] / row["rows"] * 100, 4) if row["rows"] else 0
    print("SILVER_SOURCE_QUALITY|" + "|".join(f"{key}={value}" for key, value in row.items()))

    print("SILVER_RUN_FLAG_DISTRIBUTION_BEGIN")
    silver.groupBy("run_flag").count().orderBy("run_flag").show(50, truncate=False)
    print("SILVER_RUN_FLAG_DISTRIBUTION_END")


def print_hourly_checks(silver, hourly):
    silver_hourly = silver.withColumn(
        "stat_hour", date_format(col("stat_time"), "yyyy-MM-dd HH:00:00")
    ).select("station_id", "equipment_id", "stat_hour", "dt").distinct()

    silver_groups = silver_hourly.count()
    gold_groups = hourly.select("station_id", "equipment_id", "stat_hour", "dt").distinct().count()
    missing_in_gold = silver_hourly.join(
        hourly.select("station_id", "equipment_id", "stat_hour", "dt").distinct(),
        ["station_id", "equipment_id", "stat_hour", "dt"],
        "left_anti",
    ).count()
    extra_in_gold = hourly.select("station_id", "equipment_id", "stat_hour", "dt").distinct().join(
        silver_hourly,
        ["station_id", "equipment_id", "stat_hour", "dt"],
        "left_anti",
    ).count()
    print(
        "HOURLY_KEY_COVERAGE|"
        f"silver_groups={silver_groups}|gold_groups={gold_groups}|"
        f"missing_in_gold={missing_in_gold}|extra_in_gold={extra_in_gold}"
    )

    consistency = hourly.agg(
        count(when(spark_abs(col("energy_consumption_kwh") - col("avg_power") * col("runtime_hours")) > 0.0001, 1)).alias("energy_formula_mismatch"),
        count(when(spark_abs(col("operation_rate") - col("run_minutes") / 60.0 * 100.0) > 0.0001, 1)).alias("operation_formula_mismatch"),
        count(when(spark_abs(col("cooling_supply_kwh") - col("cooling_capacity_kw") * col("runtime_hours")) > 0.0001, 1)).alias("cooling_formula_mismatch"),
        count(when((col("operation_rate") < 0) | (col("operation_rate") > 100), 1)).alias("operation_out_of_range"),
        count(when((col("run_minutes") < 0) | (col("run_minutes") > 60), 1)).alias("run_minutes_out_of_range"),
    ).collect()[0].asDict()
    print("HOURLY_FORMULA_CHECK|" + "|".join(f"{key}={value}" for key, value in consistency.items()))

    print("HOURLY_RECORD_COUNT_DISTRIBUTION_BEGIN")
    hourly.groupBy("record_count").count().orderBy("record_count").show(80, truncate=False)
    print("HOURLY_RECORD_COUNT_DISTRIBUTION_END")

    print("HOURLY_ENERGY_BY_EQUIPMENT_BEGIN")
    hourly.groupBy("equipment_id").agg(
        count(lit(1)).alias("hours"),
        count(when(col("energy_consumption_kwh").isNotNull(), 1)).alias("energy_non_null_hours"),
        count(when(col("energy_consumption_kwh") > 0, 1)).alias("energy_positive_hours"),
        spark_round(spark_sum("energy_consumption_kwh"), 3).alias("total_energy_kwh"),
        spark_round(spark_sum("cooling_supply_kwh"), 3).alias("total_cooling_kwh"),
        spark_round(avg("operation_rate"), 3).alias("avg_operation_rate"),
    ).orderBy("equipment_id").show(50, truncate=False)
    print("HOURLY_ENERGY_BY_EQUIPMENT_END")


def print_daily_reconciliation(hourly, daily):
    hourly_daily = hourly.withColumn("stat_date", to_date("stat_hour")).groupBy(
        "station_id", "equipment_id", "stat_date"
    ).agg(
        spark_sum("energy_consumption_kwh").alias("hourly_energy"),
        spark_sum("cooling_supply_kwh").alias("hourly_cooling"),
        spark_sum("run_minutes").alias("hourly_run_minutes"),
        count(lit(1)).alias("hourly_hour_count"),
    ).withColumn("_hourly_present", lit(1))
    joined = hourly_daily.join(
        daily.select(
            "station_id",
            "equipment_id",
            "stat_date",
            col("total_energy_consumption_kwh").alias("daily_energy"),
            col("total_cooling_supply_kwh").alias("daily_cooling"),
            col("total_run_minutes").alias("daily_run_minutes"),
            col("hour_count").alias("daily_hour_count"),
        ).withColumn("_daily_present", lit(1)),
        ["station_id", "equipment_id", "stat_date"],
        "full_outer",
    )
    row = joined.agg(
        count(when(col("_daily_present").isNull(), 1)).alias("missing_daily_rows"),
        count(when(col("_hourly_present").isNull(), 1)).alias("extra_daily_rows"),
        count(when(spark_abs(col("hourly_energy") - col("daily_energy")) > 0.0001, 1)).alias("energy_mismatch_rows"),
        count(when(spark_abs(col("hourly_cooling") - col("daily_cooling")) > 0.0001, 1)).alias("cooling_mismatch_rows"),
        count(when(spark_abs(col("hourly_run_minutes") - col("daily_run_minutes")) > 0.0001, 1)).alias("run_minutes_mismatch_rows"),
        count(when(col("hourly_hour_count") != col("daily_hour_count"), 1)).alias("hour_count_mismatch_rows"),
        spark_round(spark_sum("hourly_energy"), 3).alias("hourly_total_energy"),
        spark_round(spark_sum("daily_energy"), 3).alias("daily_total_energy"),
    ).collect()[0].asDict()
    print("DAILY_RECONCILIATION|" + "|".join(f"{key}={value}" for key, value in row.items()))


def print_revenue_reconciliation(forecast, revenue):
    if "forecast_hour" not in revenue.columns:
        print("REVENUE_RECONCILIATION|skipped=missing_forecast_hour")
        return
    forecast_keyed = forecast.withColumn("forecast_date", to_date("target_hour")).withColumn(
        "forecast_hour", expr("hour(target_hour)")
    ).select(
        "station_id",
        "equipment_id",
        "forecast_date",
        "forecast_hour",
        col("predicted_cooling_kwh").alias("forecast_predicted_cooling_kwh"),
    ).withColumn("_forecast_present", lit(1))
    revenue_keyed = revenue.select(
        "station_id",
        "equipment_id",
        "forecast_date",
        "forecast_hour",
        col("predicted_cooling_kwh").alias("revenue_predicted_cooling_kwh"),
        "predicted_energy_kwh",
        "energy_price",
        "cooling_price",
        "predicted_energy_cost",
        "predicted_cooling_revenue",
        "predicted_profit",
    ).withColumn("_revenue_present", lit(1))
    joined = forecast_keyed.join(
        revenue_keyed,
        ["station_id", "equipment_id", "forecast_date", "forecast_hour"],
        "full_outer",
    )
    row = joined.agg(
        count(when(col("_forecast_present").isNull(), 1)).alias("missing_forecast_rows"),
        count(when(col("_revenue_present").isNull(), 1)).alias("missing_revenue_rows"),
        count(when(spark_abs(col("forecast_predicted_cooling_kwh") - col("revenue_predicted_cooling_kwh")) > 0.0001, 1)).alias("cooling_mismatch_rows"),
        count(when(spark_abs(col("predicted_energy_cost") - col("predicted_energy_kwh") * col("energy_price")) > 0.0001, 1)).alias("energy_cost_mismatch_rows"),
        count(when(spark_abs(col("predicted_cooling_revenue") - col("revenue_predicted_cooling_kwh") * col("cooling_price")) > 0.0001, 1)).alias("cooling_revenue_mismatch_rows"),
        count(when(spark_abs(col("predicted_profit") - (col("predicted_cooling_revenue") - col("predicted_energy_cost"))) > 0.0001, 1)).alias("profit_mismatch_rows"),
        count(when(col("predicted_energy_cost") < 0, 1)).alias("negative_cost_rows"),
        count(when(col("predicted_profit").isNull(), 1)).alias("null_profit_rows"),
    ).collect()[0].asDict()
    print("REVENUE_RECONCILIATION|" + "|".join(f"{key}={value}" for key, value in row.items()))


def main():
    spark = spark_session()
    print("GOLD_ENERGY_QUALITY_BEGIN")
    print(f"SPARK_MASTER={spark.sparkContext.master}")
    print(f"APP_ID={spark.sparkContext.applicationId}")

    dataframes = {}
    for name in TABLES:
        df = read_table(spark, name)
        if df is None:
            continue
        dataframes[name] = df
        print_overview(name, df)
        print_column_quality(name, df)

    if "silver_chiller_status" in dataframes and "gold_supply_curve_hourly" in dataframes:
        print_silver_source_checks(dataframes["silver_chiller_status"])
        print_hourly_checks(dataframes["silver_chiller_status"], dataframes["gold_supply_curve_hourly"])

    if "gold_supply_curve_hourly" in dataframes and "gold_report_daily" in dataframes:
        print_daily_reconciliation(dataframes["gold_supply_curve_hourly"], dataframes["gold_report_daily"])

    if "gold_forecast_supply" in dataframes and "gold_revenue_forecast" in dataframes:
        print_revenue_reconciliation(dataframes["gold_forecast_supply"], dataframes["gold_revenue_forecast"])

    print("GOLD_ENERGY_QUALITY_END")
    spark.stop()


if __name__ == "__main__":
    main()
