#!/usr/bin/env python3
"""Train and publish trend forecasts for chiller, heating, and CCHP systems."""

import json
import math
import sys

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    coalesce,
    col,
    cos,
    count,
    current_timestamp,
    date_format,
    dayofweek,
    explode,
    first,
    greatest,
    hour,
    last,
    lit,
    max as spark_max,
    month,
    row_number,
    sequence,
    sin,
    stddev_pop,
    sum as spark_sum,
    to_date,
    to_timestamp,
    unix_timestamp,
    when,
    from_unixtime,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


HDFS_ROOT = "hdfs://node1:9000/lake"
GOLD_SYSTEM_HOURLY = f"{HDFS_ROOT}/gold/gold_system_supply_hourly"
SILVER_PRICE_DIM = f"{HDFS_ROOT}/silver/silver_price_dim"
GOLD_SYSTEM_FORECAST = f"{HDFS_ROOT}/gold/gold_system_forecast_supply"
GOLD_SYSTEM_FORECAST_METRICS = f"{HDFS_ROOT}/gold/gold_system_forecast_metrics"
GOLD_SYSTEM_REVENUE_FORECAST = f"{HDFS_ROOT}/gold/gold_system_revenue_forecast"

MODEL_VERSION = "random_forest_system_v3"
FORECAST_HOURS = 24

FEATURE_COLS = [
    "month_num",
    "season_index",
    "date_type_index",
    "is_weekend",
    "day_of_week",
    "hour_of_day",
    "hour_sin",
    "hour_cos",
    "day_sin",
    "day_cos",
    "lag_1h",
    "lag_24h",
    "lag_168h",
    "rolling_24h_avg",
    "rolling_24h_max",
    "rolling_168h_avg",
    "rolling_168h_max",
    "avg_supply_temp",
    "avg_return_temp",
    "temp_diff",
    "avg_pressure",
    "avg_flow",
    "avg_power",
    "operation_rate",
]

FORECAST_SCHEMA = StructType([
    StructField("station_id", StringType(), True),
    StructField("system_type", StringType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("forecast_time", StringType(), True),
    StructField("target_hour", StringType(), True),
    StructField("forecast_hour_offset", IntegerType(), True),
    StructField("predicted_supply_kwh", DoubleType(), True),
    StructField("predicted_cooling_kwh", DoubleType(), True),
    StructField("predicted_heating_kwh", DoubleType(), True),
    StructField("predicted_electric_kwh", DoubleType(), True),
    StructField("predicted_energy_kwh", DoubleType(), True),
    StructField("confidence_lower", DoubleType(), True),
    StructField("confidence_upper", DoubleType(), True),
    StructField("recent_24_supply_kwh", DoubleType(), True),
    StructField("recent_24_operation_rate", DoubleType(), True),
    StructField("current_operation_status", StringType(), True),
    StructField("forecast_interpretation", StringType(), True),
    StructField("profile_sample_count", IntegerType(), True),
    StructField("model_version", StringType(), True),
    StructField("algorithm", StringType(), True),
    StructField("feature_set", StringType(), True),
    StructField("train_test_split", StringType(), True),
    StructField("dt", DateType(), True),
    StructField("created_at", TimestampType(), True),
])

METRICS_SCHEMA = StructType([
    StructField("station_id", StringType(), True),
    StructField("system_type", StringType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("model_version", StringType(), True),
    StructField("algorithm", StringType(), True),
    StructField("target_col", StringType(), True),
    StructField("feature_list", StringType(), True),
    StructField("feature_design", StringType(), True),
    StructField("window_design", StringType(), True),
    StructField("model_reason", StringType(), True),
    StructField("training_method", StringType(), True),
    StructField("evaluation_method", StringType(), True),
    StructField("train_start", StringType(), True),
    StructField("train_end", StringType(), True),
    StructField("test_start", StringType(), True),
    StructField("test_end", StringType(), True),
    StructField("train_row_count", IntegerType(), True),
    StructField("test_row_count", IntegerType(), True),
    StructField("mae", DoubleType(), True),
    StructField("rmse", DoubleType(), True),
    StructField("r2", DoubleType(), True),
    StructField("mape", DoubleType(), True),
    StructField("residual_std", DoubleType(), True),
    StructField("top_features", StringType(), True),
    StructField("result_summary", StringType(), True),
    StructField("dt", DateType(), True),
    StructField("created_at", TimestampType(), True),
])


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("GenerateSystemTrendForecast")
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
    rows = (
        spark.read.format("delta")
        .load(SILVER_PRICE_DIM)
        .groupBy("price_type")
        .agg(avg("price").alias("avg_price"))
        .collect()
    )
    prices = {row["price_type"]: float(row["avg_price"]) for row in rows if row["avg_price"] is not None}
    return {
        "electricity": prices.get("electricity", 0.8),
        "cooling": prices.get("cooling", 0.3),
        "heating": prices.get("heating", 0.35),
    }


def load_hourly(spark):
    return (
        spark.read.format("delta")
        .load(GOLD_SYSTEM_HOURLY)
        .withColumn("stat_ts", to_timestamp(col("stat_hour"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("target_supply_kwh", coalesce(col("supply_kwh"), col("cooling_supply_kwh"), lit(0.0)))
        .filter(col("stat_ts").isNotNull())
    )


def add_time_features(df):
    return (
        df.withColumn("month_num", month(col("stat_ts")).cast("double"))
        .withColumn("day_of_week", dayofweek(col("stat_ts")).cast("double"))
        .withColumn("hour_of_day", hour(col("stat_ts")).cast("double"))
        .withColumn("is_weekend", when(col("day_of_week").isin(1.0, 7.0), 1.0).otherwise(0.0))
        .withColumn("date_type_index", col("is_weekend"))
        .withColumn(
            "season_index",
            when(col("month_num").isin(12.0, 1.0, 2.0), 0.0)
            .when(col("month_num").isin(3.0, 4.0, 5.0), 1.0)
            .when(col("month_num").isin(6.0, 7.0, 8.0), 2.0)
            .otherwise(3.0),
        )
        .withColumn("hour_sin", sin(col("hour_of_day") * lit(2.0 * math.pi / 24.0)))
        .withColumn("hour_cos", cos(col("hour_of_day") * lit(2.0 * math.pi / 24.0)))
        .withColumn("day_sin", sin(col("day_of_week") * lit(2.0 * math.pi / 7.0)))
        .withColumn("day_cos", cos(col("day_of_week") * lit(2.0 * math.pi / 7.0)))
    )


def build_training_features(hourly):
    base = add_time_features(hourly)

    order_window = Window.partitionBy("station_id", "equipment_id").orderBy("stat_ts")
    rolling_24 = order_window.rowsBetween(-24, -1)
    rolling_168 = order_window.rowsBetween(-168, -1)

    return (
        base.withColumn("lag_1h", last("target_supply_kwh").over(order_window.rowsBetween(-1, -1)))
        .withColumn("lag_24h", last("target_supply_kwh").over(order_window.rowsBetween(-24, -24)))
        .withColumn("lag_168h", last("target_supply_kwh").over(order_window.rowsBetween(-168, -168)))
        .withColumn("rolling_24h_avg", avg("target_supply_kwh").over(rolling_24))
        .withColumn("rolling_24h_max", spark_max("target_supply_kwh").over(rolling_24))
        .withColumn("rolling_168h_avg", avg("target_supply_kwh").over(rolling_168))
        .withColumn("rolling_168h_max", spark_max("target_supply_kwh").over(rolling_168))
        .withColumn(
            "temp_diff",
            when(
                col("system_type") == "chiller",
                coalesce(col("avg_return_temp"), lit(0.0)) - coalesce(col("avg_supply_temp"), lit(0.0)),
            ).otherwise(coalesce(col("avg_supply_temp"), lit(0.0)) - coalesce(col("avg_return_temp"), lit(0.0))),
        )
        .na.fill(0.0, subset=FEATURE_COLS + ["target_supply_kwh"])
    )


def train_and_predict_systems(spark, hourly):
    print("Training random forest models for all systems...")
    features = build_training_features(hourly).cache()
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features", handleInvalid="keep")

    equipment_rows = (
        features.select("station_id", "system_type", "equipment_id")
        .distinct()
        .orderBy("system_type", "equipment_id")
        .collect()
    )

    predictions = []
    metrics_rows = []

    evaluator_rmse = RegressionEvaluator(labelCol="target_supply_kwh", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="target_supply_kwh", predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="target_supply_kwh", predictionCol="prediction", metricName="r2")

    for row in equipment_rows:
        station_id = row["station_id"]
        system_type = row["system_type"]
        equipment_id = row["equipment_id"]
        print(f"  Training {system_type}/{equipment_id}...")

        eq_df = features.filter(
            (col("station_id") == station_id)
            & (col("system_type") == system_type)
            & (col("equipment_id") == equipment_id)
        )
        split_window = Window.partitionBy("station_id", "system_type", "equipment_id").orderBy("stat_ts")
        eq_df = (
            eq_df.withColumn("row_num", row_number().over(split_window))
            .withColumn("row_count", count("*").over(Window.partitionBy("station_id", "system_type", "equipment_id")))
        )
        train_df = eq_df.filter(col("row_num") <= col("row_count") * lit(0.8))
        test_df = eq_df.filter(col("row_num") > col("row_count") * lit(0.8))

        train_count = train_df.count()
        test_count = test_df.count()
        if train_count < 100 or test_count < 24:
            print(f"  Skip {system_type}/{equipment_id}: insufficient rows train={train_count}, test={test_count}")
            continue

        train_vec = assembler.transform(train_df)
        test_vec = assembler.transform(test_df)

        model = RandomForestRegressor(
            featuresCol="features",
            labelCol="target_supply_kwh",
            predictionCol="prediction",
            numTrees=30,
            maxDepth=6,
            minInstancesPerNode=2,
            seed=42,
        ).fit(train_vec)

        test_pred = model.transform(test_vec).withColumn("prediction", greatest(col("prediction"), lit(0.0)))
        rmse = float(evaluator_rmse.evaluate(test_pred))
        mae = float(evaluator_mae.evaluate(test_pred))
        r2 = float(evaluator_r2.evaluate(test_pred))
        if not math.isfinite(r2):
            r2 = None

        metric_agg = test_pred.agg(
            avg(when(col("target_supply_kwh") > 0, spark_abs((col("target_supply_kwh") - col("prediction")) / col("target_supply_kwh")) * 100.0)).alias("mape"),
            stddev_pop(col("target_supply_kwh") - col("prediction")).alias("residual_std"),
            first("stat_ts").alias("test_start"),
            spark_max("stat_ts").alias("test_end"),
        ).collect()[0]
        train_range = train_df.agg(first("stat_ts").alias("train_start"), spark_max("stat_ts").alias("train_end")).collect()[0]
        mape = float(metric_agg["mape"]) if metric_agg["mape"] is not None else None
        residual_std = float(metric_agg["residual_std"]) if metric_agg["residual_std"] is not None else rmse

        top_features = sorted(
            zip(FEATURE_COLS, model.featureImportances.toArray().tolist()),
            key=lambda item: item[1],
            reverse=True,
        )[:8]

        future_features = build_future_features_for_equipment(hourly, features, station_id, system_type, equipment_id)
        future_vec = assembler.transform(future_features)
        future_pred = (
            model.transform(future_vec)
            .withColumn("prediction", greatest(col("prediction"), lit(0.0)))
            .withColumn("recent_24_supply_kwh", coalesce(col("recent_24_supply_kwh"), lit(0.0)))
            .withColumn("recent_24_operation_rate", coalesce(col("recent_24_operation_rate"), lit(0.0)))
            .withColumn(
                "is_recently_inactive",
                (col("recent_24_supply_kwh") <= lit(1e-6))
                & (col("recent_24_operation_rate") <= lit(1e-6)),
            )
            .withColumn("predicted_supply_kwh", col("prediction"))
            .withColumn("confidence_lower", greatest(col("prediction") - lit(1.28 * rmse), lit(0.0)))
            .withColumn("confidence_upper", col("prediction") + lit(1.28 * rmse))
            .withColumn(
                "current_operation_status",
                when(col("is_recently_inactive"), lit("recent_inactive")).otherwise(lit("recent_active")),
            )
            .withColumn(
                "forecast_interpretation",
                when(
                    col("is_recently_inactive"),
                    lit("最近24小时持续停机，预测值代表历史规律下的潜在供能需求，不代表已调度启机。"),
                ).otherwise(lit("最近24小时存在运行，预测值可作为连续运行趋势参考。")),
            )
            .withColumn("forecast_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("target_hour", date_format(col("target_ts"), "yyyy-MM-dd HH:00:00"))
            .withColumn("predicted_cooling_kwh", col("predicted_supply_kwh") * col("cooling_share"))
            .withColumn("predicted_heating_kwh", col("predicted_supply_kwh") * col("heating_share"))
            .withColumn("predicted_electric_kwh", col("predicted_supply_kwh") * col("electric_share"))
            .withColumn("predicted_energy_kwh", col("predicted_supply_kwh") * col("energy_per_supply"))
            .withColumn("profile_sample_count", lit(train_count))
            .withColumn("model_version", lit(MODEL_VERSION))
            .withColumn("algorithm", lit("RandomForestRegressor"))
            .withColumn("feature_set", lit("season_date_time_lag_rolling_load_v1"))
            .withColumn("train_test_split", lit("time_ordered_80_20"))
            .withColumn("dt", to_date(col("target_ts")))
            .withColumn("created_at", current_timestamp())
            .select(*[field.name for field in FORECAST_SCHEMA.fields])
        )
        predictions.append(future_pred)

        r2_text = f"{r2:.3f}" if r2 is not None else "N/A"
        result_summary = (
            f"{system_type}/{equipment_id} 测试集RMSE={rmse:.2f}kWh，MAE={mae:.2f}kWh，"
            f"R2={r2_text}；未来24小时按季节/日期/时段和历史负荷窗口预测，"
            "最近24小时运行状态仅用于解释和运行建议，不对趋势预测强制归零。"
        )
        metrics_rows.append({
            "station_id": station_id,
            "system_type": system_type,
            "equipment_id": equipment_id,
            "model_version": MODEL_VERSION,
            "algorithm": "RandomForestRegressor",
            "target_col": "supply_kwh",
            "feature_list": json.dumps(FEATURE_COLS, ensure_ascii=False),
            "feature_design": "季节(month/season)、日期类型(weekday/weekend)、时段(hour sin/cos)、历史负荷滞后(1/24/168h)、滚动负荷窗口(24/168h)和设备运行状态。",
            "window_design": "按设备时间排序构造小时级样本；滞后窗口为1小时、24小时、168小时，滚动窗口为过去24小时和过去168小时，避免使用当前小时目标值。",
            "model_reason": "随机森林回归能处理非线性时段模式和多特征交互，对中等规模小时数据训练稳定，不需要深度学习的大样本和长训练时间；最近24小时运行状态作为解释字段输出，在建议层提示当前停机与趋势需求的差异，不对预测值硬置零。",
            "training_method": "按系统和设备分别训练模型，按时间顺序前80%作为训练集，后20%作为测试集。",
            "evaluation_method": "在留出的后20%时间窗口上计算MAE、RMSE、R2、MAPE和残差标准差；置信区间使用测试集RMSE的1.28倍近似80%区间。",
            "train_start": train_range["train_start"].strftime("%Y-%m-%d %H:%M:%S"),
            "train_end": train_range["train_end"].strftime("%Y-%m-%d %H:%M:%S"),
            "test_start": metric_agg["test_start"].strftime("%Y-%m-%d %H:%M:%S"),
            "test_end": metric_agg["test_end"].strftime("%Y-%m-%d %H:%M:%S"),
            "train_row_count": int(train_count),
            "test_row_count": int(test_count),
            "mae": mae,
            "rmse": rmse,
            "r2": r2,
            "mape": mape,
            "residual_std": residual_std,
            "top_features": json.dumps(
                [{"feature": feature, "importance": importance} for feature, importance in top_features],
                ensure_ascii=False,
            ),
            "result_summary": result_summary,
        })

    features.unpersist()

    if predictions:
        forecast_df = predictions[0]
        for item in predictions[1:]:
            forecast_df = forecast_df.unionByName(item)
    else:
        forecast_df = spark.createDataFrame([], FORECAST_SCHEMA)

    if metrics_rows:
        metrics_df = spark.createDataFrame(metrics_rows, METRICS_SCHEMA).withColumn("dt", to_date(current_timestamp())).withColumn("created_at", current_timestamp())
    else:
        metrics_df = spark.createDataFrame([], METRICS_SCHEMA)

    return forecast_df, metrics_df


def build_future_features_for_equipment(hourly, features, station_id, system_type, equipment_id):
    eq_hist = hourly.filter(
        (col("station_id") == station_id)
        & (col("system_type") == system_type)
        & (col("equipment_id") == equipment_id)
    )
    last_seen = (
        eq_hist.groupBy("station_id", "system_type", "equipment_id")
        .agg(
            spark_max("stat_ts").alias("last_ts"),
            last("target_supply_kwh").alias("latest_supply_kwh"),
            avg(when(col("target_supply_kwh") > 0, col("energy_consumption_kwh") / col("target_supply_kwh"))).alias("energy_per_supply"),
            spark_sum(coalesce(col("cooling_supply_kwh"), lit(0.0))).alias("mix_cooling"),
            spark_sum(coalesce(col("heating_supply_kwh"), lit(0.0))).alias("mix_heating"),
            spark_sum(coalesce(col("electric_supply_kwh"), lit(0.0))).alias("mix_electric"),
            spark_sum("target_supply_kwh").alias("mix_total"),
        )
        .withColumn("energy_per_supply", coalesce(col("energy_per_supply"), lit(1.0)))
        .withColumn("cooling_share", when(col("mix_total") > 0, col("mix_cooling") / col("mix_total")).otherwise(lit(0.0)))
        .withColumn("heating_share", when(col("mix_total") > 0, col("mix_heating") / col("mix_total")).otherwise(lit(0.0)))
        .withColumn("electric_share", when(col("mix_total") > 0, col("mix_electric") / col("mix_total")).otherwise(lit(0.0)))
    )

    recent_status = (
        eq_hist.withColumn(
            "rn_desc",
            row_number().over(Window.partitionBy("station_id", "system_type", "equipment_id").orderBy(col("stat_ts").desc())),
        )
        .filter(col("rn_desc") <= 24)
        .groupBy("station_id", "system_type", "equipment_id")
        .agg(
            spark_sum(coalesce(col("target_supply_kwh"), lit(0.0))).alias("recent_24_supply_kwh"),
            avg(coalesce(col("operation_rate"), lit(0.0))).alias("recent_24_operation_rate"),
        )
    )

    future = (
        last_seen.withColumn("forecast_hour_offset", explode(sequence(lit(1), lit(FORECAST_HOURS))))
        .withColumn("target_ts", from_unixtime(unix_timestamp(col("last_ts")) + col("forecast_hour_offset") * 3600).cast("timestamp"))
        .join(recent_status, ["station_id", "system_type", "equipment_id"], "left")
    )
    future = add_time_features(future.withColumn("stat_ts", col("target_ts")))

    hist_target = eq_hist.select("station_id", "equipment_id", "stat_ts", "target_supply_kwh")
    latest_roll = (
        features.filter(
            (col("station_id") == station_id)
            & (col("system_type") == system_type)
            & (col("equipment_id") == equipment_id)
        )
        .withColumn("rn_desc", row_number().over(Window.partitionBy("station_id", "system_type", "equipment_id").orderBy(col("stat_ts").desc())))
        .filter(col("rn_desc") == 1)
        .select(
            "station_id",
            "system_type",
            "equipment_id",
            col("rolling_24h_avg").alias("latest_rolling_24h_avg"),
            col("rolling_24h_max").alias("latest_rolling_24h_max"),
            col("rolling_168h_avg").alias("latest_rolling_168h_avg"),
            col("rolling_168h_max").alias("latest_rolling_168h_max"),
        )
    )
    hourly_profile = (
        eq_hist.withColumn("hour_of_day", hour(col("stat_ts")).cast("double"))
        .groupBy("station_id", "system_type", "equipment_id", "hour_of_day")
        .agg(
            avg("avg_supply_temp").alias("profile_supply_temp"),
            avg("avg_return_temp").alias("profile_return_temp"),
            avg("avg_pressure").alias("profile_pressure"),
            avg("avg_flow").alias("profile_flow"),
            avg("avg_power").alias("profile_power"),
            avg("operation_rate").alias("profile_operation_rate"),
        )
    )

    lagged = (
        future.withColumn("lag_1_ts", from_unixtime(unix_timestamp(col("target_ts")) - 3600).cast("timestamp"))
        .withColumn("lag_24_ts", from_unixtime(unix_timestamp(col("target_ts")) - 24 * 3600).cast("timestamp"))
        .withColumn("lag_168_ts", from_unixtime(unix_timestamp(col("target_ts")) - 168 * 3600).cast("timestamp"))
        .join(
            hist_target.select("station_id", "equipment_id", col("stat_ts").alias("lag_1_ts"), col("target_supply_kwh").alias("lag_1h_actual")),
            ["station_id", "equipment_id", "lag_1_ts"],
            "left",
        )
        .join(
            hist_target.select("station_id", "equipment_id", col("stat_ts").alias("lag_24_ts"), col("target_supply_kwh").alias("lag_24h")),
            ["station_id", "equipment_id", "lag_24_ts"],
            "left",
        )
        .join(
            hist_target.select("station_id", "equipment_id", col("stat_ts").alias("lag_168_ts"), col("target_supply_kwh").alias("lag_168h")),
            ["station_id", "equipment_id", "lag_168_ts"],
            "left",
        )
        .join(latest_roll, ["station_id", "system_type", "equipment_id"], "left")
        .join(hourly_profile, ["station_id", "system_type", "equipment_id", "hour_of_day"], "left")
        .withColumn("lag_1h", coalesce(col("lag_1h_actual"), col("latest_supply_kwh"), lit(0.0)))
        .withColumn("lag_24h", coalesce(col("lag_24h"), col("latest_supply_kwh"), lit(0.0)))
        .withColumn("lag_168h", coalesce(col("lag_168h"), col("latest_supply_kwh"), lit(0.0)))
        .withColumn("rolling_24h_avg", coalesce(col("latest_rolling_24h_avg"), col("latest_supply_kwh"), lit(0.0)))
        .withColumn("rolling_24h_max", coalesce(col("latest_rolling_24h_max"), col("latest_supply_kwh"), lit(0.0)))
        .withColumn("rolling_168h_avg", coalesce(col("latest_rolling_168h_avg"), col("latest_supply_kwh"), lit(0.0)))
        .withColumn("rolling_168h_max", coalesce(col("latest_rolling_168h_max"), col("latest_supply_kwh"), lit(0.0)))
        .withColumn("avg_supply_temp", coalesce(col("profile_supply_temp"), lit(0.0)))
        .withColumn("avg_return_temp", coalesce(col("profile_return_temp"), lit(0.0)))
        .withColumn("avg_pressure", coalesce(col("profile_pressure"), lit(0.0)))
        .withColumn("avg_flow", coalesce(col("profile_flow"), lit(0.0)))
        .withColumn("avg_power", coalesce(col("profile_power"), lit(0.0)))
        .withColumn("operation_rate", coalesce(col("profile_operation_rate"), lit(0.0)))
        .withColumn(
            "temp_diff",
            when(
                col("system_type") == "chiller",
                col("avg_return_temp") - col("avg_supply_temp"),
            ).otherwise(col("avg_supply_temp") - col("avg_return_temp")),
        )
        .na.fill(0.0, subset=FEATURE_COLS)
    )
    return lagged


def build_revenue_forecast(forecast, prices):
    return (
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
            "algorithm",
            "dt",
            "created_at",
        )
    )


def main():
    spark = create_spark_session()
    try:
        print("=" * 80)
        print("System trend forecast training started")
        print("=" * 80)

        hourly = load_hourly(spark).cache()
        prices = read_price_map(spark)

        forecast, metrics = train_and_predict_systems(spark, hourly)

        print(f"Writing forecast table: {GOLD_SYSTEM_FORECAST}")
        forecast.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("system_type", "dt").save(GOLD_SYSTEM_FORECAST)

        print(f"Writing metrics table: {GOLD_SYSTEM_FORECAST_METRICS}")
        metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("system_type", "dt").save(GOLD_SYSTEM_FORECAST_METRICS)

        revenue = build_revenue_forecast(forecast, prices)
        print(f"Writing revenue forecast table: {GOLD_SYSTEM_REVENUE_FORECAST}")
        revenue.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("system_type", "dt").save(GOLD_SYSTEM_REVENUE_FORECAST)

        print("Forecast rows by system:")
        forecast.groupBy("system_type", "algorithm").agg(count("*").alias("rows"), spark_sum("predicted_supply_kwh").alias("predicted_supply_kwh")).show(50, truncate=False)
        print("Metrics preview:")
        metrics.select("equipment_id", "algorithm", "mae", "rmse", "r2", "mape", "result_summary").show(20, truncate=False)

        print("=" * 80)
        print("System trend forecast training completed")
        print("=" * 80)
    except Exception as exc:
        print(f"Failed to generate trend forecast: {exc}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
