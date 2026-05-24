#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
运行建议生成脚本
基于当前运行状态和预测结果，应用规则引擎生成运行建议
"""

import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, avg as spark_avg, sum as spark_sum, max as spark_max, count
from pyspark.sql.types import BooleanType, StringType, StructField, StructType
import pandas as pd
from advice_rules import RuleEngine


ADVICE_SCHEMA = StructType([
    StructField("rule_id", StringType(), True),
    StructField("rule_name", StringType(), True),
    StructField("advice_type", StringType(), True),
    StructField("risk_level", StringType(), True),
    StructField("advice_text", StringType(), True),
    StructField("evidence_metrics", StringType(), True),
    StructField("station_id", StringType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("advice_time", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("dt", StringType(), True),
])


def create_spark_session():
    """创建Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Generate_Advice")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_current_status(spark, status_path):
    """
    加载当前运行状态

    Args:
        spark: SparkSession
        status_path: 状态数据路径

    Returns:
        pandas DataFrame
    """
    print(f"加载当前运行状态: {status_path}")

    df_spark = spark.read.format("delta").load(status_path)

    # 获取最近6小时的数据
    df_spark = df_spark.orderBy(col("stat_hour").desc())

    # 转换为pandas
    df = df_spark.limit(1000).toPandas()

    print(f"加载完成: {len(df)} 条记录")

    return df


def _mean_value(df, primary_col, fallback_col=None, default=None):
    """Return a numeric mean from an existing pandas column."""
    col_name = primary_col if primary_col in df.columns else fallback_col
    if not col_name or col_name not in df.columns:
        return default

    values = pd.to_numeric(df[col_name], errors='coerce').dropna()
    if values.empty:
        return default
    return float(values.mean())


def _latest_value(df, col_name, default=None):
    """Return the latest non-null numeric value from a pandas column."""
    if col_name not in df.columns:
        return default

    values = pd.to_numeric(df[col_name], errors='coerce').dropna()
    if values.empty:
        return default
    return float(values.iloc[0])


def load_forecast(spark, forecast_path):
    """
    加载预测结果

    Args:
        spark: SparkSession
        forecast_path: 预测数据路径

    Returns:
        pandas DataFrame
    """
    print(f"加载预测结果: {forecast_path}")

    try:
        df_spark = spark.read.format("delta").load(forecast_path)

        # 获取最新的预测
        df_spark = df_spark.orderBy(col("forecast_time").desc())

        # 转换为pandas
        df = df_spark.limit(1000).toPandas()

        print(f"加载完成: {len(df)} 条记录")

        return df

    except Exception as e:
        print(f"⚠️  预测数据加载失败: {e}")
        print("将在没有预测数据的情况下生成建议")
        return pd.DataFrame()


def load_daily_report(spark, report_path):
    """
    加载日报表（用于获取COP等指标）

    Args:
        spark: SparkSession
        report_path: 日报表路径

    Returns:
        pandas DataFrame
    """
    print(f"加载日报表: {report_path}")

    df_spark = spark.read.format("delta").load(report_path)

    # 获取最近7天的数据
    df_spark = df_spark.orderBy(col("stat_date").desc()).limit(100)

    # 转换为pandas
    df = df_spark.toPandas()

    print(f"加载完成: {len(df)} 条记录")

    return df


def calculate_current_metrics(df_status, df_report):
    """
    计算当前运行指标

    Args:
        df_status: 状态数据
        df_report: 日报表数据

    Returns:
        metrics: 指标字典 {equipment_id: metrics}
    """
    print("\n计算当前运行指标...")

    metrics_by_equipment = {}

    for equipment_id in df_status['equipment_id'].unique():
        df_eq = df_status[df_status['equipment_id'] == equipment_id].copy()
        df_eq = df_eq.sort_values('stat_hour', ascending=False)

        # gold_supply_curve_hourly 是小时级表，最近6/24小时分别取6/24条。
        df_6h = df_eq.head(6)
        df_24h = df_eq.head(24)

        avg_cooling_6h = _mean_value(df_6h, 'cooling_supply_kwh', default=0) or 0
        latest_operation_rate = _latest_value(df_eq, 'operation_rate', default=0) or 0
        avg_operation_rate = _mean_value(df_6h, 'operation_rate', default=0) or 0
        avg_supply_temp = _mean_value(df_6h, 'avg_supply_temp', 'supply_temp')
        avg_return_temp = _mean_value(df_6h, 'avg_return_temp', 'return_temp')

        if 'start_count' in df_24h.columns:
            start_counts = pd.to_numeric(df_24h['start_count'], errors='coerce').dropna()
            start_count_24h = float(start_counts.max() - start_counts.min()) if len(start_counts) > 1 else 0
        else:
            start_count_24h = 0

        if 'operation_rate' in df_24h.columns:
            operation_rates = pd.to_numeric(df_24h['operation_rate'], errors='coerce').fillna(0)
            high_load_hours = int((operation_rates >= 95).sum())
        else:
            high_load_hours = 0

        # 计算指标
        metrics = {
            'equipment_id': equipment_id,
            'station_id': df_eq['station_id'].iloc[0] if len(df_eq) > 0 else None,

            # 最近6小时平均供冷量
            'avg_cooling_6h': avg_cooling_6h,

            # 小时表没有run_flag，用最新小时运行率推导当前运行状态
            'run_flag': 1 if latest_operation_rate > 0 else 0,

            # 温度
            'avg_supply_temp': avg_supply_temp,
            'avg_return_temp': avg_return_temp,

            # 运行率
            'operation_rate': avg_operation_rate,

            # 24小时启动次数。start_count是累计值时，用窗口内最大最小差值估算。
            'start_count_24h': start_count_24h,

            # 最近24小时内运行率>=95%的小时数
            'high_load_hours': high_load_hours
        }

        # 从日报表获取COP
        if not df_report.empty:
            df_eq_report = df_report[df_report['equipment_id'] == equipment_id]
            if len(df_eq_report) > 0:
                metrics['avg_cop'] = df_eq_report['avg_cop'].iloc[0]

        metrics_by_equipment[equipment_id] = metrics

    # 计算运行机组数
    running_units = sum(1 for m in metrics_by_equipment.values() if m['run_flag'] > 0)

    # 添加运行机组数到每个设备的指标中
    for equipment_id in metrics_by_equipment:
        metrics_by_equipment[equipment_id]['running_units'] = running_units

    print(f"完成 {len(metrics_by_equipment)} 个设备的指标计算")

    return metrics_by_equipment


def calculate_forecast_metrics(df_forecast):
    """
    计算预测指标

    Args:
        df_forecast: 预测数据

    Returns:
        metrics: 指标字典 {equipment_id: metrics}
    """
    print("\n计算预测指标...")

    if df_forecast.empty:
        print("无预测数据")
        return {}

    metrics_by_equipment = {}

    for equipment_id in df_forecast['equipment_id'].unique():
        df_eq = df_forecast[df_forecast['equipment_id'] == equipment_id].copy()

        # 按目标时间排序
        df_eq['target_hour'] = pd.to_datetime(df_eq['target_hour'])
        df_eq = df_eq.sort_values('target_hour')

        # 未来6小时平均预测供冷量
        df_6h = df_eq.head(6)

        metrics = {
            'equipment_id': equipment_id,
            'avg_cooling_6h': df_6h['predicted_cooling_kwh'].mean() if len(df_6h) > 0 else 0
        }

        metrics_by_equipment[equipment_id] = metrics

    print(f"完成 {len(metrics_by_equipment)} 个设备的预测指标计算")

    return metrics_by_equipment


def generate_advices(current_metrics, forecast_metrics, engine):
    """
    生成建议

    Args:
        current_metrics: 当前指标字典
        forecast_metrics: 预测指标字典
        engine: 规则引擎

    Returns:
        advices: 建议列表
    """
    print("\n应用规则引擎生成建议...")

    all_advices = []

    for equipment_id, current in current_metrics.items():
        # 获取预测指标
        forecast = forecast_metrics.get(equipment_id, {})

        # 构建数据字典
        data = {
            'current': current,
            'forecast': forecast
        }

        # 应用规则
        advices = engine.apply_rules(data)

        # 添加设备信息和时间戳
        for advice in advices:
            advice['station_id'] = current['station_id']
            advice['equipment_id'] = equipment_id
            advice['advice_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            advice['is_active'] = True
            advice['dt'] = datetime.now().strftime('%Y-%m-%d')

        all_advices.extend(advices)

    print(f"生成 {len(all_advices)} 条建议")

    return all_advices


def save_advices(spark, advices, output_path):
    """
    保存建议到Delta Lake

    Args:
        spark: SparkSession
        advices: 建议列表
        output_path: 输出路径
    """
    print(f"\n保存建议: {output_path}")

    rows = [
        {field.name: advice.get(field.name) for field in ADVICE_SCHEMA.fields}
        for advice in advices
    ]

    # 无建议时也写入0行Delta表，保持Gold层路径和API查询契约稳定。
    if not rows:
        print("当前无建议，写入空Delta表")

    df_spark = spark.createDataFrame(rows, ADVICE_SCHEMA)
    df_spark = df_spark.withColumn("created_at", current_timestamp())

    # 写入Delta Lake
    df_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("dt") \
        .save(output_path)

    print("建议保存完成")

    # 验证
    df_verify = spark.read.format("delta").load(output_path)
    print(f"验证: {df_verify.count()} 条记录")


def print_advice_summary(advices):
    """
    打印建议摘要

    Args:
        advices: 建议列表
    """
    if not advices:
        print("\n✅ 当前无建议")
        return

    print(f"\n{'='*60}")
    print(f"建议摘要 (共 {len(advices)} 条)")
    print(f"{'='*60}")

    # 按风险等级分组
    by_risk = {}
    for advice in advices:
        risk = advice['risk_level']
        if risk not in by_risk:
            by_risk[risk] = []
        by_risk[risk].append(advice)

    # 按风险等级排序显示
    for risk in ['high', 'medium', 'low']:
        if risk in by_risk:
            print(f"\n{risk.upper()} 风险 ({len(by_risk[risk])} 条):")
            for i, advice in enumerate(by_risk[risk], 1):
                print(f"  {i}. [{advice['equipment_id']}] {advice['advice_text']}")

    print(f"\n{'='*60}")


def main():
    # 路径配置
    status_path = "hdfs://node1:9000/lake/gold/gold_supply_curve_hourly"
    forecast_path = "hdfs://node1:9000/lake/gold/gold_forecast_supply"
    report_path = "hdfs://node1:9000/lake/gold/gold_report_daily"
    output_path = "hdfs://node1:9000/lake/gold/gold_operation_advice"

    # 创建Spark Session
    spark = create_spark_session()

    print("="*60)
    print("运行建议生成")
    print("="*60)
    print(f"状态数据: {status_path}")
    print(f"预测数据: {forecast_path}")
    print(f"日报表: {report_path}")
    print(f"输出路径: {output_path}")
    print("="*60)

    try:
        # 初始化规则引擎
        engine = RuleEngine()
        print(f"\n规则引擎已加载 {engine.get_rule_count()} 条规则")

        # 加载数据
        df_status = load_current_status(spark, status_path)
        df_forecast = load_forecast(spark, forecast_path)
        df_report = load_daily_report(spark, report_path)

        # 计算指标
        current_metrics = calculate_current_metrics(df_status, df_report)
        forecast_metrics = calculate_forecast_metrics(df_forecast)

        # 生成建议
        advices = generate_advices(current_metrics, forecast_metrics, engine)

        # 打印摘要
        print_advice_summary(advices)

        # 保存建议
        save_advices(spark, advices, output_path)

        print(f"\n{'='*60}")
        print("✅ 任务完成")
        print(f"{'='*60}")

    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
