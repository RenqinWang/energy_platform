#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
供能预测模型训练与推理脚本
使用XGBoost预测未来24小时的供冷量
"""

import sys
import argparse
import pickle
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date
import pandas as pd
import numpy as np
import xgboost as xgb
from forecast_utils import (
    create_time_features,
    create_lag_features,
    create_rolling_features,
    create_diff_features,
    prepare_training_data,
    split_train_test,
    evaluate_model,
    print_evaluation_report,
    calculate_confidence_interval
)


def create_spark_session():
    """创建Spark Session"""
    spark = (
        SparkSession.builder
        .appName("Generate_Forecast")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_historical_data(spark, input_path):
    """
    加载历史数据

    Args:
        spark: SparkSession
        input_path: 输入路径

    Returns:
        pandas DataFrame
    """
    print(f"加载历史数据: {input_path}")
    df_spark = spark.read.format("delta").load(input_path)

    # 选择需要的列
    columns = [
        'station_id', 'equipment_id', 'stat_hour',
        'avg_supply_temp', 'avg_return_temp', 'avg_pressure',
        'avg_flow', 'avg_power', 'energy_consumption_kwh',
        'cooling_capacity_kw', 'cooling_supply_kwh',
        'runtime_hours', 'operation_rate', 'dt'
    ]

    df_spark = df_spark.select(*columns)

    # 转换为pandas DataFrame
    df = df_spark.toPandas()

    print(f"加载完成: {len(df):,} 条记录")
    print(f"设备数量: {df['equipment_id'].nunique()}")
    print(f"时间范围: {df['stat_hour'].min()} 至 {df['stat_hour'].max()}")

    return df


def engineer_features(df):
    """
    特征工程

    Args:
        df: pandas DataFrame

    Returns:
        df: 添加了特征的DataFrame
    """
    print("\n开始特征工程...")

    # 按设备和时间排序
    df = df.sort_values(['equipment_id', 'stat_hour']).reset_index(drop=True)

    # 1. 时间特征
    print("  生成时间特征...")
    df = create_time_features(df)

    # 2. 滞后特征
    print("  生成滞后特征...")
    df = create_lag_features(df, target_col='cooling_supply_kwh', lags=[1, 24, 168])

    # 3. 滚动统计特征
    print("  生成滚动统计特征...")
    df = create_rolling_features(df, target_col='cooling_supply_kwh', windows=[24, 168])

    # 4. 差分特征
    print("  生成差分特征...")
    df = create_diff_features(df, target_col='cooling_supply_kwh')

    # 5. 温差特征
    df['temp_diff'] = df['avg_return_temp'] - df['avg_supply_temp']

    print(f"特征工程完成，总特征数: {len(df.columns)}")

    return df


def train_model_for_equipment(df_equipment, equipment_id):
    """
    为单个设备训练模型

    Args:
        df_equipment: 单个设备的DataFrame
        equipment_id: 设备ID

    Returns:
        model: 训练好的模型
        metrics: 评估指标
        feature_names: 特征名称列表
    """
    print(f"\n{'='*60}")
    print(f"训练设备 {equipment_id} 的模型")
    print(f"{'='*60}")

    # 分割训练集和测试集
    train_df, test_df = split_train_test(df_equipment, test_size=0.2, time_based=True)

    print(f"训练集: {len(train_df)} 条")
    print(f"测试集: {len(test_df)} 条")

    # 准备训练数据
    X_train, y_train, feature_names = prepare_training_data(
        train_df,
        target_col='cooling_supply_kwh',
        drop_na=True
    )

    X_test, y_test, _ = prepare_training_data(
        test_df,
        target_col='cooling_supply_kwh',
        feature_cols=feature_names,
        drop_na=True
    )

    print(f"特征数量: {len(feature_names)}")
    print(f"训练样本: {len(X_train)}")
    print(f"测试样本: {len(X_test)}")

    # 训练XGBoost模型
    print("\n训练XGBoost模型...")
    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        max_depth=6,
        learning_rate=0.1,
        n_estimators=100,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False
    )

    # 预测
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)

    # 评估
    train_metrics = evaluate_model(y_train.values, y_train_pred)
    test_metrics = evaluate_model(y_test.values, y_test_pred)

    print_evaluation_report(train_metrics, f"{equipment_id} - 训练集")
    print_evaluation_report(test_metrics, f"{equipment_id} - 测试集")

    # 特征重要性
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)

    print("Top 10 重要特征:")
    print(feature_importance.head(10).to_string(index=False))

    return model, test_metrics, feature_names


def train_models(df, model_dir):
    """
    为所有设备训练模型

    Args:
        df: pandas DataFrame
        model_dir: 模型保存目录

    Returns:
        models: 模型字典 {equipment_id: model}
        metrics: 评估指标字典
    """
    models = {}
    all_metrics = {}

    equipment_ids = df['equipment_id'].unique()

    for equipment_id in equipment_ids:
        df_equipment = df[df['equipment_id'] == equipment_id].copy()

        # 训练模型
        model, metrics, feature_names = train_model_for_equipment(df_equipment, equipment_id)

        # 保存模型
        model_path = os.path.join(model_dir, f"model_{equipment_id}.pkl")
        with open(model_path, 'wb') as f:
            pickle.dump({
                'model': model,
                'feature_names': feature_names,
                'equipment_id': equipment_id,
                'train_date': datetime.now().isoformat()
            }, f)

        print(f"模型已保存: {model_path}")

        models[equipment_id] = model
        all_metrics[equipment_id] = metrics

    return models, all_metrics


def predict_future(spark, df, models, model_dir, hours=24):
    """
    预测未来N小时

    Args:
        spark: SparkSession
        df: 历史数据DataFrame
        models: 模型字典
        model_dir: 模型目录
        hours: 预测小时数

    Returns:
        predictions_df: 预测结果DataFrame
    """
    print(f"\n{'='*60}")
    print(f"预测未来 {hours} 小时")
    print(f"{'='*60}")

    all_predictions = []

    for equipment_id, model in models.items():
        print(f"\n预测设备: {equipment_id}")

        # 获取该设备的最新数据
        df_equipment = df[df['equipment_id'] == equipment_id].copy()
        df_equipment = df_equipment.sort_values('stat_hour')

        # 获取最后一个时间点
        last_time = pd.to_datetime(df_equipment['stat_hour'].max())
        print(f"  最后时间点: {last_time}")

        # 加载特征名称
        model_path = os.path.join(model_dir, f"model_{equipment_id}.pkl")
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
            feature_names = model_data['feature_names']

        # 预测未来每个小时
        for h in range(1, hours + 1):
            target_time = last_time + timedelta(hours=h)

            # 创建预测样本
            pred_sample = df_equipment.iloc[[-1]].copy()
            pred_sample['stat_hour'] = target_time

            # 重新生成时间特征
            pred_sample = create_time_features(pred_sample)

            # 提取特征
            try:
                X_pred = pred_sample[feature_names]

                # 填充缺失值（pandas 3.0+使用ffill和bfill）
                X_pred = X_pred.ffill().bfill().fillna(0)

                # 处理inf值
                X_pred = X_pred.replace([np.inf, -np.inf], 0)

                # 预测
                y_pred = model.predict(X_pred)[0]

                # 确保预测值非负
                y_pred = max(0, y_pred)

                # 计算置信区间（简化版，使用固定的误差范围）
                # 实际应该基于历史误差计算
                confidence_margin = y_pred * 0.15  # 假设15%的误差范围
                confidence_lower = max(0, y_pred - confidence_margin)
                confidence_upper = y_pred + confidence_margin

                # 保存预测结果
                all_predictions.append({
                    'station_id': df_equipment['station_id'].iloc[-1],
                    'equipment_id': equipment_id,
                    'forecast_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'target_hour': target_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'predicted_cooling_kwh': float(y_pred),
                    'confidence_lower': float(confidence_lower),
                    'confidence_upper': float(confidence_upper),
                    'model_version': 'xgboost_v1.0',
                    'dt': target_time.strftime('%Y-%m-%d')
                })

            except Exception as e:
                print(f"  预测失败 (h={h}): {e}")
                continue

        print(f"  完成 {hours} 小时预测")

    # 转换为DataFrame
    predictions_df = pd.DataFrame(all_predictions)
    print(f"\n总预测记录数: {len(predictions_df)}")

    return predictions_df


def save_forecast(spark, predictions_df, output_path):
    """
    保存预测结果到Delta Lake

    Args:
        spark: SparkSession
        predictions_df: 预测结果DataFrame
        output_path: 输出路径
    """
    print(f"\n保存预测结果: {output_path}")

    # 转换为Spark DataFrame
    df_spark = spark.createDataFrame(predictions_df)

    # 添加创建时间
    df_spark = df_spark.withColumn("created_at", current_timestamp())

    # 写入Delta Lake
    df_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("dt") \
        .save(output_path)

    print("预测结果保存完成")

    # 验证
    df_verify = spark.read.format("delta").load(output_path)
    print(f"验证: {df_verify.count()} 条记录")


def main():
    parser = argparse.ArgumentParser(description='供能预测模型训练与推理')
    parser.add_argument('--mode', type=str, default='train',
                       choices=['train', 'predict', 'both'],
                       help='运行模式: train(训练), predict(预测), both(训练+预测)')
    parser.add_argument('--input', type=str,
                       default='hdfs://node1:9000/lake/gold/gold_supply_curve_hourly',
                       help='输入数据路径')
    parser.add_argument('--output', type=str,
                       default='hdfs://node1:9000/lake/gold/gold_forecast_supply',
                       help='输出数据路径')
    parser.add_argument('--model-dir', type=str,
                       default='/home/student/energy-platform/models/forecast',
                       help='模型保存目录')
    parser.add_argument('--hours', type=int, default=24,
                       help='预测小时数')

    args = parser.parse_args()

    # 创建模型目录
    os.makedirs(args.model_dir, exist_ok=True)

    # 创建Spark Session
    spark = create_spark_session()

    print("="*60)
    print("供能预测模型")
    print("="*60)
    print(f"模式: {args.mode}")
    print(f"输入: {args.input}")
    print(f"输出: {args.output}")
    print(f"模型目录: {args.model_dir}")
    print("="*60)

    try:
        # 加载历史数据
        df = load_historical_data(spark, args.input)

        # 特征工程
        df = engineer_features(df)

        if args.mode in ['train', 'both']:
            # 训练模型
            models, metrics = train_models(df, args.model_dir)

            # 打印总体评估
            print(f"\n{'='*60}")
            print("总体评估结果")
            print(f"{'='*60}")
            for equipment_id, metric in metrics.items():
                print(f"{equipment_id}: MAPE={metric['MAPE']:.2f}%, R²={metric['R2']:.4f}")

        if args.mode in ['predict', 'both']:
            # 加载模型
            if args.mode == 'predict':
                models = {}
                equipment_ids = df['equipment_id'].unique()
                for equipment_id in equipment_ids:
                    model_path = os.path.join(args.model_dir, f"model_{equipment_id}.pkl")
                    with open(model_path, 'rb') as f:
                        model_data = pickle.load(f)
                        models[equipment_id] = model_data['model']

            # 预测未来
            predictions_df = predict_future(spark, df, models, args.model_dir, args.hours)

            # 保存预测结果
            save_forecast(spark, predictions_df, args.output)

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
