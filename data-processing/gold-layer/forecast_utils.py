#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
特征工程工具模块
用于供能预测的特征生成和数据准备
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def create_time_features(df):
    """
    生成时间特征

    Args:
        df: pandas DataFrame，必须包含 'stat_hour' 列

    Returns:
        df: 添加了时间特征的DataFrame
    """
    # 确保 stat_hour 是 datetime 类型
    df['stat_hour'] = pd.to_datetime(df['stat_hour'])

    # 提取时间特征
    df['hour'] = df['stat_hour'].dt.hour
    df['day_of_week'] = df['stat_hour'].dt.dayofweek
    df['month'] = df['stat_hour'].dt.month
    df['day_of_month'] = df['stat_hour'].dt.day
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)

    # 周期性编码（使用sin/cos变换保持周期性）
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['day_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['day_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)

    return df


def create_lag_features(df, target_col='cooling_supply_kwh', lags=[1, 24, 168]):
    """
    生成滞后特征

    Args:
        df: pandas DataFrame，按时间排序
        target_col: 目标列名
        lags: 滞后期列表（小时数）

    Returns:
        df: 添加了滞后特征的DataFrame
    """
    for lag in lags:
        df[f'{target_col}_lag_{lag}h'] = df.groupby('equipment_id')[target_col].shift(lag)

    return df


def create_rolling_features(df, target_col='cooling_supply_kwh', windows=[24, 168]):
    """
    生成滚动统计特征

    Args:
        df: pandas DataFrame，按时间排序
        target_col: 目标列名
        windows: 滚动窗口大小列表（小时数）

    Returns:
        df: 添加了滚动统计特征的DataFrame
    """
    for window in windows:
        # 滚动均值
        df[f'{target_col}_rolling_mean_{window}h'] = (
            df.groupby('equipment_id')[target_col]
            .transform(lambda x: x.rolling(window=window, min_periods=1).mean())
        )

        # 滚动最大值
        df[f'{target_col}_rolling_max_{window}h'] = (
            df.groupby('equipment_id')[target_col]
            .transform(lambda x: x.rolling(window=window, min_periods=1).max())
        )

        # 滚动最小值
        df[f'{target_col}_rolling_min_{window}h'] = (
            df.groupby('equipment_id')[target_col]
            .transform(lambda x: x.rolling(window=window, min_periods=1).min())
        )

        # 滚动标准差
        df[f'{target_col}_rolling_std_{window}h'] = (
            df.groupby('equipment_id')[target_col]
            .transform(lambda x: x.rolling(window=window, min_periods=2).std())
        )

    return df


def create_diff_features(df, target_col='cooling_supply_kwh'):
    """
    生成差分特征（变化率）

    Args:
        df: pandas DataFrame，按时间排序
        target_col: 目标列名

    Returns:
        df: 添加了差分特征的DataFrame
    """
    # 1小时变化量
    df[f'{target_col}_diff_1h'] = df.groupby('equipment_id')[target_col].diff(1)

    # 24小时变化量（同比）
    df[f'{target_col}_diff_24h'] = df.groupby('equipment_id')[target_col].diff(24)

    # 1小时变化率
    df[f'{target_col}_pct_change_1h'] = df.groupby('equipment_id')[target_col].pct_change(1)

    return df


def prepare_training_data(df, target_col='cooling_supply_kwh',
                         feature_cols=None, drop_na=True):
    """
    准备训练数据

    Args:
        df: pandas DataFrame
        target_col: 目标列名
        feature_cols: 特征列名列表（None则自动选择）
        drop_na: 是否删除包含NaN的行

    Returns:
        X: 特征矩阵
        y: 目标向量
        feature_names: 特征名称列表
    """
    if feature_cols is None:
        # 自动选择特征列（排除目标列和标识列）
        exclude_cols = [
            target_col, 'station_id', 'equipment_id', 'stat_hour',
            'dt', 'created_at', 'updated_at', 'record_count'
        ]
        feature_cols = [col for col in df.columns if col not in exclude_cols]

    # 提取特征和目标
    X = df[feature_cols].copy()
    y = df[target_col].copy()

    # 处理缺失值
    if drop_na:
        # 删除目标列为NaN的行
        valid_idx = ~y.isna()
        X = X[valid_idx]
        y = y[valid_idx]

        # 对特征列的NaN进行填充（pandas 3.0+使用ffill和bfill）
        X = X.ffill().bfill().fillna(0)

        # 处理inf值
        X = X.replace([np.inf, -np.inf], 0)

    return X, y, feature_cols


def split_train_test(df, test_size=0.2, time_based=True):
    """
    分割训练集和测试集

    Args:
        df: pandas DataFrame
        test_size: 测试集比例
        time_based: 是否基于时间分割（True则最后test_size的数据作为测试集）

    Returns:
        train_df: 训练集
        test_df: 测试集
    """
    if time_based:
        # 基于时间分割
        df = df.sort_values('stat_hour')
        split_idx = int(len(df) * (1 - test_size))
        train_df = df.iloc[:split_idx].copy()
        test_df = df.iloc[split_idx:].copy()
    else:
        # 随机分割
        from sklearn.model_selection import train_test_split
        train_df, test_df = train_test_split(df, test_size=test_size, random_state=42)

    return train_df, test_df


def calculate_confidence_interval(y_true, y_pred, confidence=0.8):
    """
    计算预测置信区间

    Args:
        y_true: 真实值
        y_pred: 预测值
        confidence: 置信度（0-1）

    Returns:
        lower: 置信区间下界
        upper: 置信区间上界
    """
    # 计算残差
    residuals = y_true - y_pred

    # 计算残差的标准差
    std = np.std(residuals)

    # 计算置信区间（基于正态分布假设）
    from scipy import stats
    z_score = stats.norm.ppf((1 + confidence) / 2)
    margin = z_score * std

    lower = y_pred - margin
    upper = y_pred + margin

    return lower, upper


def evaluate_model(y_true, y_pred):
    """
    评估模型性能

    Args:
        y_true: 真实值
        y_pred: 预测值

    Returns:
        metrics: 评估指标字典
    """
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    # 过滤掉NaN值
    mask = ~(np.isnan(y_true) | np.isnan(y_pred))
    y_true = y_true[mask]
    y_pred = y_pred[mask]

    # 计算评估指标
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)

    # 计算MAPE（平均绝对百分比误差）
    # 避免除以0
    mask_nonzero = y_true != 0
    if mask_nonzero.sum() > 0:
        mape = np.mean(np.abs((y_true[mask_nonzero] - y_pred[mask_nonzero]) / y_true[mask_nonzero])) * 100
    else:
        mape = np.nan

    metrics = {
        'MAE': mae,
        'RMSE': rmse,
        'R2': r2,
        'MAPE': mape
    }

    return metrics


def print_evaluation_report(metrics, model_name='Model'):
    """
    打印评估报告

    Args:
        metrics: 评估指标字典
        model_name: 模型名称
    """
    print(f"\n{'='*60}")
    print(f"{model_name} 评估报告")
    print(f"{'='*60}")
    print(f"MAE (平均绝对误差):        {metrics['MAE']:.2f} kWh")
    print(f"RMSE (均方根误差):         {metrics['RMSE']:.2f} kWh")
    print(f"R² (决定系数):             {metrics['R2']:.4f}")
    print(f"MAPE (平均绝对百分比误差): {metrics['MAPE']:.2f}%")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # 测试代码
    print("特征工程工具模块加载成功")
    print(f"可用函数:")
    print(f"  - create_time_features()")
    print(f"  - create_lag_features()")
    print(f"  - create_rolling_features()")
    print(f"  - create_diff_features()")
    print(f"  - prepare_training_data()")
    print(f"  - split_train_test()")
    print(f"  - calculate_confidence_interval()")
    print(f"  - evaluate_model()")
    print(f"  - print_evaluation_report()")
