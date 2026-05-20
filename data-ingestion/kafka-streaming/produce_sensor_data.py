#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka 数据生产者
读取传感器数据文件，发送到 Kafka topic
"""

import os
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

def create_producer():
    """创建 Kafka Producer"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )
    return producer

def read_sensor_file(file_path, sensor_id, label):
    """读取传感器数据文件"""
    data_points = []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) == 2:
                timestamp_str, value_str = parts
                try:
                    value = float(value_str)
                    data_points.append({
                        'timestamp': timestamp_str,
                        'value': value
                    })
                except ValueError:
                    continue

    return data_points

def send_to_kafka(producer, sensor_id, label, data_points, batch_size=100, delay=0.1):
    """发送数据到 Kafka"""
    topic = f"sensor_{sensor_id}"
    sent_count = 0

    for i, point in enumerate(data_points):
        message = {
            'sensor_id': sensor_id,
            'label': label,
            'timestamp': point['timestamp'],
            'value': point['value'],
            'is_simulated': True,
            'push_time': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        }

        try:
            future = producer.send(topic, value=message)
            future.get(timeout=10)
            sent_count += 1

            if (i + 1) % batch_size == 0:
                print(f"  [{sensor_id}] 已发送 {sent_count} 条消息")
                time.sleep(delay)

        except KafkaError as e:
            print(f"  ❌ 发送失败: {e}")

    producer.flush()
    return sent_count

def main():
    print("=" * 60)
    print("Kafka 数据生产者启动")
    print("=" * 60)

    # 创建 Producer
    producer = create_producer()
    print("✅ Kafka Producer 已创建")

    # 数据目录
    data_dir = "/home/student/作业2数据/positions"

    # 读取点位字典（从之前生成的 Delta 表中读取）
    # 这里简化处理，直接从文件名推断
    sensor_files = [f for f in os.listdir(data_dir) if f.endswith('.txt')]

    print(f"📂 找到 {len(sensor_files)} 个传感器数据文件")
    print(f"⏳ 开始发送数据（仅发送前10个文件的前1000条数据作为测试）...\n")

    total_sent = 0

    # 为了测试，只发送前10个文件的部分数据
    for i, filename in enumerate(sensor_files[:10]):
        sensor_id = filename.replace('.txt', '')
        file_path = os.path.join(data_dir, filename)

        print(f"[{i+1}/10] 处理传感器: {sensor_id}")

        # 读取数据（只取前1000条）
        data_points = read_sensor_file(file_path, sensor_id, "")
        data_points = data_points[:1000]  # 限制数量

        if not data_points:
            print(f"  ⚠️  无有效数据，跳过")
            continue

        # 发送到 Kafka
        sent = send_to_kafka(producer, sensor_id, "", data_points, batch_size=100, delay=0.05)
        total_sent += sent
        print(f"  ✅ 完成，共发送 {sent} 条消息\n")

    producer.close()

    print("=" * 60)
    print(f"✅ 数据发送完成！总计发送 {total_sent} 条消息")
    print("=" * 60)

if __name__ == "__main__":
    main()
