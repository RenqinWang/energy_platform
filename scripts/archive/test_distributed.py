#!/usr/bin/env python3
"""
Distributed Environment Test Script
Tests all stages in distributed mode to ensure production readiness
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, min as spark_min
from datetime import datetime
import sys

# HDFS Configuration
HDFS_NAMENODE = "hdfs://node1:9000"
HDFS_LAKE_PATH = f"{HDFS_NAMENODE}/lake"

# Spark Master (distributed mode)
SPARK_MASTER = "spark://node2:7077"

def print_section(title):
    """Print section header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)

def create_spark_session():
    """Create Spark session in distributed mode"""
    print_section("Initializing Spark Session (Distributed Mode)")

    spark = (
        SparkSession.builder
        .appName("DistributedEnvironmentTest")
        .master(SPARK_MASTER)  # Use distributed cluster
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"✓ Spark session created")
    print(f"  Master: {SPARK_MASTER}")
    print(f"  Application ID: {spark.sparkContext.applicationId}")
    print(f"  Executors: {len(spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()) - 1}")

    return spark

def test_bronze_layer(spark):
    """Test Bronze layer tables"""
    print_section("Testing Bronze Layer (Distributed)")

    results = {}

    # Test bronze_sensor_raw
    try:
        df = spark.read.format("delta").load(f"{HDFS_LAKE_PATH}/bronze/bronze_sensor_raw")
        count_val = df.count()
        partitions = df.rdd.getNumPartitions()

        print(f"✓ bronze_sensor_raw")
        print(f"  Records: {count_val:,}")
        print(f"  Partitions: {partitions}")
        print(f"  Schema: {len(df.columns)} columns")

        results['bronze_sensor_raw'] = {
            'status': 'PASS',
            'records': count_val,
            'partitions': partitions
        }
    except Exception as e:
        print(f"✗ bronze_sensor_raw: {str(e)}")
        results['bronze_sensor_raw'] = {'status': 'FAIL', 'error': str(e)}

    # Test bronze_price_raw
    try:
        df = spark.read.format("delta").load(f"{HDFS_LAKE_PATH}/bronze/bronze_price_raw")
        count_val = df.count()
        partitions = df.rdd.getNumPartitions()

        print(f"✓ bronze_price_raw")
        print(f"  Records: {count_val:,}")
        print(f"  Partitions: {partitions}")

        results['bronze_price_raw'] = {
            'status': 'PASS',
            'records': count_val,
            'partitions': partitions
        }
    except Exception as e:
        print(f"✗ bronze_price_raw: {str(e)}")
        results['bronze_price_raw'] = {'status': 'FAIL', 'error': str(e)}

    return results

def test_silver_layer(spark):
    """Test Silver layer tables"""
    print_section("Testing Silver Layer (Distributed)")

    results = {}

    # Test silver_point_meta_dim
    try:
        df = spark.read.format("delta").load(f"{HDFS_LAKE_PATH}/silver/silver_point_meta_dim")
        count_val = df.count()
        partitions = df.rdd.getNumPartitions()

        print(f"✓ silver_point_meta_dim")
        print(f"  Records: {count_val:,}")
        print(f"  Partitions: {partitions}")

        results['silver_point_meta_dim'] = {
            'status': 'PASS',
            'records': count_val,
            'partitions': partitions
        }
    except Exception as e:
        print(f"✗ silver_point_meta_dim: {str(e)}")
        results['silver_point_meta_dim'] = {'status': 'FAIL', 'error': str(e)}

    # Test silver_point_fact
    try:
        df = spark.read.format("delta").load(f"{HDFS_LAKE_PATH}/silver/silver_point_fact")
        count_val = df.count()
        partitions = df.rdd.getNumPartitions()

        # Test distributed aggregation
        agg_result = df.groupBy("station_id").agg(
            count("*").alias("record_count"),
            avg("value").alias("avg_value")
        ).collect()

        print(f"✓ silver_point_fact")
        print(f"  Records: {count_val:,}")
        print(f"  Partitions: {partitions}")
        print(f"  Stations: {len(agg_result)}")

        results['silver_point_fact'] = {
            'status': 'PASS',
            'records': count_val,
            'partitions': partitions
        }
    except Exception as e:
        print(f"✗ silver_point_fact: {str(e)}")
        results['silver_point_fact'] = {'status': 'FAIL', 'error': str(e)}

    # Test silver_chiller_status
    try:
        df = spark.read.format("delta").load(f"{HDFS_LAKE_PATH}/silver/silver_chiller_status")
        count_val = df.count()
        partitions = df.rdd.getNumPartitions()

        print(f"✓ silver_chiller_status")
        print(f"  Records: {count_val:,}")
        print(f"  Partitions: {partitions}")

        results['silver_chiller_status'] = {
            'status': 'PASS',
            'records': count_val,
            'partitions': partitions
        }
    except Exception as e:
        print(f"✗ silver_chiller_status: {str(e)}")
        results['silver_chiller_status'] = {'status': 'FAIL', 'error': str(e)}

    # Test silver_price_dim
    try:
        df = spark.read.format("delta").load(f"{HDFS_LAKE_PATH}/silver/silver_price_dim")
        count_val = df.count()
        partitions = df.rdd.getNumPartitions()

        print(f"✓ silver_price_dim")
        print(f"  Records: {count_val:,}")
        print(f"  Partitions: {partitions}")

        results['silver_price_dim'] = {
            'status': 'PASS',
            'records': count_val,
            'partitions': partitions
        }
    except Exception as e:
        print(f"✗ silver_price_dim: {str(e)}")
        results['silver_price_dim'] = {'status': 'FAIL', 'error': str(e)}

    return results

def test_gold_layer(spark):
    """Test Gold layer tables"""
    print_section("Testing Gold Layer (Distributed)")

    results = {}

    # Test gold_supply_curve_hourly
    try:
        df = spark.read.format("delta").load(f"{HDFS_LAKE_PATH}/gold/gold_supply_curve_hourly")
        count_val = df.count()
        partitions = df.rdd.getNumPartitions()

        # Test distributed aggregation
        agg_result = df.agg(
            spark_sum("energy_consumption_kwh").alias("total_energy"),
            spark_sum("cooling_supply_kwh").alias("total_cooling"),
            avg("operation_rate").alias("avg_operation_rate")
        ).collect()[0]

        print(f"✓ gold_supply_curve_hourly")
        print(f"  Records: {count_val:,}")
        print(f"  Partitions: {partitions}")

        # Handle NULL values
        total_energy = agg_result['total_energy'] if agg_result['total_energy'] is not None else 0
        total_cooling = agg_result['total_cooling'] if agg_result['total_cooling'] is not None else 0
        avg_op_rate = agg_result['avg_operation_rate'] if agg_result['avg_operation_rate'] is not None else 0

        print(f"  Total Energy: {total_energy:,.2f} kWh")
        print(f"  Total Cooling: {total_cooling:,.2f} kWh")
        print(f"  Avg Operation Rate: {avg_op_rate:.2f}%")

        results['gold_supply_curve_hourly'] = {
            'status': 'PASS',
            'records': count_val,
            'partitions': partitions
        }
    except Exception as e:
        print(f"✗ gold_supply_curve_hourly: {str(e)}")
        results['gold_supply_curve_hourly'] = {'status': 'FAIL', 'error': str(e)}

    # Test gold_report_daily
    try:
        df = spark.read.format("delta").load(f"{HDFS_LAKE_PATH}/gold/gold_report_daily")
        count_val = df.count()
        partitions = df.rdd.getNumPartitions()

        # Test distributed aggregation
        agg_result = df.agg(
            spark_sum("total_energy_consumption_kwh").alias("total_energy"),
            spark_sum("total_cooling_supply_kwh").alias("total_cooling"),
            spark_sum("net_profit").alias("total_profit"),
            avg("avg_cop").alias("avg_cop")
        ).collect()[0]

        print(f"✓ gold_report_daily")
        print(f"  Records: {count_val:,}")
        print(f"  Partitions: {partitions}")

        # Handle NULL values
        total_energy = agg_result['total_energy'] if agg_result['total_energy'] is not None else 0
        total_cooling = agg_result['total_cooling'] if agg_result['total_cooling'] is not None else 0
        total_profit = agg_result['total_profit'] if agg_result['total_profit'] is not None else 0
        avg_cop = agg_result['avg_cop'] if agg_result['avg_cop'] is not None else 0

        print(f"  Total Energy: {total_energy:,.2f} kWh")
        print(f"  Total Cooling: {total_cooling:,.2f} kWh")
        print(f"  Total Profit: ¥{total_profit:,.2f}")
        print(f"  Avg COP: {avg_cop:.2f}")

        results['gold_report_daily'] = {
            'status': 'PASS',
            'records': count_val,
            'partitions': partitions
        }
    except Exception as e:
        print(f"✗ gold_report_daily: {str(e)}")
        results['gold_report_daily'] = {'status': 'FAIL', 'error': str(e)}

    return results

def test_distributed_query_performance(spark):
    """Test distributed query performance"""
    print_section("Testing Distributed Query Performance")

    try:
        # Test complex join query
        start_time = datetime.now()

        df_hourly = spark.read.format("delta").load(f"{HDFS_LAKE_PATH}/gold/gold_supply_curve_hourly")
        df_daily = spark.read.format("delta").load(f"{HDFS_LAKE_PATH}/gold/gold_report_daily")

        # Perform distributed join and aggregation
        result = df_hourly.join(
            df_daily,
            (df_hourly.station_id == df_daily.station_id) &
            (df_hourly.equipment_id == df_daily.equipment_id),
            "inner"
        ).groupBy(df_hourly.station_id, df_hourly.equipment_id).agg(
            count("*").alias("record_count"),
            spark_sum(df_hourly.energy_consumption_kwh).alias("total_energy")
        ).collect()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"✓ Distributed join and aggregation")
        print(f"  Duration: {duration:.2f} seconds")
        print(f"  Result rows: {len(result)}")
        print(f"  Status: PASS")

        return {'status': 'PASS', 'duration': duration}
    except Exception as e:
        print(f"✗ Distributed query failed: {str(e)}")
        return {'status': 'FAIL', 'error': str(e)}

def print_summary(bronze_results, silver_results, gold_results, perf_result):
    """Print test summary"""
    print_section("Test Summary")

    all_results = {
        'Bronze Layer': bronze_results,
        'Silver Layer': silver_results,
        'Gold Layer': gold_results,
        'Performance': {'distributed_query': perf_result}
    }

    total_tests = 0
    passed_tests = 0

    for layer, results in all_results.items():
        print(f"\n{layer}:")
        for table, result in results.items():
            total_tests += 1
            status = result.get('status', 'UNKNOWN')
            if status == 'PASS':
                passed_tests += 1
                print(f"  ✓ {table}: PASS")
            else:
                print(f"  ✗ {table}: FAIL")
                if 'error' in result:
                    print(f"    Error: {result['error']}")

    print(f"\n{'='*80}")
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {total_tests - passed_tests}")
    print(f"Success Rate: {passed_tests/total_tests*100:.1f}%")
    print(f"{'='*80}")

    if passed_tests == total_tests:
        print("\n✓ All tests passed! System is production-ready.")
        return 0
    else:
        print(f"\n✗ {total_tests - passed_tests} test(s) failed. Please review errors above.")
        return 1

def main():
    """Main test function"""
    print("="*80)
    print("  Distributed Environment Test Suite")
    print("  Testing all stages in distributed mode")
    print("="*80)
    print(f"  Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Spark Master: {SPARK_MASTER}")
    print(f"  HDFS NameNode: {HDFS_NAMENODE}")

    try:
        # Create Spark session
        spark = create_spark_session()

        # Test each layer
        bronze_results = test_bronze_layer(spark)
        silver_results = test_silver_layer(spark)
        gold_results = test_gold_layer(spark)
        perf_result = test_distributed_query_performance(spark)

        # Print summary
        exit_code = print_summary(bronze_results, silver_results, gold_results, perf_result)

        # Stop Spark session
        spark.stop()

        print(f"\n  End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        return exit_code

    except Exception as e:
        print(f"\n✗ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
