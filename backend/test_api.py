#!/usr/bin/env python3
"""
Test script for Energy Platform API
Validates all API endpoints and data access functionality
"""
import requests
import json
from datetime import datetime

# API base URL
BASE_URL = "http://localhost:8000"

def print_section(title):
    """Print section header"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)

def test_health_check():
    """Test health check endpoint"""
    print_section("Testing Health Check")

    response = requests.get(f"{BASE_URL}/health")
    print(f"Status Code: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        print(f"✓ Health check passed")
        print(f"  Status: {data['status']}")
        print(f"  Version: {data['version']}")
        print(f"  Timestamp: {data['timestamp']}")
        return True
    else:
        print(f"✗ Health check failed")
        return False

def test_get_stations():
    """Test get stations endpoint"""
    print_section("Testing Get Stations")

    response = requests.get(f"{BASE_URL}/api/stations")
    print(f"Status Code: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        stations = data['stations']
        print(f"✓ Found {len(stations)} stations")
        print(f"  Stations: {stations}")
        return stations
    else:
        print(f"✗ Failed to get stations")
        return []

def test_get_equipment(station_id=None):
    """Test get equipment endpoint"""
    print_section(f"Testing Get Equipment (station_id={station_id})")

    params = {}
    if station_id:
        params['station_id'] = station_id

    response = requests.get(f"{BASE_URL}/api/equipment", params=params)
    print(f"Status Code: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        equipment = data['equipment']
        print(f"✓ Found {len(equipment)} equipment")
        print(f"  Equipment: {equipment}")
        return equipment
    else:
        print(f"✗ Failed to get equipment")
        return []

def test_supply_curve(station_id=None, equipment_id=None, limit=5):
    """Test supply curve endpoint"""
    print_section(f"Testing Supply Curve (limit={limit})")

    params = {'limit': limit}
    if station_id:
        params['station_id'] = station_id
    if equipment_id:
        params['equipment_id'] = equipment_id

    response = requests.get(f"{BASE_URL}/api/supply-curve", params=params)
    print(f"Status Code: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        print(f"✓ Retrieved {len(data)} records")

        if len(data) > 0:
            print(f"\n  Sample record:")
            sample = data[0]
            print(f"    Station: {sample['station_id']}")
            print(f"    Equipment: {sample['equipment_id']}")
            print(f"    Time: {sample['stat_hour']}")
            print(f"    Avg Power: {sample['avg_power']} kW")
            print(f"    Energy Consumption: {sample['energy_consumption_kwh']} kWh")
            print(f"    Cooling Supply: {sample['cooling_supply_kwh']} kWh")
            print(f"    Operation Rate: {sample['operation_rate']}%")

        return True
    else:
        print(f"✗ Failed to get supply curve data")
        print(f"  Error: {response.text}")
        return False

def test_daily_report(station_id=None, equipment_id=None, limit=5):
    """Test daily report endpoint"""
    print_section(f"Testing Daily Report (limit={limit})")

    params = {'limit': limit}
    if station_id:
        params['station_id'] = station_id
    if equipment_id:
        params['equipment_id'] = equipment_id

    response = requests.get(f"{BASE_URL}/api/daily-report", params=params)
    print(f"Status Code: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        print(f"✓ Retrieved {len(data)} records")

        if len(data) > 0:
            print(f"\n  Sample record:")
            sample = data[0]
            print(f"    Station: {sample['station_id']}")
            print(f"    Equipment: {sample['equipment_id']}")
            print(f"    Date: {sample['stat_date']}")
            print(f"    Total Energy: {sample['total_energy_consumption_kwh']} kWh")
            print(f"    Total Cooling: {sample['total_cooling_supply_kwh']} kWh")
            print(f"    COP: {sample['avg_cop']}")
            print(f"    Energy Cost: ¥{sample['energy_cost']}")
            print(f"    Cooling Revenue: ¥{sample['cooling_revenue']}")
            print(f"    Net Profit: ¥{sample['net_profit']}")

        return True
    else:
        print(f"✗ Failed to get daily report data")
        print(f"  Error: {response.text}")
        return False

def test_equipment_status(station_id=None, equipment_id=None, limit=5):
    """Test equipment status endpoint"""
    print_section(f"Testing Equipment Status (limit={limit})")

    params = {'limit': limit}
    if station_id:
        params['station_id'] = station_id
    if equipment_id:
        params['equipment_id'] = equipment_id

    response = requests.get(f"{BASE_URL}/api/equipment-status", params=params)
    print(f"Status Code: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        print(f"✓ Retrieved {len(data)} records")

        if len(data) > 0:
            print(f"\n  Sample record:")
            sample = data[0]
            print(f"    Station: {sample['station_id']}")
            print(f"    Equipment: {sample['equipment_id']}")
            print(f"    Time: {sample['stat_time']}")
            print(f"    Supply Temp: {sample['supply_temp']}°C")
            print(f"    Power: {sample['power']} kW")
            print(f"    Run Flag: {sample['run_flag']}")

        return True
    else:
        print(f"✗ Failed to get equipment status data")
        print(f"  Error: {response.text}")
        return False

def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("  Energy Platform API Test Suite")
    print("="*60)
    print(f"  Base URL: {BASE_URL}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    results = []

    # Test 1: Health check
    results.append(("Health Check", test_health_check()))

    # Test 2: Get stations
    stations = test_get_stations()
    results.append(("Get Stations", len(stations) > 0))

    # Test 3: Get equipment
    equipment = test_get_equipment()
    results.append(("Get Equipment", len(equipment) > 0))

    # Test 4: Supply curve
    results.append(("Supply Curve", test_supply_curve(limit=5)))

    # Test 5: Daily report
    results.append(("Daily Report", test_daily_report(limit=5)))

    # Test 6: Equipment status
    results.append(("Equipment Status", test_equipment_status(limit=5)))

    # Print summary
    print_section("Test Summary")
    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status}: {test_name}")

    print(f"\n  Total: {passed}/{total} tests passed")

    if passed == total:
        print("\n  ✓ All tests passed!")
        return 0
    else:
        print(f"\n  ✗ {total - passed} test(s) failed")
        return 1

if __name__ == "__main__":
    try:
        exit(main())
    except requests.exceptions.ConnectionError:
        print("\n✗ ERROR: Cannot connect to API server")
        print("  Please make sure the API server is running:")
        print("  cd /home/student/energy-platform/backend && ./start_api.sh")
        exit(1)
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        exit(1)
