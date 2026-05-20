# Energy Platform Backend API

FastAPI-based REST API for querying energy platform data from Delta Lake.

## Architecture

- **Framework**: FastAPI 0.104.1
- **Data Processing**: PySpark 3.5.0 with Delta Lake 3.0.0
- **Storage**: HDFS (hdfs://node1:9000) with Delta Lake format
- **API Server**: Uvicorn with auto-reload support

## Directory Structure

```
backend/
├── main.py              # FastAPI application with endpoints
├── data_access.py       # Data access layer using PySpark
├── config.py            # Configuration settings
├── requirements.txt     # Python dependencies
├── start_api.sh         # Server startup script
├── test_api.py          # API test suite
└── README.md            # This file
```

## Installation

### Prerequisites

- Python 3.8+ (via Conda)
- Java 17 (JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64)
- Apache Spark 3.5.7 (SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3)
- Access to HDFS cluster (hdfs://node1:9000)
- Conda environment (base)

### Setup

1. Navigate to backend directory:
```bash
cd /home/student/energy-platform/backend
```

2. Activate conda base environment and install dependencies:
```bash
conda activate base
pip install -r requirements.txt
```

Or use Tsinghua mirror for faster download:
```bash
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt
```

## Running the API Server

### Quick Start

Use the provided startup script:
```bash
./start_api.sh
```

The API server will start on `http://0.0.0.0:8000`

### Manual Start

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3
source venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### Access API Documentation

Once the server is running, access the interactive API documentation:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## API Endpoints

### Health Check

- `GET /` - Root endpoint
- `GET /health` - Health check endpoint

### Metadata Endpoints

- `GET /api/stations` - Get list of all station IDs
- `GET /api/equipment?station_id={id}` - Get list of equipment IDs (optionally filtered by station)

### Data Query Endpoints

#### 1. Supply Curve (Hourly)

`GET /api/supply-curve`

Query hourly supply curve data from `gold_supply_curve_hourly` table.

**Query Parameters:**
- `station_id` (optional): Filter by station ID
- `equipment_id` (optional): Filter by equipment ID
- `start_date` (optional): Start date (YYYY-MM-DD format)
- `end_date` (optional): End date (YYYY-MM-DD format)
- `limit` (optional): Maximum records to return (default: 1000, max: 10000)

**Response Fields:**
- `station_id`: Station identifier
- `equipment_id`: Equipment identifier
- `stat_hour`: Statistical hour (YYYY-MM-DD HH:00:00)
- `avg_supply_temp`: Average supply temperature (°C)
- `max_supply_temp`: Maximum supply temperature (°C)
- `min_supply_temp`: Minimum supply temperature (°C)
- `avg_power`: Average power consumption (kW)
- `run_minutes`: Total run minutes in the hour
- `energy_consumption_kwh`: Energy consumption (kWh)
- `cooling_capacity_kw`: Cooling capacity (kW)
- `cooling_supply_kwh`: Cooling supply (kWh)
- `operation_rate`: Operation rate (%)
- `record_count`: Number of minute-level records aggregated

**Example:**
```bash
curl "http://localhost:8000/api/supply-curve?equipment_id=LJ001&limit=10"
```

#### 2. Daily Report

`GET /api/daily-report`

Query daily report data from `gold_report_daily` table.

**Query Parameters:**
- `station_id` (optional): Filter by station ID
- `equipment_id` (optional): Filter by equipment ID
- `start_date` (optional): Start date (YYYY-MM-DD format)
- `end_date` (optional): End date (YYYY-MM-DD format)
- `limit` (optional): Maximum records to return (default: 1000, max: 10000)

**Response Fields:**
- `station_id`: Station identifier
- `equipment_id`: Equipment identifier
- `stat_date`: Statistical date (YYYY-MM-DD)
- `avg_supply_temp`: Average supply temperature (°C)
- `total_energy_consumption_kwh`: Total energy consumption (kWh)
- `total_cooling_supply_kwh`: Total cooling supply (kWh)
- `total_run_minutes`: Total run minutes
- `daily_operation_rate`: Daily operation rate (%)
- `avg_cop`: Average COP (Coefficient of Performance)
- `energy_cost`: Energy cost (¥)
- `cooling_revenue`: Cooling revenue (¥)
- `net_profit`: Net profit (¥)
- `hour_count`: Number of hourly records aggregated

**Example:**
```bash
curl "http://localhost:8000/api/daily-report?start_date=2024-01-01&end_date=2024-01-31"
```

#### 3. Equipment Status (Minute-level)

`GET /api/equipment-status`

Query minute-level equipment status from `silver_chiller_status` table.

**Query Parameters:**
- `station_id` (optional): Filter by station ID
- `equipment_id` (optional): Filter by equipment ID
- `start_time` (optional): Start time (YYYY-MM-DD HH:mm:ss format)
- `end_time` (optional): End time (YYYY-MM-DD HH:mm:ss format)
- `limit` (optional): Maximum records to return (default: 1000, max: 10000)

**Response Fields:**
- `station_id`: Station identifier
- `equipment_id`: Equipment identifier
- `stat_time`: Statistical time (YYYY-MM-DD HH:mm:ss)
- `supply_temp`: Supply temperature (°C)
- `pressure`: Pressure (kPa)
- `flow`: Flow rate (m³/h)
- `power`: Power consumption (kW)
- `runtime_hours`: Cumulative runtime hours
- `start_count`: Cumulative start count
- `run_flag`: Run status (1=running, 0=stopped)
- `record_count`: Number of raw records aggregated

**Example:**
```bash
curl "http://localhost:8000/api/equipment-status?equipment_id=LJ001&limit=100"
```

## Testing

Run the test suite to verify all endpoints:

```bash
# Make sure the API server is running first
./test_api.py
```

The test suite will:
1. Check health endpoint
2. Query station list
3. Query equipment list
4. Test supply curve endpoint
5. Test daily report endpoint
6. Test equipment status endpoint

## Data Sources

The API queries data from the following Delta Lake tables in HDFS:

- **Silver Layer**:
  - `hdfs://node1:9000/lake/silver/silver_chiller_status` - Minute-level equipment status

- **Gold Layer**:
  - `hdfs://node1:9000/lake/gold/gold_supply_curve_hourly` - Hourly supply curve
  - `hdfs://node1:9000/lake/gold/gold_report_daily` - Daily reports

## Configuration

Edit `config.py` to modify:
- HDFS namenode address
- Delta Lake table paths
- CORS allowed origins
- API metadata (title, version, description)

## Performance Considerations

- The API uses a singleton Spark session shared across all requests
- Query results are limited by default (max 10000 records per request)
- For large datasets, use date/time filters to reduce result size
- Spark log level is set to WARN to reduce console output

## Troubleshooting

### Connection Refused
- Ensure HDFS cluster is accessible: `hdfs dfs -ls hdfs://node1:9000/lake`
- Check network connectivity to node1

### Spark Initialization Errors
- Verify JAVA_HOME is set correctly
- Verify SPARK_HOME points to Spark 3.5.7 installation
- Check Delta Lake JAR files are present in Spark installation

### Empty Results
- Verify Delta Lake tables exist and contain data
- Check table paths in `config.py`
- Run verification scripts in `../scripts/` directory

## Development

### Adding New Endpoints

1. Add query method to `data_access.py`
2. Define Pydantic response model in `main.py`
3. Create endpoint handler in `main.py`
4. Add test case to `test_api.py`

### Code Style

- Follow PEP 8 style guide
- Use type hints for function parameters and returns
- Document all endpoints with docstrings
- Handle exceptions and return appropriate HTTP status codes

## License

Internal use only - Enterprise private data, do not upload to public network.
