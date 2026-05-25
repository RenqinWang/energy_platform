"""
FastAPI Backend for Energy Platform
Provides REST API endpoints for querying Delta Lake data
"""
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, date
import config
from data_access import dal


# Pydantic models for API responses
class SupplyCurveRecord(BaseModel):
    station_id: str
    equipment_id: str
    stat_hour: str
    avg_supply_temp: Optional[float]
    max_supply_temp: Optional[float]
    min_supply_temp: Optional[float]
    avg_power: Optional[float]
    run_minutes: Optional[float]
    energy_consumption_kwh: Optional[float]
    cooling_capacity_kw: Optional[float]
    cooling_supply_kwh: Optional[float]
    operation_rate: Optional[float]
    record_count: Optional[int]
    dt: Optional[str]


class DailyReportRecord(BaseModel):
    station_id: str
    equipment_id: str
    stat_date: str
    peak_cooling_kwh: Optional[float]
    valley_cooling_kwh: Optional[float]
    peak_valley_ratio: Optional[float]
    peak_duration_hours: Optional[float]
    valley_duration_hours: Optional[float]
    avg_supply_temp: Optional[float]
    total_energy_consumption_kwh: Optional[float]
    total_cooling_supply_kwh: Optional[float]
    total_runtime_hours: Optional[float]
    avg_cooling_supply_kwh: Optional[float]
    total_run_minutes: Optional[float]
    daily_operation_rate: Optional[float]
    avg_cop: Optional[float]
    energy_cost: Optional[float]
    cooling_revenue: Optional[float]
    net_profit: Optional[float]
    hour_count: Optional[int]
    dt: Optional[str]


class EquipmentStatusRecord(BaseModel):
    station_id: str
    equipment_id: str
    stat_time: str
    supply_temp: Optional[float]
    return_temp: Optional[float]
    pressure: Optional[float]
    flow: Optional[float]
    power: Optional[float]
    runtime_hours: Optional[float]
    start_count: Optional[int]
    run_flag: Optional[int]
    record_count: Optional[int]
    dt: Optional[str]


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    version: str


class StationListResponse(BaseModel):
    stations: List[str]


class EquipmentListResponse(BaseModel):
    equipment: List[str]


class ForecastRecord(BaseModel):
    station_id: str
    equipment_id: str
    forecast_time: str
    target_hour: str
    predicted_cooling_kwh: float
    confidence_lower: float
    confidence_upper: float
    model_version: str
    dt: Optional[str]


class AdviceRecord(BaseModel):
    station_id: str
    equipment_id: str
    advice_time: str
    advice_type: str
    risk_level: str
    advice_text: str
    evidence_metrics: str
    rule_id: str
    is_active: bool
    dt: Optional[str]


class WeeklyReportRecord(BaseModel):
    station_id: str
    equipment_id: str
    stat_week_str: str
    week_start_date: str
    week_end_date: str
    peak_cooling_kwh: Optional[float]
    valley_cooling_kwh: Optional[float]
    peak_valley_ratio: Optional[float]
    peak_duration_hours: Optional[float]
    valley_duration_hours: Optional[float]
    total_cooling_supply_kwh: Optional[float]
    total_energy_consumption_kwh: Optional[float]
    total_runtime_hours: Optional[float]
    avg_cooling_supply_kwh: Optional[float]
    avg_cop: Optional[float]
    avg_supply_temp: Optional[float]
    equipment_utilization_rate: Optional[float]
    load_factor: Optional[float]
    total_energy_cost: Optional[float]
    total_cooling_revenue: Optional[float]
    net_profit: Optional[float]
    data_completeness_rate: Optional[float]
    hour_count: Optional[int]
    dt: Optional[str]


class MonthlyReportRecord(BaseModel):
    station_id: str
    equipment_id: str
    stat_month_str: str
    month_start_date: str
    month_end_date: str
    days_in_month: Optional[int]
    peak_cooling_kwh: Optional[float]
    valley_cooling_kwh: Optional[float]
    peak_valley_ratio: Optional[float]
    peak_duration_hours: Optional[float]
    valley_duration_hours: Optional[float]
    total_cooling_supply_kwh: Optional[float]
    total_energy_consumption_kwh: Optional[float]
    total_runtime_hours: Optional[float]
    avg_cooling_supply_kwh: Optional[float]
    avg_cop: Optional[float]
    avg_supply_temp: Optional[float]
    equipment_utilization_rate: Optional[float]
    load_factor: Optional[float]
    total_energy_cost: Optional[float]
    total_cooling_revenue: Optional[float]
    net_profit: Optional[float]
    data_completeness_rate: Optional[float]
    hour_count: Optional[int]
    dt: Optional[str]


class RevenueForecastRecord(BaseModel):
    station_id: str
    equipment_id: str
    forecast_date: str
    target_hour: str
    forecast_hour: Optional[int]
    predicted_cooling_kwh: Optional[float]
    predicted_energy_kwh: Optional[float]
    energy_price: Optional[float]
    cooling_price: Optional[float]
    predicted_energy_cost: Optional[float]
    predicted_cooling_revenue: Optional[float]
    predicted_profit: Optional[float]
    profit_margin: Optional[float]
    model_version: Optional[str]
    dt: Optional[str]


class DataQualityRecord(BaseModel):
    station_id: str
    equipment_id: str
    stat_date: str
    total_records: Optional[int]
    expected_records: Optional[int]
    running_records: Optional[int] = None
    missing_running_power_records: Optional[int] = None
    missing_running_energy_records: Optional[int] = None
    missing_running_cooling_records: Optional[int] = None
    missing_records: Optional[int]
    completeness_rate: Optional[float]
    supply_temp_valid_rate: Optional[float]
    power_valid_rate: Optional[float]
    energy_valid_rate: Optional[float]
    cooling_capacity_valid_rate: Optional[float]
    cooling_supply_valid_rate: Optional[float]
    avg_field_valid_rate: Optional[float]
    null_field_count: Optional[int]
    zero_supply_temp: Optional[int]
    zero_power: Optional[int]
    zero_energy: Optional[int]
    zero_cooling_supply: Optional[int]
    total_run_minutes: Optional[float]
    data_quality_score: Optional[float]
    quality_flag: Optional[str]
    dt: Optional[str]


# Initialize FastAPI app
app = FastAPI(
    title=config.API_TITLE,
    version=config.API_VERSION,
    description=config.API_DESCRIPTION
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_model=HealthResponse)
async def root():
    """Root endpoint - health check"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": config.API_VERSION
    }


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": config.API_VERSION
    }


@app.get("/api/data-mode")
async def get_data_mode():
    """Return the active lake namespace used by this API process."""
    return {
        "mode": config.DATA_MODE,
        "lake_path": config.HDFS_LAKE_PATH,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/api/stations", response_model=StationListResponse)
async def get_stations():
    """Get list of all station IDs"""
    try:
        stations = dal.get_station_list()
        return {"stations": stations}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query stations: {str(e)}")


@app.get("/api/equipment", response_model=EquipmentListResponse)
async def get_equipment(
    station_id: Optional[str] = Query(None, description="Filter by station ID")
):
    """Get list of equipment IDs, optionally filtered by station"""
    try:
        equipment = dal.get_equipment_list(station_id)
        return {"equipment": equipment}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query equipment: {str(e)}")


@app.get("/api/system-equipment", response_model=EquipmentListResponse)
async def get_system_equipment(
    system_type: Optional[str] = Query(None, description="Filter by system type: chiller/heating/cchp"),
    station_id: Optional[str] = Query(None, description="Filter by station ID")
):
    """Get equipment IDs from unified system Gold reports."""
    try:
        equipment = dal.get_system_equipment_list(system_type=system_type, station_id=station_id)
        return {"equipment": equipment}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query system equipment: {str(e)}")


@app.get("/api/system-supply-curve", response_model=List[Dict[str, Any]])
async def get_system_supply_curve(
    system_type: Optional[str] = Query(None, description="Filter by system type: chiller/heating/cchp"),
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """Query unified hourly Gold data for cold, heat, and CCHP systems."""
    try:
        return dal.query_system_supply_curve(
            system_type=system_type,
            station_id=station_id,
            equipment_id=equipment_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query system supply curve: {str(e)}")


@app.get("/api/system-daily-report", response_model=List[Dict[str, Any]])
async def get_system_daily_report(
    system_type: Optional[str] = Query(None, description="Filter by system type: chiller/heating/cchp"),
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """Query unified daily comprehensive report."""
    try:
        return dal.query_system_daily_report(
            system_type=system_type,
            station_id=station_id,
            equipment_id=equipment_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query system daily report: {str(e)}")


@app.get("/api/system-weekly-report", response_model=List[Dict[str, Any]])
async def get_system_weekly_report(
    system_type: Optional[str] = Query(None, description="Filter by system type: chiller/heating/cchp"),
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    start_week: Optional[str] = Query(None, description="Start week (format: 2018-W30)"),
    end_week: Optional[str] = Query(None, description="End week (format: 2018-W35)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """Query unified weekly comprehensive report."""
    try:
        return dal.query_system_weekly_report(
            system_type=system_type,
            station_id=station_id,
            equipment_id=equipment_id,
            start_week=start_week,
            end_week=end_week,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query system weekly report: {str(e)}")


@app.get("/api/system-monthly-report", response_model=List[Dict[str, Any]])
async def get_system_monthly_report(
    system_type: Optional[str] = Query(None, description="Filter by system type: chiller/heating/cchp"),
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    start_month: Optional[str] = Query(None, description="Start month (format: 2018-07)"),
    end_month: Optional[str] = Query(None, description="End month (format: 2018-09)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """Query unified monthly comprehensive report."""
    try:
        return dal.query_system_monthly_report(
            system_type=system_type,
            station_id=station_id,
            equipment_id=equipment_id,
            start_month=start_month,
            end_month=end_month,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query system monthly report: {str(e)}")


@app.get("/api/system-forecast", response_model=List[Dict[str, Any]])
async def get_system_forecast(
    system_type: Optional[str] = Query(None, description="Filter by system type: chiller/heating/cchp"),
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """Query unified 24-hour supply forecast data."""
    try:
        return dal.query_system_forecast(
            system_type=system_type,
            station_id=station_id,
            equipment_id=equipment_id,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query system forecast: {str(e)}")


@app.get("/api/system-forecast-metrics", response_model=List[Dict[str, Any]])
async def get_system_forecast_metrics(
    system_type: Optional[str] = Query(None, description="Filter by system type: chiller/heating/cchp"),
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """Query model explanation and evaluation metrics for unified forecasts."""
    try:
        return dal.query_system_forecast_metrics(
            system_type=system_type,
            station_id=station_id,
            equipment_id=equipment_id,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query system forecast metrics: {str(e)}")


@app.get("/api/system-revenue-forecast", response_model=List[Dict[str, Any]])
async def get_system_revenue_forecast(
    system_type: Optional[str] = Query(None, description="Filter by system type: chiller/heating/cchp"),
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    forecast_date: Optional[str] = Query(None, description="Forecast date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """Query unified revenue forecast data."""
    try:
        return dal.query_system_revenue_forecast(
            system_type=system_type,
            station_id=station_id,
            equipment_id=equipment_id,
            forecast_date=forecast_date,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query system revenue forecast: {str(e)}")


@app.get("/api/dashboard/summary", response_model=Dict[str, Any])
async def get_dashboard_summary(
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    stat_date: Optional[str] = Query(None, description="Dashboard date (YYYY-MM-DD)"),
    start_date: Optional[str] = Query(None, description="Dashboard start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Dashboard end date (YYYY-MM-DD)")
):
    """Return dashboard KPI, equipment matrix, history/forecast and advice summary."""
    try:
        return dal.get_dashboard_summary(
            station_id=station_id,
            stat_date=stat_date,
            start_date=start_date,
            end_date=end_date,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query dashboard summary: {str(e)}")


@app.get("/api/dashboard/dates", response_model=List[str])
async def get_dashboard_dates(
    station_id: Optional[str] = Query(None, description="Filter by station ID")
):
    """Return dates that have Gold dashboard data."""
    try:
        return dal.query_dashboard_dates(station_id=station_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query dashboard dates: {str(e)}")


@app.get("/api/price-history", response_model=List[Dict[str, Any]])
async def get_price_history(
    station_id: Optional[str] = Query(None, description="Station code, for example ST001"),
    price_type: Optional[str] = Query(None, description="cooling/heating/electricity"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """Query Silver price dimension history."""
    try:
        return dal.query_price_history(
            station_id=station_id,
            price_type=price_type,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query price history: {str(e)}")


@app.get("/api/operation-advice", response_model=List[Dict[str, Any]])
async def get_operation_advice(
    system_type: Optional[str] = Query(None, description="Filter by system type: chiller/heating/cchp"),
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    risk_level: Optional[str] = Query(None, description="Filter by risk level"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records")
):
    """Query unified operation advice for all systems."""
    try:
        return dal.query_operation_advice(
            system_type=system_type,
            station_id=station_id,
            equipment_id=equipment_id,
            risk_level=risk_level,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query operation advice: {str(e)}")


@app.get("/api/supply-curve", response_model=List[SupplyCurveRecord])
async def get_supply_curve(
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """
    Query hourly supply curve data

    Returns hourly aggregated data including:
    - Temperature metrics (avg, max, min)
    - Power consumption
    - Energy consumption (kWh)
    - Cooling capacity and supply
    - Operation rate
    """
    try:
        data = dal.query_supply_curve(
            station_id=station_id,
            equipment_id=equipment_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query supply curve: {str(e)}")


@app.get("/api/daily-report", response_model=List[DailyReportRecord])
async def get_daily_report(
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """
    Query daily report data

    Returns daily aggregated data including:
    - Energy consumption and cooling supply
    - COP (Coefficient of Performance)
    - Economic indicators (cost, revenue, profit)
    - Operation rate
    """
    try:
        data = dal.query_daily_report(
            station_id=station_id,
            equipment_id=equipment_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query daily report: {str(e)}")


@app.get("/api/equipment-status", response_model=List[EquipmentStatusRecord])
async def get_equipment_status(
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    start_time: Optional[str] = Query(None, description="Start time (YYYY-MM-DD HH:mm:ss)"),
    end_time: Optional[str] = Query(None, description="End time (YYYY-MM-DD HH:mm:ss)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """
    Query equipment status data (minute-level)

    Returns minute-level equipment status including:
    - Temperature, pressure, flow
    - Power consumption
    - Runtime hours and start count
    - Run flag (on/off status)
    """
    try:
        data = dal.query_equipment_status(
            station_id=station_id,
            equipment_id=equipment_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query equipment status: {str(e)}")


@app.get("/api/forecast", response_model=List[ForecastRecord])
async def get_forecast(
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    hours: int = Query(24, ge=1, le=168, description="Number of hours to forecast"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """
    Query forecast data

    Returns predicted cooling supply for future hours including:
    - Predicted cooling supply (kWh)
    - Confidence interval (lower and upper bounds)
    - Model version
    """
    try:
        data = dal.query_forecast(
            station_id=station_id,
            equipment_id=equipment_id,
            hours=hours,
            limit=limit
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query forecast: {str(e)}")


@app.get("/api/advice", response_model=List[AdviceRecord])
async def get_advice(
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    risk_level: Optional[str] = Query(None, description="Filter by risk level (low, medium, high)"),
    advice_type: Optional[str] = Query(None, description="Filter by advice type (load_change, anomaly, efficiency, economic)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records")
):
    """
    Query operation advice data

    Returns operation recommendations including:
    - Advice text (Chinese description)
    - Risk level (low, medium, high)
    - Advice type (load_change, anomaly, efficiency, economic)
    - Evidence metrics (JSON format)
    """
    try:
        data = dal.query_advice(
            station_id=station_id,
            equipment_id=equipment_id,
            risk_level=risk_level,
            advice_type=advice_type,
            limit=limit
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query advice: {str(e)}")


@app.get("/api/weekly-report", response_model=List[WeeklyReportRecord])
async def get_weekly_report(
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    start_week: Optional[str] = Query(None, description="Start week (format: 2018-W30)"),
    end_week: Optional[str] = Query(None, description="End week (format: 2018-W35)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records")
):
    """
    Query weekly report data

    Returns weekly aggregated data including:
    - Peak and valley cooling values
    - Peak/valley duration hours
    - Total cooling supply and energy consumption
    - Equipment utilization rate and load factor
    - Economic metrics (cost, revenue, profit)
    - Data completeness rate
    """
    try:
        data = dal.query_weekly_report(
            station_id=station_id,
            equipment_id=equipment_id,
            start_week=start_week,
            end_week=end_week,
            limit=limit
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query weekly report: {str(e)}")


@app.get("/api/monthly-report", response_model=List[MonthlyReportRecord])
async def get_monthly_report(
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    start_month: Optional[str] = Query(None, description="Start month (format: 2018-07)"),
    end_month: Optional[str] = Query(None, description="End month (format: 2018-09)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records")
):
    """
    Query monthly report data

    Returns monthly aggregated data including:
    - Peak and valley cooling values
    - Peak/valley duration hours
    - Total cooling supply and energy consumption
    - Equipment utilization rate and load factor
    - Economic metrics (cost, revenue, profit)
    - Data completeness rate
    """
    try:
        data = dal.query_monthly_report(
            station_id=station_id,
            equipment_id=equipment_id,
            start_month=start_month,
            end_month=end_month,
            limit=limit
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query monthly report: {str(e)}")


@app.get("/api/revenue-forecast", response_model=List[RevenueForecastRecord])
async def get_revenue_forecast(
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    forecast_date: Optional[str] = Query(None, description="Forecast date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """
    Query revenue forecast data

    Returns revenue predictions including:
    - Predicted cooling supply and energy consumption
    - Energy price (time-of-use pricing)
    - Predicted costs and revenue
    - Predicted profit and profit margin
    """
    try:
        data = dal.query_revenue_forecast(
            station_id=station_id,
            equipment_id=equipment_id,
            forecast_date=forecast_date,
            limit=limit
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query revenue forecast: {str(e)}")


@app.get("/api/data-quality", response_model=List[DataQualityRecord])
async def get_data_quality(
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    equipment_id: Optional[str] = Query(None, description="Filter by equipment ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    quality_flag: Optional[str] = Query(None, description="Filter by quality flag (good, warning, poor)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records")
):
    """
    Query data quality scores

    Returns data quality metrics including:
    - Completeness rate
    - Field validity rates
    - Quality score (0-100)
    - Quality flag (good/warning/poor)
    """
    try:
        data = dal.query_data_quality(
            station_id=station_id,
            equipment_id=equipment_id,
            start_date=start_date,
            end_date=end_date,
            quality_flag=quality_flag,
            limit=limit
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query data quality: {str(e)}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    dal.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
