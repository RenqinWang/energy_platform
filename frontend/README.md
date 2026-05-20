# Energy Platform Frontend

Web-based data visualization interface for the Energy Platform.

## Overview

This frontend provides an interactive dashboard for visualizing energy consumption, cooling supply, and economic indicators from the energy platform's Delta Lake data warehouse.

## Features

- **Real-time Data Visualization**: Interactive charts showing hourly and daily trends
- **Multi-dimensional Filtering**: Filter by station, equipment, and date range
- **Status Cards**: Quick overview of key metrics (energy, cooling, COP, profit)
- **Multiple Chart Types**:
  - Daily energy consumption and cooling supply trends
  - Hourly supply curve with power and temperature
  - COP trend analysis with operation rate
  - Economic indicators (cost, revenue, profit)
- **Data Tables**: Detailed tabular view with daily and hourly data
- **CSV Export**: Export filtered data to CSV files
- **Responsive Design**: Works on desktop and mobile devices

## Architecture

- **Pure HTML/CSS/JavaScript**: No build process required
- **Chart.js 4.4.0**: For data visualization
- **Axios 1.6.0**: For API communication
- **Modular Design**: Separated concerns (API, Charts, Tables, Main logic)

## Directory Structure

```
frontend/
├── index.html           # Main HTML page
├── css/
│   └── style.css        # Stylesheet
├── js/
│   ├── config.js        # Configuration and utilities
│   ├── api.js           # API service layer
│   ├── charts.js        # Chart management
│   ├── table.js         # Table rendering and export
│   └── main.js          # Main application logic
└── README.md            # This file
```

## Prerequisites

- Backend API server running on `http://localhost:8000`
- Modern web browser (Chrome, Firefox, Safari, Edge)

## Installation

No installation required! This is a static web application.

## Running the Frontend

### Option 1: Simple HTTP Server (Python)

```bash
cd /home/student/energy-platform/frontend
python3 -m http.server 8080
```

Then open http://localhost:8080 in your browser.

### Option 2: Simple HTTP Server (Node.js)

```bash
cd /home/student/energy-platform/frontend
npx http-server -p 8080
```

Then open http://localhost:8080 in your browser.

### Option 3: Direct File Access

Simply open `index.html` in your web browser. Note: Some browsers may block API requests due to CORS when opening files directly. Use Option 1 or 2 for best results.

## Usage

### 1. Start the Backend API

Before using the frontend, ensure the backend API is running:

```bash
cd /home/student/energy-platform/backend
./start_api.sh
```

The API should be accessible at http://localhost:8000

### 2. Open the Frontend

Access the frontend at http://localhost:8080 (or whichever port you chose).

### 3. Using the Dashboard

#### Filtering Data

- **Station Selection**: Choose a specific station or view all stations
- **Equipment Selection**: Choose specific equipment or view all equipment
- **Date Range**: Select start and end dates (default: last 30 days)
- **Query Button**: Click to apply filters and reload data
- **Refresh Button**: Reload data with current filters

#### Viewing Charts

The dashboard displays four main charts:

1. **Daily Report Chart**: Shows energy consumption and cooling supply trends over time
2. **Hourly Supply Curve**: Displays power consumption and supply temperature (last 48 hours)
3. **COP Trend Chart**: Shows COP values and operation rate over time
4. **Economic Analysis**: Visualizes energy cost, cooling revenue, and net profit

#### Viewing Tables

- **Daily Report Tab**: Shows daily aggregated data with economic indicators
- **Hourly Data Tab**: Shows hourly supply curve data (last 100 records)
- **Export CSV**: Download current table view as CSV file

#### Status Cards

At the top of the dashboard, four cards show aggregated metrics for the selected date range:

- **Total Energy**: Total energy consumption (kWh)
- **Total Cooling**: Total cooling supply (kWh)
- **Average COP**: Average coefficient of performance
- **Net Profit**: Total net profit (¥)

## Configuration

### API Endpoint

To change the API endpoint, edit `js/config.js`:

```javascript
const API_CONFIG = {
    baseURL: 'http://localhost:8000',  // Change this to your API URL
    // ...
};
```

### Chart Appearance

Customize chart colors and styles in `js/charts.js` by modifying the Chart.js configuration.

### Date Range

Default date range is last 30 days. To change this, edit `js/main.js`:

```javascript
setDefaultDateRange() {
    const endDate = DateUtils.getToday();
    const startDate = DateUtils.getDaysAgo(30);  // Change 30 to desired days
    // ...
}
```

## Browser Compatibility

- Chrome 90+
- Firefox 88+
- Safari 14+
- Edge 90+

## Troubleshooting

### "Cannot connect to API server" Error

**Cause**: Backend API is not running or not accessible.

**Solution**:
1. Check if the backend API is running: `curl http://localhost:8000/health`
2. Start the backend API: `cd /home/student/energy-platform/backend && ./start_api.sh`
3. Verify the API URL in `js/config.js` matches your backend

### CORS Errors

**Cause**: Browser blocking cross-origin requests.

**Solution**:
1. Serve the frontend through a web server (not direct file access)
2. Ensure backend CORS settings allow your frontend origin (configured in `backend/config.py`)

### Charts Not Displaying

**Cause**: Chart.js library not loaded or data format issues.

**Solution**:
1. Check browser console for JavaScript errors
2. Verify internet connection (Chart.js loaded from CDN)
3. Check that API is returning valid data

### Empty Data Tables

**Cause**: No data available for selected filters.

**Solution**:
1. Verify Delta Lake tables contain data: Run `backend/test_api.py`
2. Adjust date range to include dates with data
3. Remove station/equipment filters to see all data

## Development

### Adding New Charts

1. Add canvas element to `index.html`
2. Create chart function in `js/charts.js`
3. Call chart function from `ChartsManager.updateAllCharts()`

### Adding New API Endpoints

1. Add endpoint to `API_CONFIG` in `js/config.js`
2. Create API method in `js/api.js`
3. Use the method in `js/main.js` or other modules

### Modifying Styles

Edit `css/style.css` to customize colors, fonts, and layout. CSS variables are defined in `:root` for easy theming.

## Performance Considerations

- Default query limit is 1000 records per API call
- Hourly chart displays last 48 hours for better visualization
- Table view shows last 100 records for hourly data
- Use date range filters to reduce data volume for large datasets

## Security Notes

- **Data Privacy**: This system handles enterprise private data - do not deploy to public internet
- **Local Use Only**: Intended for internal network use only
- **No Authentication**: Current version has no authentication - add authentication before production use

## License

Internal use only - Enterprise private data, do not upload to public network.
