// Main Application Logic
const App = {
    // Current filters
    filters: {
        stationId: null,
        equipmentId: null,
        startDate: null,
        endDate: null
    },

    // Initialize application
    async init() {
        console.log('Initializing Energy Platform Frontend...');

        // Initialize table manager
        TableManager.init();

        // Set default date range (last 30 days)
        this.setDefaultDateRange();

        // Load initial data
        await this.loadMetadata();

        // Set up event listeners
        this.setupEventListeners();

        // Load initial data
        await this.loadData();

        // Update last update timestamp
        this.updateTimestamp();

        console.log('Application initialized successfully');
    },

    // Set default date range
    setDefaultDateRange() {
        // 设置为2018年数据范围
        const startDate = '2018-01-01';
        const endDate = '2018-05-31';

        document.getElementById('startDate').value = startDate;
        document.getElementById('endDate').value = endDate;

        this.filters.startDate = startDate;
        this.filters.endDate = endDate;
    },

    // Load metadata (stations and equipment)
    async loadMetadata() {
        try {
            // Load stations
            const stations = await APIService.getStations();
            const stationSelect = document.getElementById('stationSelect');

            stations.forEach(station => {
                const option = document.createElement('option');
                option.value = station;
                option.textContent = station;
                stationSelect.appendChild(option);
            });

            // Load equipment
            const equipment = await APIService.getEquipment();
            const equipmentSelect = document.getElementById('equipmentSelect');

            equipment.forEach(eq => {
                const option = document.createElement('option');
                option.value = eq;
                option.textContent = eq;
                equipmentSelect.appendChild(option);
            });

            console.log(`Loaded ${stations.length} stations and ${equipment.length} equipment`);
        } catch (error) {
            console.error('Failed to load metadata:', error);
        }
    },

    // Set up event listeners
    setupEventListeners() {
        // Station selection change
        document.getElementById('stationSelect').addEventListener('change', async (e) => {
            this.filters.stationId = e.target.value || null;

            // Reload equipment list based on station
            const equipmentSelect = document.getElementById('equipmentSelect');
            equipmentSelect.innerHTML = '<option value="">全部设备</option>';

            if (this.filters.stationId) {
                const equipment = await APIService.getEquipment(this.filters.stationId);
                equipment.forEach(eq => {
                    const option = document.createElement('option');
                    option.value = eq;
                    option.textContent = eq;
                    equipmentSelect.appendChild(option);
                });
            }
        });

        // Equipment selection change
        document.getElementById('equipmentSelect').addEventListener('change', (e) => {
            this.filters.equipmentId = e.target.value || null;
        });

        // Date range change
        document.getElementById('startDate').addEventListener('change', (e) => {
            this.filters.startDate = e.target.value || null;
        });

        document.getElementById('endDate').addEventListener('change', (e) => {
            this.filters.endDate = e.target.value || null;
        });

        // Query button
        document.getElementById('queryBtn').addEventListener('click', () => {
            this.loadData();
        });

        // Refresh button
        document.getElementById('refreshBtn').addEventListener('click', () => {
            this.refresh();
        });
    },

    // Load data from API
    async loadData() {
        try {
            console.log('Loading data with filters:', this.filters);

            // Query daily report
            const dailyData = await APIService.getDailyReport(this.filters);
            console.log(`Loaded ${dailyData.length} daily records`);

            // Query hourly supply curve
            const hourlyData = await APIService.getSupplyCurve(this.filters);
            console.log(`Loaded ${hourlyData.length} hourly records`);

            // Update status cards
            this.updateStatusCards(dailyData);

            // Update charts
            ChartsManager.updateAllCharts(dailyData, hourlyData);

            // Update table
            TableManager.updateTable(dailyData, hourlyData);

            // Update timestamp
            this.updateTimestamp();

            console.log('Data loaded and visualized successfully');
        } catch (error) {
            console.error('Failed to load data:', error);
        }
    },

    // Update status cards with aggregated data
    updateStatusCards(dailyData) {
        if (!dailyData || dailyData.length === 0) {
            document.getElementById('totalEnergy').textContent = '--';
            document.getElementById('totalCooling').textContent = '--';
            document.getElementById('avgCOP').textContent = '--';
            document.getElementById('netProfit').textContent = '--';
            return;
        }

        // Calculate totals
        let totalEnergy = 0;
        let totalCooling = 0;
        let totalProfit = 0;
        let copSum = 0;
        let copCount = 0;

        dailyData.forEach(row => {
            totalEnergy += row.total_energy_consumption_kwh || 0;
            totalCooling += row.total_cooling_supply_kwh || 0;
            totalProfit += row.net_profit || 0;

            if (row.avg_cop && row.avg_cop > 0) {
                copSum += row.avg_cop;
                copCount++;
            }
        });

        const avgCOP = copCount > 0 ? copSum / copCount : 0;

        // Update cards
        document.getElementById('totalEnergy').textContent = NumberUtils.formatNumber(totalEnergy, 0);
        document.getElementById('totalCooling').textContent = NumberUtils.formatNumber(totalCooling, 0);
        document.getElementById('avgCOP').textContent = NumberUtils.formatNumber(avgCOP, 2);

        const profitElement = document.getElementById('netProfit');
        profitElement.textContent = NumberUtils.formatNumber(totalProfit, 0);
        profitElement.className = 'card-value ' + (totalProfit >= 0 ? 'text-success' : 'text-danger');
    },

    // Update last update timestamp
    updateTimestamp() {
        document.getElementById('lastUpdate').textContent = DateUtils.getCurrentTimestamp();
    },

    // Refresh all data
    async refresh() {
        console.log('Refreshing data...');
        await this.loadData();
    }
};

// Initialize application when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    App.init().catch(error => {
        console.error('Failed to initialize application:', error);
        alert('应用初始化失败，请检查API服务是否运行');
    });
});

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = App;
}
