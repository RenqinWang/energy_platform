// Charts Module
const ChartsManager = {
    charts: {},

    // Destroy existing chart
    destroyChart(chartId) {
        if (this.charts[chartId]) {
            this.charts[chartId].destroy();
            delete this.charts[chartId];
        }
    },

    // Create daily report chart (energy consumption and cooling supply)
    createDailyReportChart(data) {
        this.destroyChart('dailyReport');

        const ctx = document.getElementById('dailyReportChart').getContext('2d');

        // Sort data by date
        const sortedData = data.sort((a, b) =>
            new Date(a.stat_date) - new Date(b.stat_date)
        );

        const labels = sortedData.map(d => DateUtils.formatDate(d.stat_date));
        const energyData = sortedData.map(d => d.total_energy_consumption_kwh || 0);
        const coolingData = sortedData.map(d => d.total_cooling_supply_kwh || 0);

        this.charts.dailyReport = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: '能耗 (kWh)',
                        data: energyData,
                        borderColor: 'rgb(239, 68, 68)',
                        backgroundColor: 'rgba(239, 68, 68, 0.1)',
                        tension: 0.3,
                        fill: true
                    },
                    {
                        label: '供冷量 (kWh)',
                        data: coolingData,
                        borderColor: 'rgb(59, 130, 246)',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        tension: 0.3,
                        fill: true
                    }
                ]
            },
            options: {
                ...CHART_CONFIG,
                scales: {
                    x: {
                        display: true,
                        grid: { display: false }
                    },
                    y: {
                        display: true,
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'kWh'
                        }
                    }
                }
            }
        });
    },

    // Create hourly supply curve chart (power and temperature)
    createSupplyCurveChart(data) {
        this.destroyChart('supplyCurve');

        const ctx = document.getElementById('supplyCurveChart').getContext('2d');

        // Sort data by hour
        const sortedData = data.sort((a, b) =>
            new Date(a.stat_hour) - new Date(b.stat_hour)
        );

        // Take last 48 hours for better visualization
        const recentData = sortedData.slice(-48);

        const labels = recentData.map(d => {
            const date = new Date(d.stat_hour);
            return date.toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit' });
        });
        const powerData = recentData.map(d => d.avg_power || 0);
        const tempData = recentData.map(d => d.avg_supply_temp || 0);

        this.charts.supplyCurve = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: '平均功率 (kW)',
                        data: powerData,
                        borderColor: 'rgb(245, 158, 11)',
                        backgroundColor: 'rgba(245, 158, 11, 0.1)',
                        tension: 0.3,
                        yAxisID: 'y'
                    },
                    {
                        label: '供水温度 (°C)',
                        data: tempData,
                        borderColor: 'rgb(16, 185, 129)',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        tension: 0.3,
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                ...CHART_CONFIG,
                scales: {
                    x: {
                        display: true,
                        grid: { display: false }
                    },
                    y: {
                        type: 'linear',
                        display: true,
                        position: 'left',
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: '功率 (kW)'
                        }
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: '温度 (°C)'
                        },
                        grid: {
                            drawOnChartArea: false
                        }
                    }
                }
            }
        });
    },

    // Create COP trend chart
    createCOPTrendChart(data) {
        this.destroyChart('copTrend');

        const ctx = document.getElementById('copTrendChart').getContext('2d');

        // Sort data by date
        const sortedData = data.sort((a, b) =>
            new Date(a.stat_date) - new Date(b.stat_date)
        );

        const labels = sortedData.map(d => DateUtils.formatDate(d.stat_date));
        const copData = sortedData.map(d => d.avg_cop || 0);
        const operationRateData = sortedData.map(d => d.daily_operation_rate || 0);

        this.charts.copTrend = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'COP',
                        data: copData,
                        backgroundColor: 'rgba(99, 102, 241, 0.8)',
                        yAxisID: 'y'
                    },
                    {
                        label: '运行率 (%)',
                        data: operationRateData,
                        type: 'line',
                        borderColor: 'rgb(236, 72, 153)',
                        backgroundColor: 'rgba(236, 72, 153, 0.1)',
                        tension: 0.3,
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                ...CHART_CONFIG,
                scales: {
                    x: {
                        display: true,
                        grid: { display: false }
                    },
                    y: {
                        type: 'linear',
                        display: true,
                        position: 'left',
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'COP'
                        }
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        beginAtZero: true,
                        max: 100,
                        title: {
                            display: true,
                            text: '运行率 (%)'
                        },
                        grid: {
                            drawOnChartArea: false
                        }
                    }
                }
            }
        });
    },

    // Create economic analysis chart
    createEconomicChart(data) {
        this.destroyChart('economic');

        const ctx = document.getElementById('economicChart').getContext('2d');

        // Sort data by date
        const sortedData = data.sort((a, b) =>
            new Date(a.stat_date) - new Date(b.stat_date)
        );

        const labels = sortedData.map(d => DateUtils.formatDate(d.stat_date));
        const costData = sortedData.map(d => d.energy_cost || 0);
        const revenueData = sortedData.map(d => d.cooling_revenue || 0);
        const profitData = sortedData.map(d => d.net_profit || 0);

        this.charts.economic = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: '能源成本 (元)',
                        data: costData,
                        backgroundColor: 'rgba(239, 68, 68, 0.8)'
                    },
                    {
                        label: '供冷收入 (元)',
                        data: revenueData,
                        backgroundColor: 'rgba(34, 197, 94, 0.8)'
                    },
                    {
                        label: '净利润 (元)',
                        data: profitData,
                        type: 'line',
                        borderColor: 'rgb(37, 99, 235)',
                        backgroundColor: 'rgba(37, 99, 235, 0.1)',
                        tension: 0.3,
                        fill: true
                    }
                ]
            },
            options: {
                ...CHART_CONFIG,
                scales: {
                    x: {
                        display: true,
                        grid: { display: false }
                    },
                    y: {
                        display: true,
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: '金额 (元)'
                        }
                    }
                }
            }
        });
    },

    // Update all charts with new data
    updateAllCharts(dailyData, hourlyData) {
        if (dailyData && dailyData.length > 0) {
            this.createDailyReportChart(dailyData);
            this.createCOPTrendChart(dailyData);
            this.createEconomicChart(dailyData);
        }

        if (hourlyData && hourlyData.length > 0) {
            this.createSupplyCurveChart(hourlyData);
        }
    },

    // Destroy all charts
    destroyAllCharts() {
        Object.keys(this.charts).forEach(chartId => {
            this.destroyChart(chartId);
        });
    }
};

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ChartsManager;
}
