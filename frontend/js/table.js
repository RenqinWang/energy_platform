// Table Module
const TableManager = {
    currentView: 'daily', // 'daily' or 'hourly'
    currentData: null,

    // Initialize table view
    init() {
        document.getElementById('showDailyBtn').addEventListener('click', () => {
            this.switchView('daily');
        });

        document.getElementById('showHourlyBtn').addEventListener('click', () => {
            this.switchView('hourly');
        });

        document.getElementById('exportBtn').addEventListener('click', () => {
            this.exportToCSV();
        });
    },

    // Switch between daily and hourly views
    switchView(view) {
        this.currentView = view;

        // Update button states
        document.getElementById('showDailyBtn').classList.toggle('active', view === 'daily');
        document.getElementById('showHourlyBtn').classList.toggle('active', view === 'hourly');

        // Re-render table with current data
        if (this.currentData) {
            if (view === 'daily') {
                this.renderDailyTable(this.currentData.daily);
            } else {
                this.renderHourlyTable(this.currentData.hourly);
            }
        }
    },

    // Render daily report table
    renderDailyTable(data) {
        const thead = document.getElementById('tableHead');
        const tbody = document.getElementById('tableBody');

        // Clear existing content
        thead.innerHTML = '';
        tbody.innerHTML = '';

        if (!data || data.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align: center;">暂无数据</td></tr>';
            return;
        }

        // Create header - 只显示有数据的列
        const headerRow = document.createElement('tr');
        const headers = [
            '日期', '站点', '设备', '平均温度(°C)',
            '运行时长(分钟)', '运行率(%)', '记录数'
        ];
        headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = header;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);

        // Create rows - 只显示有数据的列
        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${DateUtils.formatDate(row.stat_date)}</td>
                <td>${row.station_id || '--'}</td>
                <td>${row.equipment_id || '--'}</td>
                <td>${NumberUtils.formatNumber(row.avg_supply_temp, 1)}</td>
                <td>${NumberUtils.formatNumber(row.total_run_minutes, 0)}</td>
                <td>${NumberUtils.formatPercent(row.daily_operation_rate, 1)}</td>
                <td>${row.hour_count || '--'}</td>
            `;
            tbody.appendChild(tr);
        });

        // 添加数据说明
        const noteRow = document.createElement('tr');
        noteRow.innerHTML = `
            <td colspan="7" style="background-color: #fff3cd; padding: 10px; text-align: left; font-size: 0.9em;">
                <strong>📝 数据说明：</strong>当前冷机历史数据已有出回水温度、流量和运行状态，
                但功率字段仍缺失，因此能耗、COP、成本和收益等依赖功率的指标暂时无法显示。
                完整数据接入后，这些指标将自动计算并显示。
            </td>
        `;
        tbody.appendChild(noteRow);
    },

    // Render hourly supply curve table
    renderHourlyTable(data) {
        const thead = document.getElementById('tableHead');
        const tbody = document.getElementById('tableBody');

        // Clear existing content
        thead.innerHTML = '';
        tbody.innerHTML = '';

        if (!data || data.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align: center;">暂无数据</td></tr>';
            return;
        }

        // Create header
        const headerRow = document.createElement('tr');
        const headers = [
            '时间', '站点', '设备', '平均温度(°C)', '最高温度(°C)',
            '最低温度(°C)', '平均功率(kW)', '运行时长(分钟)', '能耗(kWh)',
            '供冷量(kWh)', '运行率(%)'
        ];
        headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = header;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);

        // Create rows (show last 100 records)
        const displayData = data.slice(-100);
        displayData.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${DateUtils.formatDateTime(row.stat_hour)}</td>
                <td>${row.station_id || '--'}</td>
                <td>${row.equipment_id || '--'}</td>
                <td>${NumberUtils.formatNumber(row.avg_supply_temp, 1)}</td>
                <td>${NumberUtils.formatNumber(row.max_supply_temp, 1)}</td>
                <td>${NumberUtils.formatNumber(row.min_supply_temp, 1)}</td>
                <td>${NumberUtils.formatNumber(row.avg_power, 2)}</td>
                <td>${NumberUtils.formatNumber(row.run_minutes, 0)}</td>
                <td>${NumberUtils.formatNumber(row.energy_consumption_kwh, 2)}</td>
                <td>${NumberUtils.formatNumber(row.cooling_supply_kwh, 2)}</td>
                <td>${NumberUtils.formatPercent(row.operation_rate, 1)}</td>
            `;
            tbody.appendChild(tr);
        });
    },

    // Update table with new data
    updateTable(dailyData, hourlyData) {
        this.currentData = {
            daily: dailyData,
            hourly: hourlyData
        };

        if (this.currentView === 'daily') {
            this.renderDailyTable(dailyData);
        } else {
            this.renderHourlyTable(hourlyData);
        }
    },

    // Export current table to CSV
    exportToCSV() {
        if (!this.currentData) {
            alert('没有可导出的数据');
            return;
        }

        const data = this.currentView === 'daily' ? this.currentData.daily : this.currentData.hourly;
        if (!data || data.length === 0) {
            alert('没有可导出的数据');
            return;
        }

        let csv = '';
        let filename = '';

        if (this.currentView === 'daily') {
            // Daily report CSV
            filename = `daily_report_${DateUtils.getToday()}.csv`;
            csv = '日期,站点,设备,平均温度(°C),总能耗(kWh),总供冷量(kWh),运行时长(分钟),运行率(%),COP,能源成本(元),供冷收入(元),净利润(元)\n';

            data.forEach(row => {
                csv += [
                    DateUtils.formatDate(row.stat_date),
                    row.station_id,
                    row.equipment_id,
                    row.avg_supply_temp || '',
                    row.total_energy_consumption_kwh || '',
                    row.total_cooling_supply_kwh || '',
                    row.total_run_minutes || '',
                    row.daily_operation_rate || '',
                    row.avg_cop || '',
                    row.energy_cost || '',
                    row.cooling_revenue || '',
                    row.net_profit || ''
                ].join(',') + '\n';
            });
        } else {
            // Hourly supply curve CSV
            filename = `supply_curve_${DateUtils.getToday()}.csv`;
            csv = '时间,站点,设备,平均温度(°C),最高温度(°C),最低温度(°C),平均功率(kW),运行时长(分钟),能耗(kWh),供冷量(kWh),运行率(%)\n';

            data.forEach(row => {
                csv += [
                    DateUtils.formatDateTime(row.stat_hour),
                    row.station_id,
                    row.equipment_id,
                    row.avg_supply_temp || '',
                    row.max_supply_temp || '',
                    row.min_supply_temp || '',
                    row.avg_power || '',
                    row.run_minutes || '',
                    row.energy_consumption_kwh || '',
                    row.cooling_supply_kwh || '',
                    row.operation_rate || ''
                ].join(',') + '\n';
            });
        }

        // Create download link
        const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);

        link.setAttribute('href', url);
        link.setAttribute('download', filename);
        link.style.visibility = 'hidden';

        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }
};

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = TableManager;
}
