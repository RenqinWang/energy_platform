// API Configuration
const API_CONFIG = {
    baseURL: 'http://localhost:8000',
    endpoints: {
        health: '/health',
        stations: '/api/stations',
        equipment: '/api/equipment',
        supplyCurve: '/api/supply-curve',
        dailyReport: '/api/daily-report',
        equipmentStatus: '/api/equipment-status'
    },
    defaultLimit: 1000
};

// Chart Configuration
const CHART_CONFIG = {
    responsive: true,
    maintainAspectRatio: true,
    plugins: {
        legend: {
            display: true,
            position: 'top'
        },
        tooltip: {
            mode: 'index',
            intersect: false
        }
    },
    scales: {
        x: {
            display: true,
            grid: {
                display: false
            }
        },
        y: {
            display: true,
            beginAtZero: true
        }
    }
};

// Date Utilities
const DateUtils = {
    // Get date N days ago
    getDaysAgo(days) {
        const date = new Date();
        date.setDate(date.getDate() - days);
        return date.toISOString().split('T')[0];
    },

    // Get today's date
    getToday() {
        return new Date().toISOString().split('T')[0];
    },

    // Format date for display
    formatDate(dateStr) {
        if (!dateStr) return '--';
        return dateStr.split('T')[0];
    },

    // Format datetime for display
    formatDateTime(dateTimeStr) {
        if (!dateTimeStr) return '--';
        return dateTimeStr.replace('T', ' ').split('.')[0];
    },

    // Get current timestamp
    getCurrentTimestamp() {
        return new Date().toLocaleString('zh-CN', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
    }
};

// Number Utilities
const NumberUtils = {
    // Format number with thousand separators
    formatNumber(num, decimals = 2) {
        if (num === null || num === undefined || isNaN(num)) return '--';
        return Number(num).toLocaleString('zh-CN', {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals
        });
    },

    // Format as percentage
    formatPercent(num, decimals = 1) {
        if (num === null || num === undefined || isNaN(num)) return '--';
        return Number(num).toFixed(decimals) + '%';
    },

    // Format as currency (CNY)
    formatCurrency(num, decimals = 2) {
        if (num === null || num === undefined || isNaN(num)) return '--';
        return '¥' + Number(num).toLocaleString('zh-CN', {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals
        });
    }
};

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { API_CONFIG, CHART_CONFIG, DateUtils, NumberUtils };
}
