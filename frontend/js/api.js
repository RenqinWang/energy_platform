// API Service Module
const APIService = {
    // Show loading overlay
    showLoading() {
        document.getElementById('loadingOverlay').style.display = 'flex';
    },

    // Hide loading overlay
    hideLoading() {
        document.getElementById('loadingOverlay').style.display = 'none';
    },

    // Handle API errors
    handleError(error) {
        console.error('API Error:', error);
        this.hideLoading();

        let message = '请求失败';
        if (error.response) {
            message = `错误 ${error.response.status}: ${error.response.data.detail || '服务器错误'}`;
        } else if (error.request) {
            message = '无法连接到服务器，请检查API服务是否运行';
        } else {
            message = error.message;
        }

        alert(message);
        throw error;
    },

    // Health check
    async checkHealth() {
        try {
            const response = await axios.get(`${API_CONFIG.baseURL}${API_CONFIG.endpoints.health}`);
            return response.data;
        } catch (error) {
            this.handleError(error);
        }
    },

    // Get station list
    async getStations() {
        try {
            this.showLoading();
            const response = await axios.get(`${API_CONFIG.baseURL}${API_CONFIG.endpoints.stations}`);
            this.hideLoading();
            return response.data.stations;
        } catch (error) {
            this.handleError(error);
        }
    },

    // Get equipment list
    async getEquipment(stationId = null) {
        try {
            this.showLoading();
            const params = {};
            if (stationId) {
                params.station_id = stationId;
            }
            const response = await axios.get(
                `${API_CONFIG.baseURL}${API_CONFIG.endpoints.equipment}`,
                { params }
            );
            this.hideLoading();
            return response.data.equipment;
        } catch (error) {
            this.handleError(error);
        }
    },

    // Query supply curve data
    async getSupplyCurve(filters = {}) {
        try {
            this.showLoading();
            const params = {
                limit: filters.limit || API_CONFIG.defaultLimit
            };

            if (filters.stationId) params.station_id = filters.stationId;
            if (filters.equipmentId) params.equipment_id = filters.equipmentId;
            if (filters.startDate) params.start_date = filters.startDate;
            if (filters.endDate) params.end_date = filters.endDate;

            const response = await axios.get(
                `${API_CONFIG.baseURL}${API_CONFIG.endpoints.supplyCurve}`,
                { params }
            );
            this.hideLoading();
            return response.data;
        } catch (error) {
            this.handleError(error);
        }
    },

    // Query daily report data
    async getDailyReport(filters = {}) {
        try {
            this.showLoading();
            const params = {
                limit: filters.limit || API_CONFIG.defaultLimit
            };

            if (filters.stationId) params.station_id = filters.stationId;
            if (filters.equipmentId) params.equipment_id = filters.equipmentId;
            if (filters.startDate) params.start_date = filters.startDate;
            if (filters.endDate) params.end_date = filters.endDate;

            const response = await axios.get(
                `${API_CONFIG.baseURL}${API_CONFIG.endpoints.dailyReport}`,
                { params }
            );
            this.hideLoading();
            return response.data;
        } catch (error) {
            this.handleError(error);
        }
    },

    // Query equipment status data
    async getEquipmentStatus(filters = {}) {
        try {
            this.showLoading();
            const params = {
                limit: filters.limit || API_CONFIG.defaultLimit
            };

            if (filters.stationId) params.station_id = filters.stationId;
            if (filters.equipmentId) params.equipment_id = filters.equipmentId;
            if (filters.startTime) params.start_time = filters.startTime;
            if (filters.endTime) params.end_time = filters.endTime;

            const response = await axios.get(
                `${API_CONFIG.baseURL}${API_CONFIG.endpoints.equipmentStatus}`,
                { params }
            );
            this.hideLoading();
            return response.data;
        } catch (error) {
            this.handleError(error);
        }
    }
};

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = APIService;
}
