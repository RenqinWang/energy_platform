// Axios客户端配置

import axios from 'axios';

export type DataMode = 'full' | 'stream';

const DATA_MODE_KEY = 'energy-platform-data-mode';

export function getDataMode(): DataMode {
  if (typeof window === 'undefined') {
    return 'full';
  }
  const value = window.localStorage.getItem(DATA_MODE_KEY);
  return value === 'stream' ? 'stream' : 'full';
}

export function setDataMode(mode: DataMode) {
  if (typeof window !== 'undefined') {
    window.localStorage.setItem(DATA_MODE_KEY, mode);
  }
}

function getApiBaseUrl() {
  if (typeof window === 'undefined') {
    return '/api';
  }

  const mode = getDataMode();
  const port = mode === 'stream'
    ? import.meta.env.VITE_API_STREAM_PORT || '8002'
    : import.meta.env.VITE_API_FULL_PORT || '8001';

  return `${window.location.protocol}//${window.location.hostname}:${port}/api`;
}

const client = axios.create({
  baseURL: getApiBaseUrl(),
  timeout: 120000,
  headers: {
    'Content-Type': 'application/json'
  }
});

// 请求拦截器
client.interceptors.request.use(
  (config) => {
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// 响应拦截器
client.interceptors.response.use(
  (response) => {
    return response.data;
  },
  (error) => {
    console.error('API Error:', error);
    return Promise.reject(error);
  }
);

export default client;
