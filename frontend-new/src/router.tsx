// 路由配置
import { createBrowserRouter, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import ErrorBoundary from './components/Common/ErrorBoundary';
import DeviceQueryPage from './pages/DeviceQuery';
import ThemeQueryPage from './pages/ThemeQuery';
import SystemOverviewPage from './pages/SystemOverview';
import ForecastPage from './pages/Forecast';
import ComprehensiveReportPage from './pages/ComprehensiveReport';
import RevenueForecastPage from './pages/RevenueForecast';

const router = createBrowserRouter([
  {
    path: '/',
    element: (
      <ErrorBoundary>
        <Layout />
      </ErrorBoundary>
    ),
    children: [
      {
        index: true,
        element: <Navigate to="/system" replace />
      },
      {
        path: 'device',
        element: <DeviceQueryPage />
      },
      {
        path: 'theme',
        element: <ThemeQueryPage />
      },
      {
        path: 'system',
        element: <SystemOverviewPage />
      },
      {
        path: 'forecast',
        element: <ForecastPage />
      },
      {
        path: 'reports',
        element: <ComprehensiveReportPage />
      },
      {
        path: 'revenue',
        element: <RevenueForecastPage />
      }
    ]
  }
]);

export default router;
