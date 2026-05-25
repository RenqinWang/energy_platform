import { Layout, Menu, Segmented, Typography } from 'antd';
import {
  AppstoreOutlined,
  BarChartOutlined,
  DashboardOutlined,
  DollarOutlined,
  FileTextOutlined,
  FundProjectionScreenOutlined,
  LineChartOutlined,
} from '@ant-design/icons';
import { useLocation, useNavigate } from 'react-router-dom';
import { getDataMode, setDataMode, type DataMode } from '../../api/client';

const { Header: AntHeader } = Layout;
const { Title } = Typography;

const menuItems = [
  { key: '/dashboard', icon: <AppstoreOutlined />, label: '监测大屏' },
  { key: '/system', icon: <FundProjectionScreenOutlined />, label: '系统级展示' },
  { key: '/device', icon: <DashboardOutlined />, label: '设备级查询' },
  { key: '/theme', icon: <BarChartOutlined />, label: '主题级查询' },
  { key: '/reports', icon: <FileTextOutlined />, label: '综合报表' },
  { key: '/forecast', icon: <LineChartOutlined />, label: '供能预测' },
  { key: '/revenue', icon: <DollarOutlined />, label: '收益预测' },
];

export default function Header() {
  const navigate = useNavigate();
  const location = useLocation();
  const dataMode = getDataMode();

  return (
    <AntHeader
      className="app-header-light sticky top-0 z-20 flex h-16 items-center gap-5 border-b border-slate-200 px-4 shadow-sm lg:px-6"
      style={{ background: '#ffffff', color: '#0f172a' }}
    >
      <div className="flex shrink-0 items-center gap-3">
        <div className="grid h-9 w-9 place-items-center rounded-lg bg-sky-500 text-white">
          <FundProjectionScreenOutlined />
        </div>
        <Title
          level={4}
          className="!m-0 !text-base !font-semibold !tracking-normal lg:!text-lg"
          style={{ color: '#0f172a' }}
        >
          智慧能源网监测平台
        </Title>
      </div>

      <Menu
        theme="light"
        mode="horizontal"
        selectedKeys={[location.pathname]}
        items={menuItems}
        onClick={({ key }) => navigate(key)}
        className="app-header-menu min-w-0 flex-1 border-0"
      />
      <Segmented
        className="app-data-mode-segmented"
        size="small"
        value={dataMode}
        options={[
          { label: '全量', value: 'full' },
          { label: '流式', value: 'stream' },
        ]}
        onChange={(value) => {
          setDataMode(value as DataMode);
          window.location.reload();
        }}
      />
    </AntHeader>
  );
}
