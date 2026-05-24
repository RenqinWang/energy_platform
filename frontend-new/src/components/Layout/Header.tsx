// Layout Header组件
import { Layout, Menu, Space, Typography } from 'antd';
import {
  DashboardOutlined,
  LineChartOutlined,
  BarChartOutlined,
  FundProjectionScreenOutlined,
  FileTextOutlined,
  DollarOutlined
} from '@ant-design/icons';
import { useNavigate, useLocation } from 'react-router-dom';

const { Header: AntHeader } = Layout;
const { Title } = Typography;

export default function Header() {
  const navigate = useNavigate();
  const location = useLocation();

  const menuItems = [
    {
      key: '/device',
      icon: <DashboardOutlined />,
      label: '设备级查询'
    },
    {
      key: '/theme',
      icon: <BarChartOutlined />,
      label: '主题级查询'
    },
    {
      key: '/system',
      icon: <FundProjectionScreenOutlined />,
      label: '系统级展示'
    },
    {
      key: '/forecast',
      icon: <LineChartOutlined />,
      label: '供能预测'
    },
    {
      key: '/reports',
      icon: <FileTextOutlined />,
      label: '综合报表'
    },
    {
      key: '/revenue',
      icon: <DollarOutlined />,
      label: '收益预测'
    }
  ];

  return (
    <AntHeader style={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      background: '#001529',
      padding: '0 24px'
    }}>
      <Space>
        <Title level={4} style={{ color: 'white', margin: 0 }}>
          🏭 智慧能源网监测平台
        </Title>
      </Space>

      <Menu
        theme="dark"
        mode="horizontal"
        selectedKeys={[location.pathname]}
        items={menuItems}
        onClick={({ key }) => navigate(key)}
        style={{ flex: 1, minWidth: 0, justifyContent: 'flex-end' }}
      />
    </AntHeader>
  );
}
