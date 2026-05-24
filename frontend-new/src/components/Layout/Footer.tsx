// Layout Footer组件
import { Layout, Typography } from 'antd';

const { Footer: AntFooter } = Layout;
const { Text } = Typography;

export default function Footer() {
  return (
    <AntFooter style={{ textAlign: 'center', background: '#f0f2f5' }}>
      <Text type="secondary">
        智慧能源网监测平台 ©2026 | 基于 React + Vite + TypeScript + Ant Design + ECharts
      </Text>
    </AntFooter>
  );
}
