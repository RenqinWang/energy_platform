// Loading组件

import { Spin } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';

interface LoadingProps {
  tip?: string;
  size?: 'small' | 'default' | 'large';
}

export default function Loading({ tip = '加载中...', size = 'large' }: LoadingProps) {
  return (
    <div style={{
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      minHeight: '200px',
      width: '100%'
    }}>
      <Spin
        indicator={<LoadingOutlined style={{ fontSize: size === 'large' ? 48 : 24 }} spin />}
        tip={tip}
        size={size}
      />
    </div>
  );
}
