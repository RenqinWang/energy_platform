import { Layout as AntLayout } from 'antd';
import { Outlet, useLocation } from 'react-router-dom';
import Header from './Header';

const { Content } = AntLayout;

export default function Layout() {
  const location = useLocation();
  const isDashboard = location.pathname === '/dashboard';

  return (
    <AntLayout className="min-h-screen">
      <Header />
      <Content className={isDashboard ? 'bg-slate-100' : 'bg-slate-100 p-4 lg:p-6'}>
        <div className={isDashboard ? '' : 'mx-auto max-w-[1480px]'}>
          <Outlet />
        </div>
      </Content>
    </AntLayout>
  );
}
