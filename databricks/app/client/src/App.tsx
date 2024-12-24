import { lazy, Suspense, useState } from 'react';
import { createBrowserRouter, NavLink, Outlet, RouterProvider } from 'react-router';
import { Button, Sheet, SheetContent, SheetHeader, SheetTitle, useIsMobile } from '@databricks/appkit-ui/react';
import { Activity, Gauge, LoaderCircle, Menu, Scale, ServerCog, ShieldCheck, UsersRound } from 'lucide-react';
import { NotFoundPage } from '@/pages/NotFoundPage';
import { OverviewPage } from '@/pages/OverviewPage';
import { cn } from '@/lib/utils';

const CustomersPage = lazy(() => import('@/pages/CustomersPage').then((module) => ({ default: module.CustomersPage })));
const ErasurePage = lazy(() => import('@/pages/ErasurePage').then((module) => ({ default: module.ErasurePage })));
const FraudPage = lazy(() => import('@/pages/FraudPage').then((module) => ({ default: module.FraudPage })));
const GovernancePage = lazy(() =>
  import('@/pages/GovernancePage').then((module) => ({ default: module.GovernancePage }))
);
const OperationsPage = lazy(() =>
  import('@/pages/OperationsPage').then((module) => ({ default: module.OperationsPage }))
);

const navigation = [
  { to: '/', label: 'Overview', icon: Gauge, end: true },
  { to: '/customers', label: 'Customers', icon: UsersRound, end: false },
  { to: '/fraud', label: 'Fraud', icon: Activity, end: false },
  { to: '/governance', label: 'Governance', icon: ShieldCheck, end: false },
  { to: '/erasure', label: 'Article 17', icon: Scale, end: false },
  { to: '/operations', label: 'Operations', icon: ServerCog, end: false },
];

function navClass(isActive: boolean): string {
  return cn(
    'inline-flex items-center gap-2 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
    isActive ? 'bg-slate-950 text-white shadow-sm' : 'text-slate-600 hover:bg-slate-100 hover:text-slate-950'
  );
}

function Navigation({ onNavigate }: { onNavigate?: () => void }) {
  return (
    <nav className="flex items-center gap-1 overflow-x-auto" aria-label="Primary navigation">
      {navigation.map(({ to, label, icon: Icon, end }) => (
        <NavLink key={to} to={to} end={end} className={({ isActive }) => navClass(isActive)} onClick={onNavigate}>
          <Icon className="size-4" />
          <span className="whitespace-nowrap">{label}</span>
        </NavLink>
      ))}
    </nav>
  );
}

function Layout() {
  const isMobile = useIsMobile();
  const [mobileNavOpen, setMobileNavOpen] = useState(false);

  return (
    <div className="min-h-screen bg-slate-50 text-slate-950">
      <header className="sticky top-0 z-40 border-b border-slate-200 bg-white/90 backdrop-blur-xl">
        <div className="mx-auto flex max-w-[1500px] items-center gap-4 px-4 py-3 md:px-8">
          <div className="flex min-w-fit items-center gap-3">
            <span className="grid size-10 place-items-center rounded-xl bg-gradient-to-br from-sky-600 via-blue-700 to-violet-700 text-sm font-black tracking-tight text-white shadow-lg shadow-blue-700/15">
              ES
            </span>
            <div>
              <p className="font-bold tracking-tight text-slate-950">EuroStream</p>
              <p className="text-[11px] font-medium text-slate-500">Databricks-native showcase</p>
            </div>
          </div>
          <div className="ml-4 hidden flex-1 lg:block">
            <Navigation />
          </div>
          <span className="ml-auto hidden rounded-full border border-blue-200 bg-blue-50 px-3 py-1 text-[11px] font-semibold text-blue-700 md:inline-flex">
            Unity Catalog · Delta · Lakeflow · AppKit
          </span>
          <div className="ml-auto lg:hidden">
            <Button variant="outline" size="icon" onClick={() => setMobileNavOpen(true)} aria-label="Open navigation">
              <Menu className="size-5" />
            </Button>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-[1500px] px-4 py-8 md:px-8 md:py-10">
        <Suspense
          fallback={
            <div className="grid min-h-[50vh] place-items-center text-sm text-slate-500">
              <LoaderCircle className="mr-2 inline size-4 animate-spin" /> Loading view…
            </div>
          }
        >
          <Outlet />
        </Suspense>
      </main>

      <footer className="border-t border-slate-200 bg-white">
        <div className="mx-auto flex max-w-[1500px] flex-col gap-2 px-4 py-6 text-xs text-slate-500 md:flex-row md:items-center md:justify-between md:px-8">
          <p>Independent portfolio implementation. The existing local EuroStream workflow remains unchanged.</p>
          <p className="font-mono">OBO reads · no Bronze access · least-privilege job action</p>
        </div>
      </footer>

      <Sheet open={mobileNavOpen && isMobile} onOpenChange={setMobileNavOpen}>
        <SheetContent side="left">
          <SheetHeader>
            <SheetTitle>EuroStream navigation</SheetTitle>
          </SheetHeader>
          <div className="mt-4 flex flex-col gap-1">
            <Navigation onNavigate={() => setMobileNavOpen(false)} />
          </div>
        </SheetContent>
      </Sheet>
    </div>
  );
}

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: '/', element: <OverviewPage /> },
      { path: '/customers', element: <CustomersPage /> },
      { path: '/fraud', element: <FraudPage /> },
      { path: '/governance', element: <GovernancePage /> },
      { path: '/erasure', element: <ErasurePage /> },
      { path: '/operations', element: <OperationsPage /> },
      { path: '*', element: <NotFoundPage /> },
    ],
  },
]);

export default function App() {
  return <RouterProvider router={router} />;
}
