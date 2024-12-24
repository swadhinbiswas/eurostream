import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { ResourceStatusIndicator, ResourceStatusProvider } from '@databricks/appkit-ui/react';
import App from './App';
import { ErrorBoundary } from './ErrorBoundary';
import './index.css';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ErrorBoundary>
      <ResourceStatusProvider>
        <ResourceStatusIndicator />
        <App />
      </ResourceStatusProvider>
    </ErrorBoundary>
  </StrictMode>
);
