import { Component, type ErrorInfo, type ReactNode } from 'react';
import { Button, Card, CardContent, CardHeader, CardTitle } from '@databricks/appkit-ui/react';

interface ErrorBoundaryProps {
  children: ReactNode;
}

interface ErrorBoundaryState {
  error: Error | null;
}

export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: null };

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('EuroStream App render failed', { error, componentStack: info.componentStack });
  }

  render() {
    if (this.state.error) {
      return (
        <div className="grid min-h-screen place-items-center bg-slate-50 p-6">
          <Card className="w-full max-w-xl border-rose-200 shadow-lg">
            <CardHeader>
              <CardTitle className="text-rose-700">The showcase could not render</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <p className="text-sm text-slate-600">
                The error was contained by the App boundary. Check the browser console and Databricks App deployment
                logs for details.
              </p>
              <pre className="max-h-48 overflow-auto rounded-lg bg-slate-950 p-3 text-xs text-slate-100">
                {this.state.error.message}
              </pre>
              <Button onClick={() => window.location.reload()}>Reload App</Button>
            </CardContent>
          </Card>
        </div>
      );
    }
    return this.props.children;
  }
}
