import { Link } from 'react-router';
import { ArrowLeft } from 'lucide-react';

export function NotFoundPage() {
  return (
    <div className="grid min-h-[60vh] place-items-center">
      <div className="max-w-md space-y-4 text-center">
        <p className="font-mono text-sm font-semibold text-sky-600">404</p>
        <h1 className="text-3xl font-bold text-slate-950">Page not found</h1>
        <p className="text-slate-600">Choose a governed showcase view from the navigation.</p>
        <Link
          to="/"
          className="inline-flex h-9 items-center justify-center rounded-md bg-slate-950 px-4 text-sm font-medium text-white"
        >
          <ArrowLeft className="mr-2 size-4" /> Back to overview
        </Link>
      </div>
    </div>
  );
}
