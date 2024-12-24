import { Boxes, CloudCog, Database, GitBranch, LockKeyhole, Workflow } from 'lucide-react';
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { DataState } from '@/components/DataState';
import { PageHeader } from '@/components/PageHeader';
import { formatNumber } from '@/lib/format';

const EMPTY_PARAMS: Record<string, never> = {};

export function OperationsPage() {
  const query = useAnalyticsQuery('operations_summary', EMPTY_PARAMS);
  const rows = query.data ?? [];

  return (
    <div className="space-y-8">
      <PageHeader
        eyebrow="Platform operations"
        title="Native control plane"
        description="The showcase uses Databricks services end to end. The existing local GitHub Actions, DuckDB, Turso, Render, and FastAPI workflow remains separate and is not a dependency."
      />

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {[
          {
            icon: GitBranch,
            title: 'Lakeflow ingestion',
            text: 'Kafka or Auto Loader feeds a continuous Lakeflow Job and creates governed Bronze Delta tables.',
          },
          {
            icon: Boxes,
            title: 'Medallion transforms',
            text: 'Silver and Gold pipelines apply expectations, suppression-aware logic, and consent gates.',
          },
          {
            icon: Workflow,
            title: 'Lakeflow Jobs',
            text: 'Task dependencies, continuous execution, parameters, retries, schedules, and Run Now are workspace-native.',
          },
          {
            icon: Database,
            title: 'Unity Catalog',
            text: 'Catalog, schemas, grants, tags, column masks, row filters, lineage, and governed Volumes.',
          },
          {
            icon: CloudCog,
            title: 'Databricks App',
            text: 'React 19, TypeScript, Vite, and AppKit run on the Apps serverless compute plane.',
          },
          {
            icon: LockKeyhole,
            title: 'Least privilege',
            text: 'OBO reads respect each user; the App service principal can run only the erasure Job resource.',
          },
        ].map(({ icon: Icon, title, text }) => (
          <Card key={title} className="border-slate-200 shadow-sm">
            <CardContent className="flex gap-4 p-5">
              <span className="grid size-10 shrink-0 place-items-center rounded-xl bg-slate-950 text-white">
                <Icon className="size-5" />
              </span>
              <div>
                <h2 className="font-semibold text-slate-900">{title}</h2>
                <p className="mt-1 text-sm leading-6 text-slate-600">{text}</p>
              </div>
            </CardContent>
          </Card>
        ))}
      </section>

      <Card className="border-slate-200 shadow-sm">
        <CardHeader>
          <CardTitle>Governed table inventory</CardTitle>
        </CardHeader>
        <CardContent>
          <DataState loading={query.loading} error={query.error} empty={rows.length === 0}>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Unity Catalog table</TableHead>
                  <TableHead className="text-right">Current rows</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((row) => (
                  <TableRow key={row.table_name}>
                    <TableCell className="font-mono text-xs font-semibold text-sky-700">{row.table_name}</TableCell>
                    <TableCell className="text-right font-mono">{formatNumber(row.row_count)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </DataState>
        </CardContent>
      </Card>
    </div>
  );
}
