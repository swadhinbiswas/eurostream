import { useEffect, useMemo, useState } from 'react';
import { AlertTriangle, CheckCircle2, ExternalLink, KeyRound, LoaderCircle, ShieldCheck } from 'lucide-react';
import {
  Alert,
  AlertDescription,
  AlertTitle,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Input,
  Label,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { sql } from '@databricks/appkit-ui/js';
import { DataState, InlineSpinner } from '@/components/DataState';
import { PageHeader } from '@/components/PageHeader';
import { StatusBadge } from '@/components/StatusBadge';
import { getJson, postJson } from '@/lib/api';
import { formatDateTime, formatNumber, formatSeconds } from '@/lib/format';
import type { ErasureRunResponse, ErasureSubmissionResponse, PreviewResponse } from '../../../shared/contracts';

const CUSTOMER_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$/;
const TICKET_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:/-]{2,127}$/;

interface PreparedRequest {
  customerId: string;
  ticketId: string;
  nonce: number;
}

export function ErasurePage() {
  const [customerId, setCustomerId] = useState('');
  const [ticketId, setTicketId] = useState('PORTFOLIO-DEMO-001');
  const [prepared, setPrepared] = useState<PreparedRequest | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);

  const prepare = () => {
    if (!CUSTOMER_ID_PATTERN.test(customerId)) {
      setValidationError('Use 3–128 letters, numbers, dots, underscores, colons, or hyphens.');
      return;
    }
    if (!TICKET_ID_PATTERN.test(ticketId)) {
      setValidationError('Enter a valid DSAR or showcase ticket ID.');
      return;
    }
    setValidationError(null);
    setPrepared({ customerId, ticketId, nonce: Date.now() });
  };

  return (
    <div className="space-y-8">
      <PageHeader
        eyebrow="Fail-closed governance"
        title="Article 17 erasure console"
        description="Prepare a governed impact preview, issue a short-lived user-bound confirmation, then queue the parameterized Databricks erasure Job. The App never executes Spark or SQL mutations itself."
      />

      <Alert>
        <ShieldCheck className="size-4" />
        <AlertTitle>Databricks-native boundary</AlertTitle>
        <AlertDescription>
          The 60-second target is an internal platform SLO, not a universal statutory deadline. Logical, export, and
          fraud-sink physical-file evidence remain separate.
        </AlertDescription>
      </Alert>

      <Card className="border-slate-200 shadow-sm">
        <CardHeader>
          <CardTitle>1. Identify the governed record</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="customer-id">Customer ID</Label>
              <Input
                id="customer-id"
                value={customerId}
                onChange={(event) => setCustomerId(event.target.value)}
                placeholder="cust_424242"
                autoComplete="off"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="ticket-id">DSAR / showcase ticket</Label>
              <Input
                id="ticket-id"
                value={ticketId}
                onChange={(event) => setTicketId(event.target.value)}
                placeholder="DSAR-2026-00123"
                autoComplete="off"
              />
            </div>
          </div>
          {validationError && <p className="text-sm text-rose-700">{validationError}</p>}
          <Button onClick={prepare} disabled={!customerId || !ticketId}>
            Check governed impact
          </Button>
        </CardContent>
      </Card>

      {prepared && <PreparedErasure key={prepared.nonce} prepared={prepared} />}
    </div>
  );
}

function PreparedErasure({ prepared }: { prepared: PreparedRequest }) {
  const impactParameters = useMemo(() => ({ customerId: sql.string(prepared.customerId) }), [prepared.customerId]);
  const impactQuery = useAnalyticsQuery('erasure_impact', impactParameters);
  const impact = impactQuery.data?.[0];
  const [preview, setPreview] = useState<PreviewResponse | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [confirmation, setConfirmation] = useState('');
  const [idempotencyKey] = useState(() => crypto.randomUUID());
  const [submission, setSubmission] = useState<ErasureSubmissionResponse | null>(null);
  const [submissionError, setSubmissionError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [run, setRun] = useState<ErasureRunResponse | null>(null);

  const totalRows = impact
    ? impact.silver_customer_rows +
      impact.silver_order_rows +
      impact.silver_payment_rows +
      impact.silver_order_quarantine_rows +
      impact.silver_payment_quarantine_rows +
      impact.gold_customer_rows +
      impact.gold_order_rows +
      impact.fraud_summary_rows
    : 0;

  const createPreview = async () => {
    setPreviewLoading(true);
    setPreviewError(null);
    try {
      const response = await postJson<PreviewResponse>('/api/erasure-actions/previews', {
        customerId: prepared.customerId,
        ticketId: prepared.ticketId,
      });
      setPreview(response);
    } catch (error) {
      setPreviewError(error instanceof Error ? error.message : 'Could not create preview');
    } finally {
      setPreviewLoading(false);
    }
  };

  const submit = async () => {
    if (!preview) return;
    setSubmitting(true);
    setSubmissionError(null);
    try {
      const response = await postJson<ErasureSubmissionResponse>('/api/erasure-actions/requests', {
        previewToken: preview.token,
        customerId: prepared.customerId,
        confirmation,
        idempotencyKey,
      });
      setSubmission(response);
    } catch (error) {
      setSubmissionError(error instanceof Error ? error.message : 'Could not submit erasure request');
    } finally {
      setSubmitting(false);
    }
  };

  useEffect(() => {
    if (!submission) return;
    let active = true;
    let timer: number | undefined;
    const poll = async () => {
      try {
        const response = await getJson<ErasureRunResponse>(submission.pollUrl);
        if (active) setRun(response);
        if (active && !response.terminal) {
          timer = window.setTimeout(() => void poll(), 3_000);
        }
      } catch (error) {
        if (active) setSubmissionError(error instanceof Error ? error.message : 'Could not read run status');
      }
    };
    void poll();
    return () => {
      active = false;
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [submission]);

  return (
    <div className="space-y-6">
      <Card className="border-slate-200 shadow-sm">
        <CardHeader>
          <CardTitle>2. Review governed impact</CardTitle>
        </CardHeader>
        <CardContent>
          <DataState loading={impactQuery.loading} error={impactQuery.error} empty={!impact}>
            {impact && (
              <div className="space-y-5">
                {impact.already_suppressed && (
                  <Alert variant="destructive">
                    <AlertTriangle className="size-4" />
                    <AlertTitle>Customer is already suppressed</AlertTitle>
                    <AlertDescription>
                      Suppression already exists. A new confirmation will verify and reconcile the governed state; it
                      will never unsuppress or restore the customer.
                    </AlertDescription>
                  </Alert>
                )}
                <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                  {[
                    ['Silver customers', impact.silver_customer_rows],
                    ['Silver orders', impact.silver_order_rows],
                    ['Silver payments', impact.silver_payment_rows],
                    ['Quarantine rows', impact.silver_order_quarantine_rows + impact.silver_payment_quarantine_rows],
                    ['Gold customer rows', impact.gold_customer_rows],
                    ['Gold fact rows', impact.gold_order_rows],
                    ['Fraud summaries', impact.fraud_summary_rows],
                    ['Suppressed already', impact.already_suppressed ? 'Yes' : 'No'],
                  ].map(([label, value]) => (
                    <div key={String(label)} className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                      <p className="text-xs text-slate-500">{label}</p>
                      <p className="mt-1 font-mono text-lg font-bold text-slate-900">
                        {typeof value === 'number' ? formatNumber(value) : value}
                      </p>
                    </div>
                  ))}
                </div>
                <p className="text-sm text-slate-500">
                  {formatNumber(totalRows)} non-Bronze rows are currently associated with this customer. The App does
                  not count or expose raw Bronze PII.
                </p>
                {!preview && (
                  <Button onClick={() => void createPreview()} disabled={previewLoading}>
                    {previewLoading ? (
                      <LoaderCircle className="mr-2 size-4 animate-spin" />
                    ) : (
                      <KeyRound className="mr-2 size-4" />
                    )}
                    Lock this preview for 10 minutes
                  </Button>
                )}
                {previewError && <p className="text-sm text-rose-700">{previewError}</p>}
              </div>
            )}
          </DataState>
        </CardContent>
      </Card>

      {preview && !submission && (
        <Card className="border-amber-200 bg-amber-50/40 shadow-sm">
          <CardHeader>
            <CardTitle>3. Confirm and queue the Databricks Job</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-amber-900">
              Preview expires {formatDateTime(preview.expiresAt)}. Type <strong>{prepared.customerId}</strong> exactly
              to continue.
            </p>
            <div className="space-y-2">
              <Label htmlFor="confirmation">Type customer ID</Label>
              <Input
                id="confirmation"
                value={confirmation}
                onChange={(event) => setConfirmation(event.target.value)}
                autoComplete="off"
                placeholder={prepared.customerId}
              />
            </div>
            <Button
              variant="destructive"
              onClick={() => void submit()}
              disabled={submitting || confirmation !== prepared.customerId}
            >
              {submitting ? (
                <LoaderCircle className="mr-2 size-4 animate-spin" />
              ) : (
                <ShieldCheck className="mr-2 size-4" />
              )}
              Queue governed erasure
            </Button>
            {submissionError && <p className="text-sm text-rose-700">{submissionError}</p>}
          </CardContent>
        </Card>
      )}

      {submission && (
        <Card className="border-sky-200 bg-sky-50/40 shadow-sm">
          <CardHeader>
            <CardTitle>4. Workflow status</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid gap-3 sm:grid-cols-3">
              <div>
                <p className="text-xs text-slate-500">Request ID</p>
                <p className="break-all font-mono text-xs font-semibold">{submission.requestId}</p>
              </div>
              <div>
                <p className="text-xs text-slate-500">Workflow run</p>
                <p className="font-mono text-sm font-semibold">#{submission.runId}</p>
              </div>
              <div>
                <p className="text-xs text-slate-500">Status</p>
                <StatusBadge value={run?.lifeCycleState ?? 'SUBMITTED'} />
              </div>
            </div>
            {!run?.terminal ? (
              <InlineSpinner label="Polling the Databricks Jobs run…" />
            ) : (
              <div className="flex items-center gap-2 text-sm">
                {run.resultState === 'SUCCESS' ? (
                  <CheckCircle2 className="size-5 text-emerald-600" />
                ) : (
                  <AlertTriangle className="size-5 text-rose-600" />
                )}
                Result: <StatusBadge value={run.resultState} />
                {run.startTime && run.endTime && (
                  <span className="text-slate-500">{formatSeconds((run.endTime - run.startTime) / 1000)}</span>
                )}
              </div>
            )}
            {run?.runPageUrl && (
              <a
                href={run.runPageUrl}
                target="_blank"
                rel="noreferrer"
                className="inline-flex h-9 w-fit items-center justify-center rounded-md border border-slate-300 bg-white px-4 text-sm font-medium text-slate-900 transition hover:bg-slate-50"
              >
                Open Workflow run <ExternalLink className="ml-2 size-4" />
              </a>
            )}
            {submissionError && <p className="text-sm text-rose-700">{submissionError}</p>}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
