export interface PreviewResponse {
  token: string;
  expiresAt: string;
  confirmationText: string;
}

export interface ErasureSubmissionResponse {
  requestId: string;
  runId: number;
  status: 'SUBMITTED';
  requestedBy: string;
  pollUrl: string;
}

export interface ErasureRunResponse {
  runId: number;
  runName: string | null;
  lifeCycleState: string;
  resultState: string;
  terminal: boolean;
  startTime: number | null;
  endTime: number | null;
  runPageUrl: string | null;
}

export interface ApiErrorResponse {
  error: string;
}
