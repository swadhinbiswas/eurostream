import type { ApiErrorResponse } from '../../../shared/contracts';

async function parseResponse<T extends object>(response: Response): Promise<T> {
  const data = (await response.json().catch(() => ({}))) as T | ApiErrorResponse;
  if (!response.ok) {
    const message =
      'error' in data && typeof data.error === 'string' ? data.error : `Request failed (${response.status})`;
    throw new Error(message);
  }
  return data as T;
}

export async function postJson<T extends object>(url: string, body: unknown): Promise<T> {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return parseResponse<T>(response);
}

export async function getJson<T extends object>(url: string): Promise<T> {
  const response = await fetch(url, { headers: { accept: 'application/json' } });
  return parseResponse<T>(response);
}
