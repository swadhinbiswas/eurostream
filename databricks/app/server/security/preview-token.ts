import { createHmac, randomUUID, timingSafeEqual } from 'node:crypto';

export interface ErasurePreviewPayload {
  version: 1;
  customerId: string;
  ticketId: string;
  user: string;
  nonce: string;
  expiresAt: number;
}

function encode(value: string): string {
  return Buffer.from(value, 'utf8').toString('base64url');
}

function decode(value: string): string {
  return Buffer.from(value, 'base64url').toString('utf8');
}

function signature(payload: string, secret: string): string {
  return createHmac('sha256', secret).update(payload).digest('base64url');
}

export function issuePreviewToken(
  input: Omit<ErasurePreviewPayload, 'version' | 'nonce' | 'expiresAt'>,
  secret: string,
  ttlSeconds: number,
  nowMs = Date.now()
): { token: string; expiresAt: string } {
  const expiresAt = Math.floor(nowMs / 1000) + ttlSeconds;
  const payload: ErasurePreviewPayload = {
    version: 1,
    ...input,
    nonce: randomUUID(),
    expiresAt,
  };
  const encodedPayload = encode(JSON.stringify(payload));
  return {
    token: `${encodedPayload}.${signature(encodedPayload, secret)}`,
    expiresAt: new Date(expiresAt * 1000).toISOString(),
  };
}

export type PreviewVerification =
  | { ok: true; payload: ErasurePreviewPayload }
  | { ok: false; reason: 'invalid' | 'expired' };

export function verifyPreviewToken(token: string, secret: string, nowMs = Date.now()): PreviewVerification {
  const [encodedPayload, suppliedSignature, ...rest] = token.split('.');
  if (!encodedPayload || !suppliedSignature || rest.length > 0) return { ok: false, reason: 'invalid' };

  const expectedSignature = signature(encodedPayload, secret);
  const supplied = Buffer.from(suppliedSignature, 'utf8');
  const expected = Buffer.from(expectedSignature, 'utf8');
  if (supplied.length !== expected.length || !timingSafeEqual(supplied, expected)) {
    return { ok: false, reason: 'invalid' };
  }

  let payload: ErasurePreviewPayload;
  try {
    payload = JSON.parse(decode(encodedPayload)) as ErasurePreviewPayload;
  } catch {
    return { ok: false, reason: 'invalid' };
  }

  if (
    payload.version !== 1 ||
    typeof payload.customerId !== 'string' ||
    typeof payload.ticketId !== 'string' ||
    typeof payload.user !== 'string' ||
    typeof payload.nonce !== 'string' ||
    typeof payload.expiresAt !== 'number'
  ) {
    return { ok: false, reason: 'invalid' };
  }

  if (payload.expiresAt <= Math.floor(nowMs / 1000)) {
    return { ok: false, reason: 'expired' };
  }

  return { ok: true, payload };
}

export function isSameSiteBrowserRequest(secFetchSite: string | undefined): boolean {
  return secFetchSite === undefined || ['same-origin', 'same-site', 'none'].includes(secFetchSite);
}
