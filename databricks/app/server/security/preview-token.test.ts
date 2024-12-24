import { describe, expect, test } from 'vitest';
import { isSameSiteBrowserRequest, issuePreviewToken, verifyPreviewToken } from './preview-token';

const secret = 'a-secure-preview-secret-with-at-least-32-characters';

describe('erasure preview token', () => {
  test('round-trips a user-bound preview', () => {
    const now = Date.UTC(2026, 8, 25, 10, 0, 0);
    const { token, expiresAt } = issuePreviewToken(
      { customerId: 'cust_123', ticketId: 'DSAR-2026-001', user: 'dpo@example.com' },
      secret,
      600,
      now
    );

    expect(verifyPreviewToken(token, secret, now + 1_000)).toMatchObject({
      ok: true,
      payload: { customerId: 'cust_123', ticketId: 'DSAR-2026-001', user: 'dpo@example.com' },
    });
    expect(expiresAt).toBe('2026-09-25T10:10:00.000Z');
  });

  test('rejects tampering, a different secret, and expiry', () => {
    const now = Date.UTC(2026, 8, 25, 10, 0, 0);
    const { token } = issuePreviewToken(
      { customerId: 'cust_123', ticketId: 'DSAR-2026-001', user: 'dpo@example.com' },
      secret,
      600,
      now
    );

    expect(verifyPreviewToken(`${token}x`, secret, now).ok).toBe(false);
    expect(verifyPreviewToken(token, `${secret}-other`, now).ok).toBe(false);
    expect(verifyPreviewToken(token, secret, now + 601_000)).toEqual({ ok: false, reason: 'expired' });
  });
});

describe('browser origin guard', () => {
  test('accepts same-origin requests and non-browser clients', () => {
    expect(isSameSiteBrowserRequest(undefined)).toBe(true);
    expect(isSameSiteBrowserRequest('same-origin')).toBe(true);
    expect(isSameSiteBrowserRequest('same-site')).toBe(true);
    expect(isSameSiteBrowserRequest('none')).toBe(true);
  });

  test('rejects cross-site browser requests', () => {
    expect(isSameSiteBrowserRequest('cross-site')).toBe(false);
  });
});
