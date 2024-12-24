import { Plugin, createWorkspaceClient, toPlugin, type PluginManifest, type WorkspaceClient } from '@databricks/appkit';
import type express from 'express';
import { z } from 'zod';
import manifest from './manifest.json';
import { isSameSiteBrowserRequest, issuePreviewToken, verifyPreviewToken } from '../../security/preview-token';

const customerIdSchema = z
  .string()
  .trim()
  .min(3)
  .max(128)
  .regex(/^[A-Za-z0-9][A-Za-z0-9._:-]*$/, 'Use only letters, numbers, dot, underscore, colon, or hyphen');

const ticketIdSchema = z
  .string()
  .trim()
  .min(3)
  .max(128)
  .regex(/^[A-Za-z0-9][A-Za-z0-9._:/-]*$/, 'Invalid ticket ID');

const previewSchema = z.object({
  customerId: customerIdSchema,
  ticketId: ticketIdSchema,
});

const requestSchema = z.object({
  previewToken: z.string().min(32).max(4096),
  customerId: customerIdSchema,
  confirmation: customerIdSchema,
  idempotencyKey: z.string().uuid(),
});

const runIdSchema = z.coerce.number().int().positive();
const terminalRunStates = new Set(['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']);

class ErasureActionsPlugin extends Plugin {
  static manifest = manifest as unknown as PluginManifest<'erasureActions'>;

  private client?: WorkspaceClient;
  private jobId?: number;
  private previewSecret?: string;
  private previewTtlSeconds = 600;
  private internalSloSeconds = 60;
  private readonly recentRequests = new Map<string, number[]>();

  setup() {
    const rawJobId = process.env.DATABRICKS_JOB_ID;
    const jobId = rawJobId ? Number.parseInt(rawJobId, 10) : Number.NaN;
    if (!Number.isSafeInteger(jobId) || jobId <= 0) {
      throw new Error('DATABRICKS_JOB_ID must be a positive numeric job ID');
    }

    const previewSecret = process.env.EUROSTREAM_PREVIEW_SECRET;
    if (!previewSecret || previewSecret.length < 32) {
      throw new Error('EUROSTREAM_PREVIEW_SECRET must contain at least 32 characters');
    }

    const ttl = Number.parseInt(process.env.EUROSTREAM_PREVIEW_TTL_SECONDS ?? '600', 10);
    if (!Number.isSafeInteger(ttl) || ttl < 60 || ttl > 3600) {
      throw new Error('EUROSTREAM_PREVIEW_TTL_SECONDS must be between 60 and 3600');
    }

    const internalSlo = Number.parseInt(process.env.EUROSTREAM_INTERNAL_SLO_SECONDS ?? '60', 10);
    if (!Number.isSafeInteger(internalSlo) || internalSlo <= 0 || internalSlo > 86400) {
      throw new Error('EUROSTREAM_INTERNAL_SLO_SECONDS must be between 1 and 86400');
    }

    this.jobId = jobId;
    this.previewSecret = previewSecret;
    this.previewTtlSeconds = ttl;
    this.internalSloSeconds = internalSlo;
    this.client = createWorkspaceClient();
    return Promise.resolve();
  }

  clientConfig() {
    return {
      previewTtlSeconds: this.previewTtlSeconds,
      internalSloSeconds: this.internalSloSeconds,
    };
  }

  injectRoutes(router: express.Router) {
    this.route(router, {
      name: 'createPreview',
      method: 'post',
      path: '/previews',
      handler: (req, res) => {
        if (!this.requireJsonAndSameSite(req, res)) return Promise.resolve();
        const parsed = previewSchema.safeParse(req.body);
        if (!parsed.success) {
          res.status(400).json({ error: 'Invalid customer or ticket ID' });
          return Promise.resolve();
        }

        const user = this.requireAuthenticatedUser(req, res);
        if (!user) return Promise.resolve();
        const issued = issuePreviewToken(
          { customerId: parsed.data.customerId, ticketId: parsed.data.ticketId, user },
          this.requireSecret(),
          this.previewTtlSeconds
        );

        res.status(201).json({
          ...issued,
          confirmationText: `Type ${parsed.data.customerId} to confirm`,
        });
        return Promise.resolve();
      },
    });

    this.route(router, {
      name: 'submitRequest',
      method: 'post',
      path: '/requests',
      handler: async (req, res) => {
        if (!this.requireJsonAndSameSite(req, res)) return;
        const parsed = requestSchema.safeParse(req.body);
        if (!parsed.success) {
          res.status(400).json({ error: 'Invalid erasure confirmation request' });
          return;
        }

        const user = this.requireAuthenticatedUser(req, res);
        if (!user) return;
        if (!this.allowRequest(user)) {
          res.status(429).json({ error: 'Too many erasure requests; wait before trying again' });
          return;
        }

        const verification = verifyPreviewToken(parsed.data.previewToken, this.requireSecret());
        if (!verification.ok) {
          res.status(verification.reason === 'expired' ? 410 : 400).json({
            error: verification.reason === 'expired' ? 'Preview expired; run it again' : 'Invalid preview',
          });
          return;
        }

        const { payload } = verification;
        if (
          payload.user !== user ||
          payload.customerId !== parsed.data.customerId ||
          payload.customerId !== parsed.data.confirmation
        ) {
          res.status(400).json({ error: 'Confirmation does not match the prepared request' });
          return;
        }

        const requestId = parsed.data.idempotencyKey;
        const response = await this.requireClient().jobs.runNow({
          job_id: this.requireJobId(),
          job_parameters: {
            customer_id: payload.customerId,
            ticket_id: payload.ticketId,
            request_id: requestId,
            idempotency_key: parsed.data.idempotencyKey,
            requested_by: user,
            reason: 'GDPR_ARTICLE_17',
          },
        });
        const runId = response.run_id;
        if (!runId) throw new Error('Jobs API did not return a run ID');

        console.info(
          JSON.stringify({
            event: 'eurostream.erasure.submitted',
            requestId,
            runId,
            requestedBy: user,
            ticketId: payload.ticketId,
          })
        );

        res.status(202).json({
          requestId,
          runId,
          status: 'SUBMITTED',
          requestedBy: user,
          pollUrl: `/api/erasure-actions/runs/${runId}`,
        });
      },
    });

    this.route(router, {
      name: 'getRun',
      method: 'get',
      path: '/runs/:runId',
      handler: async (req, res) => {
        if (!this.requireAuthenticatedUser(req, res)) return;
        const parsed = runIdSchema.safeParse(req.params.runId);
        if (!parsed.success) {
          res.status(400).json({ error: 'Invalid run ID' });
          return;
        }

        const run = await this.requireClient().jobs.getRun({ run_id: parsed.data });
        const lifeCycleState = run.state?.life_cycle_state ?? 'UNKNOWN';
        const resultState = run.state?.result_state ?? 'UNKNOWN';
        res.json({
          runId: parsed.data,
          runName: run.run_name ?? null,
          lifeCycleState,
          resultState,
          terminal: terminalRunStates.has(lifeCycleState),
          startTime: run.start_time ?? null,
          endTime: run.end_time ?? null,
          runPageUrl: run.run_page_url ?? null,
        });
      },
    });
  }

  private requireAuthenticatedUser(req: express.Request, res: express.Response): string | undefined {
    const user = this.resolveUserId(req);
    const normalizedUser = user.trim().toLowerCase();
    const hasUser = normalizedUser.length > 0 && !['unknown', 'anonymous'].includes(normalizedUser);
    if (process.env.NODE_ENV === 'production' && (!req.header('x-forwarded-access-token')?.trim() || !hasUser)) {
      res.status(401).json({ error: 'Authenticated Databricks user context required' });
      return undefined;
    }
    return user;
  }

  private requireClient(): WorkspaceClient {
    if (!this.client) throw new Error('Erasure actions client is not initialized');
    return this.client;
  }

  private requireJobId(): number {
    if (!this.jobId) throw new Error('Erasure job ID is not initialized');
    return this.jobId;
  }

  private requireSecret(): string {
    if (!this.previewSecret) throw new Error('Erasure preview secret is not initialized');
    return this.previewSecret;
  }

  private requireJsonAndSameSite(req: express.Request, res: express.Response): boolean {
    if (!req.is('application/json')) {
      res.status(415).json({ error: 'Content-Type must be application/json' });
      return false;
    }
    if (!isSameSiteBrowserRequest(req.get('sec-fetch-site'))) {
      res.status(403).json({ error: 'Cross-site request rejected' });
      return false;
    }
    return true;
  }

  private allowRequest(user: string): boolean {
    const now = Date.now();
    const cutoff = now - 10 * 60 * 1000;
    const recent = (this.recentRequests.get(user) ?? []).filter((timestamp) => timestamp >= cutoff);
    if (recent.length >= 5) {
      this.recentRequests.set(user, recent);
      return false;
    }
    recent.push(now);
    this.recentRequests.set(user, recent);
    return true;
  }
}

export const erasureActions = toPlugin(ErasureActionsPlugin);
