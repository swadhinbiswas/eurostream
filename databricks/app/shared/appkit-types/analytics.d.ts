// Checked-in fallback types for offline builds. Regenerate with:
// npm run typegen -- --wait
import '@databricks/appkit-ui/react';
import type { SQLNumberMarker, SQLStringMarker } from '@databricks/appkit-ui/js';

declare module '@databricks/appkit-ui/react' {
  interface QueryRegistry {
    overview: {
      name: 'overview';
      parameters: Record<string, never>;
      result: Array<{
        generated_at: string;
        customer_count: number;
        consented_customers: number;
        lifetime_spend_eur: number;
        latest_order_at: string | null;
        velocity_alerts: number;
        zscore_alerts: number;
        geo_alerts: number;
        fraud_alert_count: number;
        latest_alert_at: string | null;
        quality_pass_rate: number | null;
        last_quality_check_at: string | null;
        erasure_requests: number;
        completed_erasures: number;
        failed_erasures: number;
        average_latency_seconds: number | null;
        suppressed_customers: number;
      }>;
    };
    consent_by_country: {
      name: 'consent_by_country';
      parameters: Record<string, never>;
      result: Array<{
        country: string;
        customer_count: number;
        opted_in_count: number;
        opted_out_count: number;
        consent_rate_pct: number;
      }>;
    };
    customers: {
      name: 'customers';
      parameters: {
        searchTerm: SQLStringMarker;
        country: SQLStringMarker;
        consentFilter: SQLStringMarker;
        limit: SQLNumberMarker;
        offset: SQLNumberMarker;
      };
      result: Array<{
        customer_id: string;
        country: string;
        order_count: number;
        lifetime_spend_eur: number;
        average_order_value_eur: number | null;
        marketing_consent: boolean;
        velocity_alerts: number;
        zscore_alerts: number;
        geo_alerts: number;
        fraud_alert_count: number;
        last_alert_at: string | null;
        suppressed: boolean;
        last_order_at: string | null;
      }>;
    };
    fraud_summary: {
      name: 'fraud_summary';
      parameters: Record<string, never>;
      result: Array<{
        customer_id: string;
        velocity_alerts: number;
        zscore_alerts: number;
        geo_alerts: number;
        total_alerts: number;
        max_score: number | null;
        last_alert_at: string | null;
      }>;
    };
    quality_latest: {
      name: 'quality_latest';
      parameters: Record<string, never>;
      result: Array<{
        check_name: string;
        passed: boolean;
        detail: string | null;
        checked_at: string;
      }>;
    };
    erasure_audit: {
      name: 'erasure_audit';
      parameters: { limit: SQLNumberMarker };
      result: Array<{
        request_id: string;
        customer_id: string;
        status: string;
        requested_by: string;
        ticket_id: string;
        requested_at: string;
        completed_at: string | null;
        latency_seconds: number | null;
        logical_verified: boolean | null;
        physical_verified: boolean | null;
        export_verified: boolean | null;
        physical_purge_status: string;
        blocked_layer: string | null;
        next_action: string | null;
        confirmation_hash: string;
        failed_layer: string | null;
        error_detail: string | null;
      }>;
    };
    erasure_impact: {
      name: 'erasure_impact';
      parameters: { customerId: SQLStringMarker };
      result: Array<{
        silver_customer_rows: number;
        silver_order_rows: number;
        silver_payment_rows: number;
        silver_order_quarantine_rows: number;
        silver_payment_quarantine_rows: number;
        gold_customer_rows: number;
        gold_order_rows: number;
        fraud_summary_rows: number;
        already_suppressed: boolean;
      }>;
    };
    operations_summary: {
      name: 'operations_summary';
      parameters: Record<string, never>;
      result: Array<{
        table_name: string;
        row_count: number;
      }>;
    };
  }
}
