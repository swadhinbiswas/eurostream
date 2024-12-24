import { readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { describe, expect, test } from 'vitest';

const queryDirectory = path.resolve(__dirname, '../config/queries');
const queryFiles = readdirSync(queryDirectory).filter((file) => file.endsWith('.sql'));
const queryText = Object.fromEntries(
  queryFiles.map((file) => [file, readFileSync(path.join(queryDirectory, file), 'utf8')])
) as Record<string, string>;

const forbiddenColumns = ['email', 'iban', 'ip_address', 'email_hash', 'iban_hash'];

describe('Databricks App query contract', () => {
  test('ships the expected allow-listed read models', () => {
    expect(queryFiles.sort()).toEqual([
      'consent_by_country.obo.sql',
      'customers.obo.sql',
      'erasure_audit.obo.sql',
      'erasure_impact.obo.sql',
      'fraud_summary.obo.sql',
      'operations_summary.obo.sql',
      'overview.obo.sql',
      'quality_latest.obo.sql',
    ]);
  });

  test('every browser query runs on behalf of the signed-in user', () => {
    expect(queryFiles.every((file) => file.endsWith('.obo.sql'))).toBe(true);
  });

  test('read models never use SELECT *', () => {
    for (const [file, sql] of Object.entries(queryText)) {
      expect(sql, file).not.toMatch(/select\s+\*/i);
    }
  });

  test('read models never query Bronze tables', () => {
    for (const [file, sql] of Object.entries(queryText)) {
      expect(sql, file).not.toMatch(/\beurostream\.bronze\./i);
    }
  });

  test('browser queries do not require direct Silver or quarantine access', () => {
    for (const [file, sql] of Object.entries(queryText)) {
      expect(sql, file).not.toMatch(/\beurostream\.silver\./i);
    }
  });

  test('read models never project raw or hashed PII columns', () => {
    for (const [file, sql] of Object.entries(queryText)) {
      for (const column of forbiddenColumns) {
        expect(sql, `${file}: ${column}`).not.toMatch(new RegExp(`\\b${column}\\b`, 'i'));
      }
    }
  });
});
