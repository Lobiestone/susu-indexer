import { assertEquals, assertThrows } from '@std/assert';
import {
  canAdvanceCheckpoint,
  type Checkpoint,
  computeLedgerRange,
  ledgerLag,
} from '../supabase/functions/_shared/checkpoint.ts';
import { IndexerDb } from '../supabase/functions/_shared/db.ts';

function checkpoint(lastProcessedLedger: number, startLedger = 1): Checkpoint {
  return { lastProcessedLedger, startLedger, updatedAt: '2026-08-01T00:00:00.000Z' };
}

// ---------------------------------------------------------------------------
// Range computation
// ---------------------------------------------------------------------------

Deno.test('a first run starts at the configured deployment ledger', () => {
  assertEquals(
    computeLedgerRange({
      lastProcessedLedger: null,
      latestLedger: 5000,
      startLedger: 1000,
      maxRange: 100,
    }),
    { from: 1000, to: 1099, truncated: true },
  );
});

Deno.test('a resumed run continues immediately after the checkpoint', () => {
  assertEquals(
    computeLedgerRange({
      lastProcessedLedger: 1099,
      latestLedger: 5000,
      startLedger: 1000,
      maxRange: 100,
    }),
    { from: 1100, to: 1199, truncated: true },
  );
});

Deno.test('no ledger is ever skipped between consecutive runs', () => {
  const first = computeLedgerRange({
    lastProcessedLedger: null,
    latestLedger: 10_000,
    startLedger: 500,
    maxRange: 250,
  });
  const second = computeLedgerRange({
    lastProcessedLedger: first?.to ?? null,
    latestLedger: 10_000,
    startLedger: 500,
    maxRange: 250,
  });
  assertEquals(second?.from, (first?.to ?? 0) + 1);
});

Deno.test('the range is capped at the chain head and not marked truncated', () => {
  assertEquals(
    computeLedgerRange({
      lastProcessedLedger: 4990,
      latestLedger: 5000,
      startLedger: 1000,
      maxRange: 100,
    }),
    { from: 4991, to: 5000, truncated: false },
  );
});

Deno.test('a caught-up indexer returns no range', () => {
  assertEquals(
    computeLedgerRange({
      lastProcessedLedger: 5000,
      latestLedger: 5000,
      startLedger: 1000,
      maxRange: 100,
    }),
    null,
  );
});

Deno.test('a checkpoint ahead of the chain head returns no range', () => {
  assertEquals(
    computeLedgerRange({
      lastProcessedLedger: 6000,
      latestLedger: 5000,
      startLedger: 1000,
      maxRange: 100,
    }),
    null,
  );
});

Deno.test('the range never spans more than maxRange ledgers', () => {
  const range = computeLedgerRange({
    lastProcessedLedger: 10,
    latestLedger: 100_000,
    startLedger: 1,
    maxRange: 25,
  });
  assertEquals((range?.to ?? 0) - (range?.from ?? 0) + 1, 25);
});

Deno.test('computeLedgerRange rejects invalid inputs', () => {
  for (
    const bad of [
      { maxRange: 0 },
      { maxRange: -1 },
      { latestLedger: -1 },
      { startLedger: -1 },
    ]
  ) {
    assertThrows(
      () =>
        computeLedgerRange({
          lastProcessedLedger: null,
          latestLedger: 100,
          startLedger: 1,
          maxRange: 10,
          ...bad,
        }),
      Error,
      undefined,
      `expected ${JSON.stringify(bad)} to be rejected`,
    );
  }
});

Deno.test('computeLedgerRange rejects a negative checkpoint', () => {
  assertThrows(() =>
    computeLedgerRange({
      lastProcessedLedger: -1,
      latestLedger: 100,
      startLedger: 1,
      maxRange: 10,
    })
  );
});

// ---------------------------------------------------------------------------
// Checkpoint advancement
// ---------------------------------------------------------------------------

Deno.test('canAdvanceCheckpoint allows the first checkpoint', () => {
  assertEquals(canAdvanceCheckpoint(undefined, 10), true);
});

Deno.test('canAdvanceCheckpoint allows a strictly greater ledger', () => {
  assertEquals(canAdvanceCheckpoint(checkpoint(10), 11), true);
});

Deno.test('canAdvanceCheckpoint rejects an equal ledger (replay is a no-op)', () => {
  assertEquals(canAdvanceCheckpoint(checkpoint(10), 10), false);
});

Deno.test('canAdvanceCheckpoint rejects a lower ledger (no regression)', () => {
  assertEquals(canAdvanceCheckpoint(checkpoint(10), 9), false);
});

Deno.test('canAdvanceCheckpoint rejects invalid ledgers', () => {
  assertEquals(canAdvanceCheckpoint(checkpoint(10), -1), false);
  assertEquals(canAdvanceCheckpoint(checkpoint(10), 1.5), false);
  assertEquals(canAdvanceCheckpoint(checkpoint(10), Number.NaN), false);
});

// ---------------------------------------------------------------------------
// Lag
// ---------------------------------------------------------------------------

Deno.test('ledgerLag is undefined before the first checkpoint', () => {
  assertEquals(ledgerLag(undefined, 100), undefined);
});

Deno.test('ledgerLag reports the distance from the chain tip', () => {
  assertEquals(ledgerLag(checkpoint(90), 100), 10);
});

Deno.test('ledgerLag never reports a negative lag', () => {
  assertEquals(ledgerLag(checkpoint(110), 100), 0);
});

// ---------------------------------------------------------------------------
// Database Checkpoint Persistence
// ---------------------------------------------------------------------------

Deno.test('advanceCheckpoint leaves higher ledger when two runs write in reverse order', async () => {
  type CheckpointRow = {
    id: string;
    last_processed_ledger: number;
    start_ledger: number;
    updated_at: string;
  };

  const state: { row: CheckpointRow | null } = { row: null };

  const originalFetch = globalThis.fetch;
  globalThis.fetch = ((urlInput: string | URL | Request, init?: RequestInit) => {
    const urlStr = typeof urlInput === 'string'
      ? urlInput
      : urlInput instanceof URL
      ? urlInput.href
      : urlInput.url;
    const url = new URL(urlStr);
    const method = init?.method ?? (urlInput instanceof Request ? urlInput.method : 'GET');
    const bodyStr = String(init?.body ?? '{}');

    if (url.pathname.endsWith('/indexer_checkpoints')) {
      if (method === 'PATCH') {
        const body = JSON.parse(bodyStr) as Partial<CheckpointRow>;
        const idCond = url.searchParams.get('id');
        const ltMatch = url.searchParams.get('last_processed_ledger')?.match(/^lt\.(\d+)$/);
        const ltVal = ltMatch?.[1] ? Number.parseInt(ltMatch[1], 10) : null;

        if (
          state.row !== null &&
          idCond === 'eq.default' &&
          ltVal !== null &&
          state.row.last_processed_ledger < ltVal
        ) {
          state.row = { ...state.row, ...body };
          return Promise.resolve(
            new Response(JSON.stringify([{ id: 'default' }]), {
              status: 200,
              headers: { 'content-type': 'application/json' },
            }),
          );
        }
        return Promise.resolve(
          new Response(JSON.stringify([]), {
            status: 200,
            headers: { 'content-type': 'application/json' },
          }),
        );
      }

      if (method === 'POST') {
        const body = JSON.parse(bodyStr) as CheckpointRow;
        const onConflict = url.searchParams.get('on_conflict');
        if (onConflict === 'id') {
          // Unconditional upsert (the buggy old behavior)
          state.row = { ...body };
          return Promise.resolve(
            new Response(JSON.stringify([state.row]), {
              status: 201,
              headers: { 'content-type': 'application/json' },
            }),
          );
        }

        if (state.row !== null && state.row.id === body.id) {
          return Promise.resolve(
            new Response(
              JSON.stringify({
                code: '23505',
                message:
                  'duplicate key value violates unique constraint "indexer_checkpoints_pkey"',
                details: 'Key (id)=(default) already exists.',
              }),
              {
                status: 409,
                headers: { 'content-type': 'application/json' },
              },
            ),
          );
        }

        state.row = { ...body };
        return Promise.resolve(
          new Response(JSON.stringify(state.row), {
            status: 201,
            headers: { 'content-type': 'application/json' },
          }),
        );
      }

      if (method === 'GET') {
        if (state.row) {
          return Promise.resolve(
            new Response(JSON.stringify(state.row), {
              status: 200,
              headers: { 'content-type': 'application/json' },
            }),
          );
        }
        return Promise.resolve(
          new Response(JSON.stringify(null), {
            status: 406,
            headers: { 'content-type': 'application/json' },
          }),
        );
      }
    }

    return Promise.resolve(new Response('not found', { status: 404 }));
  }) as typeof fetch;

  try {
    const db = new IndexerDb('https://example.supabase.co', 'dummy-key');

    // Run 1 writes higher ledger (200)
    await db.advanceCheckpoint({ lastProcessedLedger: 200, startLedger: 1 });
    assertEquals(state.row?.last_processed_ledger, 200);

    // Run 2 (out of order / stale) writes lower ledger (180)
    await db.advanceCheckpoint({ lastProcessedLedger: 180, startLedger: 1 });

    // The higher ledger (200) must remain!
    assertEquals(state.row?.last_processed_ledger, 200);

    // Writing equal ledger (200) is also a no-op
    await db.advanceCheckpoint({ lastProcessedLedger: 200, startLedger: 1 });
    assertEquals(state.row?.last_processed_ledger, 200);

    // Writing strictly higher ledger (250) advances the checkpoint
    await db.advanceCheckpoint({ lastProcessedLedger: 250, startLedger: 1 });
    assertEquals(state.row?.last_processed_ledger, 250);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
