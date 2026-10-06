/**
 * Susu Protocol indexer — scheduled Supabase Edge Function.
 *
 * Invoked by Supabase Cron (or manually by an operator). It reads Soroban
 * contract events, records them idempotently, and advances a checkpoint.
 *
 * DESIGN CONSTRAINTS
 * - The indexer is **not** a financial authority. It observes; it never decides
 *   balances, recipients, eligibility, or authorization, and it never signs.
 * - Chain state is authoritative. If index state conflicts with chain state,
 *   the chain wins and reconciliation repairs the index.
 * - Runs are idempotent, resumable, and safely retryable. A missed schedule is
 *   not data loss: the next run resumes from the persisted checkpoint.
 * - No paid always-on worker is required for the MVP.
 *
 * SECURITY
 * - Every invocation must present the shared task secret. This function is not
 *   publicly triggerable.
 * - The service-role key is used only here, server-side, and is never logged or
 *   returned.
 */

import { authorizeInvocation } from '../_shared/auth.ts';
import { canAdvanceCheckpoint, computeLedgerRange, ledgerLag } from '../_shared/checkpoint.ts';
import { loadConfig } from '../_shared/config.ts';
import { type IndexedEventRow, IndexerDb } from '../_shared/db.ts';
import { decodeChainEvents } from '../_shared/decode.ts';
import { discoverGroups } from '../_shared/discovery.ts';
import { buildEventIdentity, compareEventOrder, dedupeByIdentity } from '../_shared/events.ts';
import { planIngest } from '../_shared/ingest.ts';
import { createLogger } from '../_shared/logger.ts';
import { withRetry } from '../_shared/retry.ts';
import { type EventSource, fetchRangeEvents } from '../_shared/scan.ts';
import { compareGroupState, deriveGroupState, NO_FACTS } from '../_shared/state.ts';
import { type RpcEvent, SorobanRpcClient } from '../_shared/stellar.ts';

/** Bounded retry policy for transient RPC and database failures. */
const RETRY = { attempts: 4, baseDelayMs: 250, maxDelayMs: 4_000 } as const;

/**
 * The slice of the RPC client the request handler needs: what the scan needs,
 * plus the chain head. Narrow on purpose, so tests can script it without
 * standing up a network server.
 */
export type RpcSource = EventSource & {
  getLatestLedger(): Promise<number>;
};

type RunSummary = {
  status: 'ok' | 'skipped' | 'failed';
  correlationId: string;
  ledgerFrom?: number;
  ledgerTo?: number;
  eventsIndexed?: number;
  eventsDecoded?: number;
  eventsRejected?: number;
  groupsDiscovered?: number;
  checkpoint?: number;
  lag?: number;
  reason?: string;
};

function jsonResponse(body: RunSummary, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

/** Maps a raw RPC event onto an index row, deriving its chain-assigned identity. */
export function toIndexedRow(event: RpcEvent): IndexedEventRow {
  return {
    event_identity: buildEventIdentity(event),
    ledger: event.ledger,
    tx_hash: event.txHash,
    tx_index: event.txIndex,
    event_index: event.eventIndex,
    contract_id: event.contractId,
    topic: [...event.topic],
    value: event.value,
  };
}

/**
 * Recomputes each group's state from the facts on record, and writes it back.
 *
 * Returns the figures that disagreed with what was stored. The derived values
 * are written either way, so this is a report rather than a decision: the repair
 * is the write that follows it.
 */
async function reconcileGroups(
  db: IndexerDb,
  contractIds: readonly string[],
): Promise<string[]> {
  if (contractIds.length === 0) return [];

  const facts = await withRetry(() => db.readGroupFacts(contractIds), RETRY);
  const states = contractIds.map((id) => deriveGroupState(id, facts.get(id) ?? NO_FACTS));

  const stored = await withRetry(() => db.readGroupState(contractIds), RETRY);
  const divergences = states.flatMap((state) =>
    compareGroupState(stored.get(state.contract_id), state)
  );

  await withRetry(() => db.upsertGroupState(states), RETRY);

  return divergences;
}

export async function handleRequest(
  request: Request,
  deps: { db?: IndexerDb; rpc?: RpcSource } = {},
): Promise<Response> {
  const correlationId = crypto.randomUUID();
  const logger = createLogger(correlationId);

  const configResult = (deps && 'config' in deps && deps.config)
    ? { ok: true as const, config: deps.config }
    : loadConfig();
  if (!configResult.ok) {
    // Report which variables are problematic — never their values.
    const reason = `invalid configuration (missing: ${
      configResult.missing.join(', ') || 'none'
    }; invalid: ${configResult.invalid.join(', ') || 'none'})`;
    logger.error('Indexer configuration is invalid', { reason });
    return jsonResponse({ status: 'failed', correlationId, reason }, 500);
  }

  const config = configResult.config;

  const auth = authorizeInvocation(request.headers, config.taskSecret);
  if (!auth.authorized) {
    logger.warn('Rejected unauthorised indexer invocation', { reason: auth.reason });
    return jsonResponse({ status: 'failed', correlationId, reason: 'unauthorized' }, 401);
  }

  const db = deps.db ?? new IndexerDb(config.supabaseUrl, config.serviceRoleKey);
  const rpc = deps.rpc ?? new SorobanRpcClient(config.rpcUrl);

  try {
    const checkpoint = await withRetry(() => db.getCheckpoint(), RETRY);
    const latestLedger = await withRetry(() => rpc.getLatestLedger(), RETRY);

    const range = computeLedgerRange({
      lastProcessedLedger: checkpoint?.lastProcessedLedger ?? null,
      latestLedger,
      startLedger: checkpoint?.startLedger ?? config.startLedger,
      maxRange: config.maxLedgersPerRun,
    });

    if (range === null) {
      logger.info('Nothing to index', { latestLedger });
      return jsonResponse(
        {
          status: 'skipped',
          correlationId,
          checkpoint: checkpoint?.lastProcessedLedger,
          lag: ledgerLag(checkpoint, latestLedger),
        },
        200,
      );
    }

    logger.info('Indexing ledger range', {
      ledgerFrom: range.from,
      ledgerTo: range.to,
      truncated: range.truncated,
    });

    // The watch list: the Factory, the token, and every group seen so far. A
    // group's events are emitted by its own contract, so without the groups the
    // indexer would see only the Factory and nothing a group ever did.
    const knownGroups = await withRetry(() => db.listGroupContractIds(), RETRY);
    const watched = [config.factoryContractId, config.usdcContractId, ...knownGroups];

    const firstPass = await fetchRangeEvents(rpc, watched, range.from, range.to);

    // The token contract is watched so that the token movements themselves are
    // on record, but they are not Susu events: decoding them would only reject
    // them, and reporting that as a problem would be noise about a non-problem.
    const susuContracts = new Set([config.factoryContractId, ...knownGroups]);
    const firstDecoded = decodeChainEvents(
      dedupeByIdentity([...firstPass].sort(compareEventOrder), buildEventIdentity)
        .filter((event) => susuContracts.has(event.contractId)),
    );

    // A group is normally created and used within one range, and the checkpoint
    // moves past that range at the end of this run. Waiting for the next run to
    // read the new group's events would skip them permanently, because the next
    // run begins after the ledgers they are in. So the same range is read again
    // for the contracts just discovered.
    //
    // One extra pass suffices: only the Factory emits `group_created`, and this
    // pass watches group contracts alone, so it can discover nothing further.
    const newGroups = discoverGroups(firstDecoded.events, watched);

    if (newGroups.length > 0) {
      logger.info('Discovered groups in range; reading it again for them', {
        ledgerFrom: range.from,
        ledgerTo: range.to,
        groupsDiscovered: newGroups.length,
      });
    }

    const secondPass = newGroups.length === 0 ? [] : await fetchRangeEvents(
      rpc,
      newGroups.map((group) => group.contract_id),
      range.from,
      range.to,
    );

    // Both passes are deduplicated by chain identity, so the overlap that a
    // retried range can produce collapses to one write per event.
    // Events from failed contract calls are never indexed (stellar.ts:35-42).
    const raw = dedupeByIdentity(
      [...firstPass, ...secondPass]
        .filter((event) => event.successful === true)
        .sort(compareEventOrder),
      buildEventIdentity,
    );
    const secondDecoded = decodeChainEvents(secondPass);
    const decoded = dedupeByIdentity(
      [...firstDecoded.events, ...secondDecoded.events],
      buildEventIdentity,
    );
    const rejected = [...firstDecoded.rejected, ...secondDecoded.rejected];

    if (rejected.length > 0) {
      // Not fatal, but not expected either: the decoder knows every event the
      // contracts emit, so an unrecognised one means the interface moved.
      logger.warn('Skipped events the decoder did not recognise', {
        rejected: rejected.length,
        reasons: rejected.slice(0, 5).map((item) => item.reason),
      });
    }

    // Groups before events: every other fact refers to a group row, and in the
    // range that discovers a group, both arrive together.
    await withRetry(() => db.upsertGroups(newGroups), RETRY);
    await withRetry(() => db.upsertEvents(raw.map(toIndexedRow)), RETRY);

    const plan = planIngest(decoded);
    await withRetry(() => db.persistPlan(plan), RETRY);

    // Recompute rather than accumulate: a range processed twice corrects the
    // figures instead of inflating them.
    const divergences = await reconcileGroups(db, plan.touchedGroups);
    if (divergences.length > 0) {
      logger.warn('Group state disagreed with the recorded facts; repaired from them', {
        divergences: divergences.length,
        examples: divergences.slice(0, 5),
      });
    }

    // Advance only after the writes succeed, and only if it moves forward.
    if (canAdvanceCheckpoint(checkpoint, range.to)) {
      await withRetry(
        () =>
          db.advanceCheckpoint({
            lastProcessedLedger: range.to,
            startLedger: checkpoint?.startLedger ?? config.startLedger,
          }),
        RETRY,
      );
    }

    logger.info('Indexed ledger range', {
      eventsIndexed: raw.length,
      eventsDecoded: decoded.length,
      eventsRejected: rejected.length,
      groupsDiscovered: newGroups.length,
      checkpoint: range.to,
    });

    return jsonResponse(
      {
        status: 'ok',
        correlationId,
        ledgerFrom: range.from,
        ledgerTo: range.to,
        eventsIndexed: raw.length,
        eventsDecoded: decoded.length,
        eventsRejected: rejected.length,
        groupsDiscovered: newGroups.length,
        checkpoint: range.to,
        lag: Math.max(0, latestLedger - range.to),
      },
      200,
    );
  } catch (error) {
    const reason = error instanceof Error ? error.message : 'unknown indexing failure';
    logger.error('Indexing run failed', { reason });

    // Record the failure for operators. The checkpoint is deliberately left
    // untouched so the same range is retried on the next run.
    try {
      await db.recordRunFailure({
        correlationId,
        ledgerFrom: 0,
        ledgerTo: 0,
        reason,
      });
    } catch (recordError) {
      logger.error('Failed to record run failure in database', {
        reason: recordError instanceof Error ? recordError.message : String(recordError),
      });
    }

    return jsonResponse({ status: 'failed', correlationId, reason }, 500);
  }
}

// Supabase Edge Functions run this module as the request handler. Guarded so
// importing the module (e.g. in tests) does not start a server: the platform
// executes the entrypoint file as the main module, where this is true.
if (import.meta.main) {
  Deno.serve((request) => handleRequest(request));
}
