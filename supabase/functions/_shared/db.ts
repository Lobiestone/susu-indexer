/**
 * Index tables and checkpoint persistence.
 *
 * Uses the service-role key, which bypasses RLS. This code runs **only** in the
 * trusted indexer function: the key must never be shipped to a client, embedded
 * in a response, or written to a log.
 *
 * RLS remains enabled on these tables and grants browser roles no access, so a
 * leaked anon key cannot read or write index state. The service role is not a
 * substitute for those protections — it is an additional, server-only path.
 */

import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import type { Checkpoint } from './checkpoint.ts';
import type { NewGroup } from './discovery.ts';
import type { IngestPlan } from './ingest.ts';
import type { GroupFacts, GroupState, StoredGroupState } from './state.ts';

/**
 * Facts as they are accumulated while reading.
 *
 * The arrays are mutable so they can be pushed to; `GroupFacts` accepts them
 * because a mutable array is assignable to a readonly one.
 */
type MutableFacts = {
  members: { position: number }[];
  contributions: { round: number; amount: string }[];
  payouts: { round: number; recipient_amount: string }[];
  fees: { round: number; fee: string }[];
  started: boolean;
  completed: boolean;
  lastEventLedger: number;
};

export type IndexedEventRow = {
  /** Chain-derived identity; unique, so replays never duplicate rows. */
  event_identity: string;
  ledger: number;
  tx_hash: string;
  tx_index: number;
  event_index: number;
  contract_id: string;
  topic: string[];
  value: string;
};

export class IndexerDb {
  #client: SupabaseClient;

  /**
   * An explicit client may be injected for tests, so the orchestration can be
   * exercised against a stub instead of a live database. Production callers
   * omit it and get the service-role client as before.
   */
  constructor(supabaseUrl: string, serviceRoleKey: string, client?: SupabaseClient) {
    this.#client = client ??
      createClient(supabaseUrl, serviceRoleKey, {
        auth: { persistSession: false, autoRefreshToken: false },
      });
  }

  /**
   * Reads the stored checkpoint.
   *
   * Returns `undefined` when none exists, which means the indexer has not run
   * yet and should start from the configured deployment ledger.
   */
  async getCheckpoint(): Promise<Checkpoint | undefined> {
    const { data, error } = await this.#client
      .from('indexer_checkpoints')
      .select('last_processed_ledger, start_ledger, updated_at')
      .eq('id', 'default')
      .maybeSingle();

    if (error) {
      throw new Error(`Failed to read indexer checkpoint: ${error.message}`);
    }
    if (!data) return undefined;

    return {
      lastProcessedLedger: Number(data.last_processed_ledger),
      startLedger: Number(data.start_ledger),
      updatedAt: String(data.updated_at),
    };
  }

  /**
   * Idempotently upserts indexed events.
   *
   * Conflicting on `event_identity` means a replay or an overlapping range, so
   * the existing row is left as-is rather than overwritten.
   */
  async upsertEvents(rows: readonly IndexedEventRow[]): Promise<void> {
    if (rows.length === 0) return;

    const { error } = await this.#client
      .from('indexed_events')
      .upsert([...rows], { onConflict: 'event_identity', ignoreDuplicates: true });

    if (error) {
      throw new Error(`Failed to upsert indexed events: ${error.message}`);
    }
  }

  /**
   * The group contracts the indexer already knows about.
   *
   * This is the watch list: a group's events are emitted by its own contract,
   * so without these the indexer would see only the Factory. The set grows with
   * every group deployed. Pages through groups in batches using .range() so lists
   * larger than PostgREST's max-rows cap (typically 1000) are not truncated.
   */
  async listGroupContractIds(pageSize = 1000): Promise<string[]> {
    const contractIds: string[] = [];
    let from = 0;

    while (true) {
      const to = from + pageSize - 1;
      const { data, error } = await this.#client
        .from('groups')
        .select('contract_id')
        .order('contract_id', { ascending: true })
        .range(from, to);

      if (error) {
        throw new Error(`Failed to read indexed group contracts: ${error.message}`);
      }

      if (!data || data.length === 0) {
        break;
      }

      for (const row of data) {
        contractIds.push(String(row.contract_id));
      }

      if (data.length < pageSize) {
        break;
      }

      from += pageSize;
    }

    return contractIds;
  }

  /**
   * Records groups the indexer has just discovered.
   *
   * Existing rows are left alone. Discovery supplies identity and nothing else,
   * and the columns it does not set — status, member count, totals — are derived
   * state that belongs to reconciliation, not to discovery.
   */
  async upsertGroups(rows: readonly NewGroup[]): Promise<void> {
    if (rows.length === 0) return;

    const { error } = await this.#client
      .from('groups')
      .upsert([...rows], { onConflict: 'contract_id', ignoreDuplicates: true });

    if (error) {
      throw new Error(`Failed to upsert groups: ${error.message}`);
    }
  }

  /**
   * Records a run's projected events.
   *
   * Groups must already be written: every fact table refers to a group row, and
   * in the range that discovers a group, the group and its facts arrive
   * together.
   *
   * Every write ignores rows that already exist. A replay, an overlapping range
   * and a retried failure therefore all do the same thing, so the index cannot
   * be corrupted by running the same ledger twice.
   */
  async persistPlan(plan: IngestPlan): Promise<void> {
    await this.#insertIgnoringDuplicates('decoded_events', plan.decoded, 'event_identity');
    await this.#insertIgnoringDuplicates('group_members', plan.members, 'contract_id,member');
    await this.#insertIgnoringDuplicates('contributions', plan.contributions, 'event_identity');
    await this.#insertIgnoringDuplicates('payouts', plan.payouts, 'event_identity');
    await this.#insertIgnoringDuplicates('protocol_fees', plan.fees, 'event_identity');
  }

  async #insertIgnoringDuplicates(
    table: string,
    rows: readonly object[],
    onConflict: string,
  ): Promise<void> {
    if (rows.length === 0) return;

    const { error } = await this.#client
      .from(table)
      .upsert([...rows], { onConflict, ignoreDuplicates: true });

    if (error) {
      throw new Error(`Failed to record ${table}: ${error.message}`);
    }
  }

  /**
   * Reads the facts a group's state is derived from.
   *
   * Money columns are read through an explicit `::text` cast. PostgREST renders
   * `numeric` as a JSON *number*, and JavaScript loses integers above 2^53, so
   * reading one without the cast would silently round a total before any of our
   * code saw it. `sumAmounts` refuses a value that is not an integer string, so
   * dropping a cast here fails loudly rather than quietly.
   */
  async readGroupFacts(contractIds: readonly string[]): Promise<Map<string, GroupFacts>> {
    const facts = new Map<string, GroupFacts>();
    if (contractIds.length === 0) return facts;

    const ids = [...contractIds];

    const forGroup = (contractId: string): MutableFacts => {
      let existing = facts.get(contractId) as MutableFacts | undefined;
      if (existing === undefined) {
        existing = {
          members: [],
          contributions: [],
          payouts: [],
          fees: [],
          started: false,
          completed: false,
          lastEventLedger: 0,
        };
        facts.set(contractId, existing);
      }
      return existing;
    };

    const rows = await Promise.all([
      this.#selectIn('group_members', 'contract_id, position', ids),
      this.#selectIn('contributions', 'contract_id, round, amount::text', ids),
      this.#selectIn('payouts', 'contract_id, round, recipient_amount::text', ids),
      this.#selectIn('protocol_fees', 'contract_id, round, fee::text', ids),
      // Lifecycle and the last ledger the group was heard from. `start` and
      // `completed` are the only events that change its status, and both are
      // emitted by the group itself.
      this.#selectIn('decoded_events', 'contract_id, name, ledger', ids),
    ]);

    const [members, contributions, payouts, fees, events] = rows as [
      Record<string, unknown>[],
      Record<string, unknown>[],
      Record<string, unknown>[],
      Record<string, unknown>[],
      Record<string, unknown>[],
    ];

    for (const row of members) {
      forGroup(String(row['contract_id'])).members.push({ position: Number(row['position']) });
    }
    for (const row of contributions) {
      forGroup(String(row['contract_id'])).contributions.push({
        round: Number(row['round']),
        amount: String(row['amount']),
      });
    }
    for (const row of payouts) {
      forGroup(String(row['contract_id'])).payouts.push({
        round: Number(row['round']),
        recipient_amount: String(row['recipient_amount']),
      });
    }
    for (const row of fees) {
      forGroup(String(row['contract_id'])).fees.push({
        round: Number(row['round']),
        fee: String(row['fee']),
      });
    }
    for (const row of events) {
      const group = forGroup(String(row['contract_id']));
      const name = String(row['name']);
      const ledger = Number(row['ledger']);

      if (name === 'start') group.started = true;
      if (name === 'completed') group.completed = true;
      if (ledger > group.lastEventLedger) group.lastEventLedger = ledger;
    }

    return facts;
  }

  /** Reads the derived state currently stored for the given groups. */
  async readGroupState(contractIds: readonly string[]): Promise<Map<string, StoredGroupState>> {
    const states = new Map<string, StoredGroupState>();
    if (contractIds.length === 0) return states;

    const rows = await this.#selectIn(
      'groups',
      'contract_id, status, member_count, current_round, completed_rounds, ' +
        'contributed_total::text, paid_out_total::text, fee_total::text, last_event_ledger',
      contractIds,
    );

    for (const row of rows) {
      states.set(String(row['contract_id']), {
        status: String(row['status']) as StoredGroupState['status'],
        member_count: Number(row['member_count']),
        current_round: Number(row['current_round']),
        completed_rounds: Number(row['completed_rounds']),
        contributed_total: String(row['contributed_total']),
        paid_out_total: String(row['paid_out_total']),
        fee_total: String(row['fee_total']),
        last_event_ledger: Number(row['last_event_ledger']),
      });
    }

    return states;
  }

  /**
   * Writes derived state over the stored figures.
   *
   * An UPDATE, not an upsert, and the distinction is not stylistic. Postgres
   * checks a row's `NOT NULL` constraints on the tuple an `INSERT` proposes,
   * *before* it resolves an `ON CONFLICT` against an existing row. `groups`
   * carries the group's identity — factory, id, creator, token, terms — as
   * `NOT NULL` columns with no defaults, and this method knows none of them,
   * having only derived figures to write. So an upsert here fails on the first
   * row with `null value in column "factory_contract_id"` even though the
   * conflicting row exists and holds every one of those values.
   *
   * That failure only appears against a real Postgres; a stubbed client accepts
   * whatever it is handed.
   *
   * Updating is also the honest description of what reconciliation does. It
   * corrects state on a group that discovery has already recorded; it never
   * introduces a group, and if asked to, something is wrong enough to say so
   * rather than create a half-populated row.
   */
  async upsertGroupState(states: readonly GroupState[]): Promise<void> {
    if (states.length === 0) return;
    const updatedAt = new Date().toISOString();

    for (const state of states) {
      const { contract_id: contractId, ...figures } = state;

      const { error, count } = await this.#client
        .from('groups')
        .update({ ...figures, updated_at: updatedAt }, { count: 'exact' })
        .eq('contract_id', contractId);

      if (error) {
        throw new Error(`Failed to record group state: ${error.message}`);
      }

      // Zero rows means reconciliation was asked about a group that has no row,
      // so discovery and ingest disagree. Creating one here would write a group
      // with no identity, which is worse than failing the run.
      if (count === 0) {
        throw new Error(
          `Failed to record group state: no groups row for ${contractId}. ` +
            'Reconciliation updates existing groups; it never creates them.',
        );
      }
    }
  }

  async #selectIn(
    table: string,
    columns: string,
    contractIds: readonly string[],
  ): Promise<Record<string, unknown>[]> {
    const { data, error } = await this.#client
      .from(table)
      .select(columns)
      .in('contract_id', [...contractIds]);

    if (error) {
      throw new Error(`Failed to read ${table}: ${error.message}`);
    }

    // The column list is dynamic, so the client's inferred row type is not
    // usable here. Callers coerce each field they read.
    return (data ?? []) as unknown as Record<string, unknown>[];
  }

  /**
   * Advances the checkpoint.
   *
   * Guards against regression in the database as well as in code: the update
   * only applies when the new ledger is strictly greater, so concurrent runs
   * cannot move the checkpoint backwards.
   */
  async advanceCheckpoint(params: {
    lastProcessedLedger: number;
    startLedger: number;
  }): Promise<void> {
    const { data: updated, error: updateError } = await this.#client
      .from('indexer_checkpoints')
      .update({
        last_processed_ledger: params.lastProcessedLedger,
        start_ledger: params.startLedger,
        updated_at: new Date().toISOString(),
      })
      .eq('id', 'default')
      .lt('last_processed_ledger', params.lastProcessedLedger)
      .select('id');

    if (updateError) {
      throw new Error(`Failed to advance indexer checkpoint: ${updateError.message}`);
    }

    if (updated && updated.length > 0) {
      return;
    }

    const { error: insertError } = await this.#client
      .from('indexer_checkpoints')
      .insert({
        id: 'default',
        last_processed_ledger: params.lastProcessedLedger,
        start_ledger: params.startLedger,
        updated_at: new Date().toISOString(),
      });

    if (insertError) {
      const isUniqueViolation = insertError.code === '23505' ||
        insertError.message?.includes('duplicate key') ||
        insertError.message?.includes('unique constraint');

      if (isUniqueViolation) {
        const { error: retryError } = await this.#client
          .from('indexer_checkpoints')
          .update({
            last_processed_ledger: params.lastProcessedLedger,
            start_ledger: params.startLedger,
            updated_at: new Date().toISOString(),
          })
          .eq('id', 'default')
          .lt('last_processed_ledger', params.lastProcessedLedger);

        if (retryError) {
          throw new Error(`Failed to advance indexer checkpoint: ${retryError.message}`);
        }
        return;
      }

      throw new Error(`Failed to advance indexer checkpoint: ${insertError.message}`);
    }
  }

  /** Records a failed run for operational visibility. Never throws. */
  async recordRunFailure(params: {
    correlationId: string;
    ledgerFrom: number;
    ledgerTo: number;
    reason: string;
  }): Promise<void> {
    try {
      const { error } = await this.#client.from('indexer_runs').insert({
        correlation_id: params.correlationId,
        ledger_from: params.ledgerFrom,
        ledger_to: params.ledgerTo,
        status: 'failed',
        // Truncated: error text can be long, and never contains secrets by construction.
        reason: params.reason.slice(0, 500),
      });

      if (error) {
        console.error(
          JSON.stringify({
            level: 'error',
            message: 'Failed to record indexer run failure',
            correlationId: params.correlationId,
            error: error.message,
          }),
        );
      }
    } catch (err) {
      console.error(
        JSON.stringify({
          level: 'error',
          message: 'Failed to record indexer run failure',
          correlationId: params.correlationId,
          error: err instanceof Error ? err.message : String(err),
        }),
      );
    }
  }
}
