/**
 * Decoding raw Soroban events into typed chain events.
 *
 * The RPC delivers topics and values as base64 XDR. This module turns that into
 * the small, explicit set of events the contracts actually emit, and rejects
 * anything it does not fully recognise.
 *
 * Strictness is the point. These events are the evidence for money moving, so a
 * payload that is merely *almost* understood must not be coerced into something
 * plausible: a contribution of an unreadable amount is not a contribution of
 * zero, and an event naming four topics where three are expected is not a
 * near-miss to be patched up. A rejected event is skipped and counted; a
 * misread event becomes a wrong balance.
 *
 * The shapes below are not inferred from the contract source alone — they are
 * asserted against bytes captured from Testnet, kept in
 * `tests/fixtures/chain_events.json`.
 */

import { scValToNative, xdr } from '@stellar/stellar-sdk';
import type { RpcEvent } from './stellar.ts';

/**
 * Every event this protocol emits.
 *
 * The first four come from the Factory, the rest from each Group contract.
 */
export type ChainEventName =
  | 'group_created'
  | 'fee_updated'
  | 'treasury_updated'
  | 'pause_updated'
  | 'join'
  | 'start'
  | 'contribution'
  | 'payout'
  | 'fee'
  | 'completed';

export const CHAIN_EVENT_NAMES: readonly ChainEventName[] = [
  'group_created',
  'fee_updated',
  'treasury_updated',
  'pause_updated',
  'join',
  'start',
  'contribution',
  'payout',
  'fee',
  'completed',
];

/** The namespace every event carries as its first topic. */
const EVENT_NAMESPACE = 'susu';

type EventCoordinates = {
  /** Contract that emitted the event. */
  contractId: string;
  ledger: number;
  txHash: string;
  txIndex: number;
  /**
   * Ledger-scoped event ordinal, taken from the paging token.
   *
   * Carried next to `eventId` rather than re-parsed at each use, because the
   * event identity — the key every derived table shares with `indexed_events` —
   * is built from it, and a second place to derive it is a second place to get
   * it wrong.
   */
  eventIndex: number;
  /** The RPC's paging token for this event. Unique and stable. */
  eventId: string;
};

/**
 * A decoded event: where it came from, and what it said.
 *
 * Amounts are base-unit (stroop) integers held as strings. They are `i128` on
 * chain and arrive as `bigint`; they are never converted to `number`, because
 * a JSON number cannot hold every `i128` and silently rounding money is worse
 * than refusing to read it.
 */
export type DecodedChainEvent =
  & EventCoordinates
  & (
    | {
      name: 'group_created';
      creator: string;
      group: string;
      groupId: number;
      token: string;
      contributionAmount: string;
      memberCapacity: number;
    }
    | { name: 'fee_updated'; feeBps: number }
    | { name: 'treasury_updated'; treasury: string }
    | { name: 'pause_updated'; paused: boolean }
    | { name: 'join'; member: string; position: number }
    | { name: 'start'; memberCount: number }
    | { name: 'contribution'; member: string; round: number; amount: string }
    | { name: 'payout'; recipient: string; round: number; recipientAmount: string }
    | { name: 'fee'; treasury: string; round: number; fee: string }
    | { name: 'completed'; rounds: number }
  );

export type DecodeResult =
  | { ok: true; event: DecodedChainEvent }
  | { ok: false; reason: string };

/** How many topics each event is required to carry, including the namespace. */
const TOPIC_COUNT: Record<ChainEventName, number> = {
  group_created: 4,
  fee_updated: 2,
  treasury_updated: 3,
  pause_updated: 2,
  join: 3,
  start: 2,
  contribution: 3,
  payout: 3,
  fee: 3,
  completed: 2,
};

const ADDRESS_PATTERN = /^[GC][A-Z2-7]{55}$/;

function fail(reason: string): DecodeResult {
  return { ok: false, reason };
}

/** Decodes one base64 XDR `ScVal`, converting XDR failures into `undefined`. */
function decodeScVal(base64: string): unknown {
  try {
    return scValToNative(xdr.ScVal.fromXDR(base64, 'base64'));
  } catch {
    return undefined;
  }
}

function isEventName(value: unknown): value is ChainEventName {
  return typeof value === 'string' && (CHAIN_EVENT_NAMES as readonly string[]).includes(value);
}

function asAddress(value: unknown): string | undefined {
  return typeof value === 'string' && ADDRESS_PATTERN.test(value) ? value : undefined;
}

/**
 * Reads a `u32`-like field.
 *
 * Rejects negatives and non-integers rather than truncating: a `round` of 1.5
 * or -1 means the field is not what we think it is.
 */
function asCount(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}

/**
 * Reads an `i128` amount and renders it as a base-unit string.
 *
 * `bigint` is the only accepted representation. A JSON number would mean the
 * SDK gave us something we do not expect, and reading it as an amount is
 * precisely the rounding error this function exists to prevent.
 */
function asAmount(value: unknown): string | undefined {
  if (typeof value !== 'bigint') return undefined;
  if (value < 0n) return undefined;
  return value.toString();
}

/** Reads the value payload, which every event carries as a map. */
function asPayload(value: unknown): Record<string, unknown> | undefined {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return undefined;
  return value as Record<string, unknown>;
}

/**
 * Decodes a single raw RPC event.
 *
 * Returns a result rather than throwing so one unrecognised event cannot abort
 * an entire ledger range.
 */
export function decodeChainEvent(raw: RpcEvent): DecodeResult {
  if (raw.successful !== true) {
    return fail('event from unsuccessful contract call');
  }

  if (raw.topic.length < 2) {
    return fail(`expected at least 2 topics, got ${raw.topic.length}`);
  }

  const topics = raw.topic.map(decodeScVal);
  if (topics.some((topic) => topic === undefined)) {
    return fail('a topic was not decodable XDR');
  }

  const [namespace, name, ...rest] = topics as [unknown, unknown, ...unknown[]];

  if (namespace !== EVENT_NAMESPACE) {
    return fail(`unexpected event namespace: ${String(namespace)}`);
  }
  if (!isEventName(name)) {
    return fail(`unknown event name: ${String(name)}`);
  }

  const expectedTopics = TOPIC_COUNT[name];
  if (raw.topic.length !== expectedTopics) {
    return fail(`${name} expects ${expectedTopics} topics, got ${raw.topic.length}`);
  }

  const payload = asPayload(decodeScVal(raw.value));
  if (payload === undefined) {
    return fail(`${name} value is not a map`);
  }

  const coordinates: EventCoordinates = {
    contractId: raw.contractId,
    ledger: raw.ledger,
    txHash: raw.txHash,
    txIndex: raw.txIndex,
    eventIndex: raw.eventIndex,
    eventId: raw.id,
  };

  switch (name) {
    case 'group_created': {
      const creator = asAddress(rest[0]);
      const group = asAddress(rest[1]);
      const groupId = asCount(payload['group_id']);
      const token = asAddress(payload['token']);
      const contributionAmount = asAmount(payload['contribution_amount']);
      const memberCapacity = asCount(payload['member_capacity']);

      if (
        creator === undefined || group === undefined || groupId === undefined ||
        token === undefined || contributionAmount === undefined || memberCapacity === undefined
      ) {
        return fail('group_created payload is malformed');
      }
      return {
        ok: true,
        event: {
          ...coordinates,
          name,
          creator,
          group,
          groupId,
          token,
          contributionAmount,
          memberCapacity,
        },
      };
    }

    case 'fee_updated': {
      const feeBps = asCount(payload['fee_bps']);
      if (feeBps === undefined) return fail('fee_updated payload is malformed');
      return { ok: true, event: { ...coordinates, name, feeBps } };
    }

    case 'treasury_updated': {
      const treasury = asAddress(rest[0]);
      if (treasury === undefined) return fail('treasury_updated payload is malformed');
      return { ok: true, event: { ...coordinates, name, treasury } };
    }

    case 'pause_updated': {
      const paused = payload['paused'];
      if (typeof paused !== 'boolean') return fail('pause_updated payload is malformed');
      return { ok: true, event: { ...coordinates, name, paused } };
    }

    case 'join': {
      const member = asAddress(rest[0]);
      const position = asCount(payload['position']);
      if (member === undefined || position === undefined) {
        return fail('join payload is malformed');
      }
      return { ok: true, event: { ...coordinates, name, member, position } };
    }

    case 'start': {
      const memberCount = asCount(payload['member_count']);
      if (memberCount === undefined) return fail('start payload is malformed');
      return { ok: true, event: { ...coordinates, name, memberCount } };
    }

    case 'contribution': {
      const member = asAddress(rest[0]);
      const round = asCount(payload['round']);
      const amount = asAmount(payload['amount']);
      if (member === undefined || round === undefined || amount === undefined) {
        return fail('contribution payload is malformed');
      }
      return { ok: true, event: { ...coordinates, name, member, round, amount } };
    }

    case 'payout': {
      const recipient = asAddress(rest[0]);
      const round = asCount(payload['round']);
      const recipientAmount = asAmount(payload['recipient_amount']);
      if (recipient === undefined || round === undefined || recipientAmount === undefined) {
        return fail('payout payload is malformed');
      }
      return { ok: true, event: { ...coordinates, name, recipient, round, recipientAmount } };
    }

    case 'fee': {
      const treasury = asAddress(rest[0]);
      const round = asCount(payload['round']);
      const fee = asAmount(payload['fee']);
      if (treasury === undefined || round === undefined || fee === undefined) {
        return fail('fee payload is malformed');
      }
      return { ok: true, event: { ...coordinates, name, treasury, round, fee } };
    }

    case 'completed': {
      const rounds = asCount(payload['rounds']);
      if (rounds === undefined) return fail('completed payload is malformed');
      return { ok: true, event: { ...coordinates, name, rounds } };
    }
  }
}

/** Decodes a batch, keeping the events that decode and counting the rest. */
export function decodeChainEvents(raw: readonly RpcEvent[]): {
  events: DecodedChainEvent[];
  rejected: Array<{ eventId: string; reason: string }>;
} {
  const events: DecodedChainEvent[] = [];
  const rejected: Array<{ eventId: string; reason: string }> = [];

  for (const item of raw) {
    const result = decodeChainEvent(item);
    if (result.ok) events.push(result.event);
    else rejected.push({ eventId: item.id, reason: result.reason });
  }

  return { events, rejected };
}
