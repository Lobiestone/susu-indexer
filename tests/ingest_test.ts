/**
 * Ingest projection tests.
 *
 * These run on real Testnet events, so they assert the projection agrees with
 * what the contracts actually emitted: how many rows of each kind a range
 * produces, which contract each row belongs to, and that the amounts arrive
 * unchanged.
 */

import { assertEquals } from '@std/assert';
import { decodeChainEvents } from '../supabase/functions/_shared/decode.ts';
import { buildEventIdentity } from '../supabase/functions/_shared/events.ts';
import { planIngest } from '../supabase/functions/_shared/ingest.ts';
import type { RpcEvent } from '../supabase/functions/_shared/stellar.ts';
import { allEvents, decodeOk, FACTORY_ID, GROUP_ID, groupEvents } from './fixture.ts';

const decoded = allEvents.map(decodeOk);
const plan = planIngest(decoded);

/** The decoded events of one kind, for counting against the projection. */
function eventsNamed(name: string) {
  return decoded.filter((event) => event.name === name);
}

// ---------------------------------------------------------------------------
// Every event is recorded
// ---------------------------------------------------------------------------

Deno.test('every decoded event is recorded in decoded_events', () => {
  assertEquals(plan.decoded.length, decoded.length);
});

Deno.test('decoded_events rows carry the same identity as the raw index', () => {
  // This is the join between the raw layer and every derived layer. If the two
  // disagree, a fact cannot be traced back to the event that produced it.
  const expected = decoded.map(buildEventIdentity).sort();
  assertEquals(plan.decoded.map((row) => row.event_identity).sort(), expected);
});

Deno.test('the payload carries the event fields and none of its coordinates', () => {
  const row = plan.decoded.find((item) => item.name === 'contribution');
  if (row === undefined) throw new Error('no contribution in the projection');

  assertEquals(row.payload['amount'], '100000000');
  assertEquals(row.payload['round'], 1);

  for (
    const key of ['contractId', 'ledger', 'txHash', 'txIndex', 'eventIndex', 'eventId', 'name']
  ) {
    assertEquals(key in row.payload, false, `${key} must not be duplicated into the payload`);
  }
});

Deno.test('a group_created row belongs to the factory but names the group', () => {
  const rows = plan.decoded.filter((item) => item.name === 'group_created');
  assertEquals(rows.length, 6);

  for (const row of rows) {
    // The Factory emits the announcement, so that is where the event came from;
    // the group it describes is in the payload.
    assertEquals(row.contract_id, FACTORY_ID);
    assertEquals(String(row.payload['group']).startsWith('C'), true);
  }

  // One of the six is the group whose whole lifecycle is in this range, and its
  // announcement names that group rather than the emitting factory.
  assertEquals(rows.some((row) => row.payload['group'] === GROUP_ID), true);
});

Deno.test('the captured range is a whole three-round lifecycle', () => {
  // Worth stating explicitly, because the rest of these tests read as folklore
  // without it: three members joined, contributed in each of three rounds, and
  // every round paid out and took its fee.
  assertEquals(plan.members.length, 3);
  assertEquals(plan.contributions.length, 9);
  assertEquals(plan.payouts.length, 3);
  assertEquals(plan.fees.length, 3);
});

Deno.test('no member contributes twice in the same round', () => {
  // The database refuses this, the contract refuses this, and if the reading of
  // the events produced it the write would fail rather than record a fiction.
  const keys = plan.contributions.map((row) => `${row.round}:${row.member}`);
  assertEquals(new Set(keys).size, keys.length);
});

// ---------------------------------------------------------------------------
// Facts land in their own tables
// ---------------------------------------------------------------------------

Deno.test('each join becomes one member row on the group contract', () => {
  assertEquals(plan.members.length, eventsNamed('join').length);

  const row = plan.members[0];
  if (row === undefined) throw new Error('no member rows');

  // A join is emitted by the group, so the group is the contract that owns it.
  assertEquals(row.contract_id, GROUP_ID);
  assertEquals(row.position > 0, true);
  assertEquals(row.joined_ledger > 0, true);
});

Deno.test('each contribution becomes one contribution row', () => {
  assertEquals(plan.contributions.length, eventsNamed('contribution').length);

  const row = plan.contributions[0];
  if (row === undefined) throw new Error('no contribution rows');

  assertEquals(row.contract_id, GROUP_ID);
  assertEquals(row.round, 1);
  assertEquals(row.amount, '100000000');
  assertEquals(row.member.startsWith('G'), true);
});

Deno.test('each payout becomes a payout row carrying the net amount', () => {
  assertEquals(plan.payouts.length, eventsNamed('payout').length);

  const event = eventsNamed('payout')[0];
  const row = plan.payouts[0];
  if (event?.name !== 'payout' || row === undefined) throw new Error('no payout rows');

  assertEquals(row.recipient_amount, event.recipientAmount);
  assertEquals(row.recipient, event.recipient);
  assertEquals(row.round, event.round);
});

Deno.test('each fee becomes a protocol fee row', () => {
  assertEquals(plan.fees.length, eventsNamed('fee').length);

  const event = eventsNamed('fee')[0];
  const row = plan.fees[0];
  if (event?.name !== 'fee' || row === undefined) throw new Error('no fee rows');

  assertEquals(row.fee, event.fee);
  assertEquals(row.treasury, event.treasury);
});

Deno.test('a fee plus its payout accounts for the whole pool', () => {
  // 3 members at 10 USDC is a 30 USDC pool; the protocol takes 0.5%.
  const payout = plan.payouts[0];
  const fee = plan.fees[0];
  if (payout === undefined || fee === undefined) throw new Error('expected a payout and a fee');

  assertEquals(payout.round, fee.round);
  assertEquals(
    BigInt(payout.recipient_amount) + BigInt(fee.fee),
    BigInt('300000000'),
  );
});

// ---------------------------------------------------------------------------
// Which groups these events can change
// ---------------------------------------------------------------------------

Deno.test('touched groups are the groups, not the factory that announced them', () => {
  // This range announces six groups and carries the lifecycle of one of them.
  // All six can change state; the Factory that emitted the announcements cannot.
  assertEquals(plan.touchedGroups.length, 6);
  assertEquals(plan.touchedGroups.includes(FACTORY_ID), false);
  assertEquals(plan.touchedGroups.includes(GROUP_ID), true);
});

Deno.test('touched groups are deduplicated', () => {
  // The group whose lifecycle is here is also one of the announced groups, so
  // it must not appear twice.
  const many = [...decoded, ...decoded];
  assertEquals(planIngest(many).touchedGroups.length, 6);
});

Deno.test('an empty batch projects to nothing', () => {
  assertEquals(planIngest([]), {
    decoded: [],
    members: [],
    contributions: [],
    payouts: [],
    fees: [],
    touchedGroups: [],
  });
});

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

Deno.test('projecting the same events twice produces the same rows', () => {
  // Idempotency at the write is what makes a replay safe, and it depends on
  // this being pure: the same events must describe the same rows every time.
  const again = planIngest(decoded);
  assertEquals(again.contributions, plan.contributions);
  assertEquals(again.members, plan.members);
  assertEquals(again.payouts, plan.payouts);
  assertEquals(again.fees, plan.fees);
  assertEquals(again.decoded, plan.decoded);
});

Deno.test('amounts survive the projection as exact strings', () => {
  for (const row of plan.contributions) assertEquals(typeof row.amount, 'string');
  for (const row of plan.payouts) assertEquals(typeof row.recipient_amount, 'string');
  for (const row of plan.fees) assertEquals(typeof row.fee, 'string');
});

Deno.test('a failed contribution or payout produces no rows in decoded or fact tables', () => {
  const contribEvent = groupEvents.find((e) => decodeOk(e).name === 'contribution');
  if (contribEvent === undefined) throw new Error('no contribution in fixture');

  const failedContrib: RpcEvent = { ...contribEvent, successful: false };
  const decodedContribBatch = decodeChainEvents([failedContrib]);
  assertEquals(decodedContribBatch.events, []);
  assertEquals(decodedContribBatch.rejected.length, 1);

  const contribPlan = planIngest(decodedContribBatch.events);
  assertEquals(contribPlan.decoded.length, 0);
  assertEquals(contribPlan.contributions.length, 0);

  const payoutEvent = groupEvents.find((e) => decodeOk(e).name === 'payout');
  if (payoutEvent === undefined) throw new Error('no payout in fixture');

  const failedPayout: RpcEvent = { ...payoutEvent, successful: false };
  const decodedPayoutBatch = decodeChainEvents([failedPayout]);
  assertEquals(decodedPayoutBatch.events, []);
  assertEquals(decodedPayoutBatch.rejected.length, 1);

  const payoutPlan = planIngest(decodedPayoutBatch.events);
  assertEquals(payoutPlan.decoded.length, 0);
  assertEquals(payoutPlan.payouts.length, 0);
});
