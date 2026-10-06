import { assertEquals } from '@std/assert';
import { IndexerDb } from '../supabase/functions/_shared/db.ts';
import type { SupabaseClient } from '@supabase/supabase-js';

Deno.test('listGroupContractIds pages through >1000 groups without dropping any', async () => {
  // Generate 2500 group contracts: C0000 to C2499
  const totalGroups = 2500;
  const allGroups = Array.from({ length: totalGroups }, (_, i) => ({
    contract_id: `C${String(i).padStart(4, '0')}`,
  }));

  const rangesRequested: { from: number; to: number }[] = [];

  const stubClient = {
    from: (table: string) => {
      assertEquals(table, 'groups');
      return {
        select: (columns: string) => {
          assertEquals(columns, 'contract_id');
          return {
            order: (col: string, options: { ascending: boolean }) => {
              assertEquals(col, 'contract_id');
              assertEquals(options.ascending, true);
              return {
                range: (from: number, to: number) => {
                  rangesRequested.push({ from, to });
                  // Simulate PostgREST returning rows in range [from, to] capped by slice
                  const page = allGroups.slice(from, to + 1);
                  return Promise.resolve({ data: page, error: null });
                },
              };
            },
          };
        },
      };
    },
  } as unknown as SupabaseClient;

  const db = new IndexerDb('https://example.supabase.co', 'dummy-key', stubClient);

  const result = await db.listGroupContractIds();

  // 1. Asserts all 2500 groups are returned
  assertEquals(result.length, totalGroups);
  assertEquals(result[0], 'C0000');
  assertEquals(result[1000], 'C1000');
  assertEquals(result[2499], 'C2499');

  // 2. Asserts pagination occurred across multiple pages
  // Default pageSize 1000:
  // Page 1: 0 - 999 (returns 1000)
  // Page 2: 1000 - 1999 (returns 1000)
  // Page 3: 2000 - 2999 (returns 500, data.length < 1000 -> stops)
  assertEquals(rangesRequested.length, 3);
  assertEquals(rangesRequested[0], { from: 0, to: 999 });
  assertEquals(rangesRequested[1], { from: 1000, to: 1999 });
  assertEquals(rangesRequested[2], { from: 2000, to: 2999 });
});

Deno.test('listGroupContractIds handles an empty groups table in a single query', async () => {
  let callCount = 0;
  const stubClient = {
    from: () => ({
      select: () => ({
        order: () => ({
          range: () => {
            callCount++;
            return Promise.resolve({ data: [], error: null });
          },
        }),
      }),
    }),
  } as unknown as SupabaseClient;

  const db = new IndexerDb('https://example.supabase.co', 'dummy-key', stubClient);
  const result = await db.listGroupContractIds();

  assertEquals(result, []);
  assertEquals(callCount, 1);
});

Deno.test('listGroupContractIds handles exact page boundary (e.g. 1000 groups)', async () => {
  const allGroups = Array.from({ length: 1000 }, (_, i) => ({
    contract_id: `C${String(i).padStart(4, '0')}`,
  }));

  const stubClient = {
    from: () => ({
      select: () => ({
        order: () => ({
          range: (from: number, to: number) => {
            const page = allGroups.slice(from, to + 1);
            return Promise.resolve({ data: page, error: null });
          },
        }),
      }),
    }),
  } as unknown as SupabaseClient;

  const db = new IndexerDb('https://example.supabase.co', 'dummy-key', stubClient);
  const result = await db.listGroupContractIds(1000);

  assertEquals(result.length, 1000);
});

Deno.test('listGroupContractIds supports custom pageSize parameter', async () => {
  const allGroups = Array.from({ length: 50 }, (_, i) => ({
    contract_id: `C${i}`,
  }));

  let calls = 0;
  const stubClient = {
    from: () => ({
      select: () => ({
        order: () => ({
          range: (from: number, to: number) => {
            calls++;
            const page = allGroups.slice(from, to + 1);
            return Promise.resolve({ data: page, error: null });
          },
        }),
      }),
    }),
  } as unknown as SupabaseClient;

  const db = new IndexerDb('https://example.supabase.co', 'dummy-key', stubClient);
  const result = await db.listGroupContractIds(20);

  // 50 items with pageSize 20 -> 20 + 20 + 10 = 3 pages
  assertEquals(result.length, 50);
  assertEquals(calls, 3);
});
