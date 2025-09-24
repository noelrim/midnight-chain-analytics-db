// validator_registrations_stat_ingest.mjs
// Per-epoch stats for federated + registered (valid/invalid) + dparam
// Fix: robust counting of candidateRegistrations per epoch (no more all-zeroes).

// @ts-check
import { Pool } from 'pg';
import REQUEST from './public/request.mjs';

const PG_URL = process.env.DATABASE_URL
  || 'postgresql://indexer:REDACTED@localhost:5432/indexer';
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 500);
const pool = new Pool({ connectionString: PG_URL });

const nzInt = (n) => (Number.isFinite(+n) ? Math.trunc(+n) : 0);

/** Get latest epoch already in stats table (or null if none). */
async function getLatestStatEpoch(client) {
  const { rows } = await client.query(
    'SELECT MAX(cardano_epoch_no) AS max_epoch FROM public.validator_registrations_stat'
  );
  return rows?.[0]?.max_epoch ?? null;
}

/**
 * When the stats table is empty:
 * - find earliest block time from public.block
 * - map that timestamp to epoch in public.epoch_schedule
 * - return max(0, epoch - 2)
 */
async function getStartEpochWhenEmpty(client) {
  const { rows: blkRows } = await client.query('SELECT MIN(time) AS min_time FROM public.block');
  const minTime = blkRows?.[0]?.min_time;

  if (!minTime) {
    const { rows: sch1 } = await client.query(
      'SELECT MIN(cardano_epoch_no) AS min_epoch FROM public.epoch_schedule WHERE cardano_epoch_no IS NOT NULL'
    );
    const minEpoch = sch1?.[0]?.min_epoch;
    return Math.max(0, Number.isFinite(+minEpoch) ? (+minEpoch) : 0);
  }

  // Prefer closed-open interval match
  const { rows: schRows } = await client.query(
    `
    SELECT cardano_epoch_no
    FROM public.epoch_schedule
    WHERE start_ts <= $1 AND end_ts > $1
    ORDER BY start_ts DESC
    LIMIT 1
    `,
    [minTime]
  );

  let baseEpoch = schRows?.[0]?.cardano_epoch_no;
  if (baseEpoch == null) {
    const { rows: sch2 } = await client.query(
      `
      SELECT cardano_epoch_no
      FROM public.epoch_schedule
      WHERE start_ts <= $1
      ORDER BY start_ts DESC
      LIMIT 1
      `,
      [minTime]
    );
    baseEpoch = sch2?.[0]?.cardano_epoch_no ?? 0;
  }

  return Math.max(0, Number(baseEpoch) - 2);
}

/** Latest current mainchain epoch via RPC. */
async function getLatestMainchainEpoch() {
  const epochResp = await REQUEST.getEpoch();
  const e = epochResp?.result?.mainchain?.epoch;
  if (e == null) throw new Error('getEpoch() missing mainchain.epoch');
  return nzInt(e);
}

/** Iterate safely over candidateRegistrations entries regardless of shape (Object or Map). */
function* iterCandidateRegistrations(candidateRegistrations) {
  if (!candidateRegistrations) return;

  // Map-like (has .values and not an array)
  if (typeof candidateRegistrations.values === 'function' && !Array.isArray(candidateRegistrations)) {
    for (const arr of candidateRegistrations.values()) {
      if (Array.isArray(arr)) {
        for (const r of arr) yield r;
      }
    }
    return;
  }

  // Plain object
  if (typeof candidateRegistrations === 'object') {
    for (const key of Object.keys(candidateRegistrations)) {
      const arr = candidateRegistrations[key];
      if (Array.isArray(arr)) {
        for (const r of arr) yield r;
      }
    }
  }
}

/** Count registered (valid/invalid) for a given target epoch. */
function countRegisteredForEpoch(candidateRegistrations) {
  let valid = 0, invalid = 0;

  for (const r of iterCandidateRegistrations(candidateRegistrations)) {

    const isValid =
      r?.isValid === true ||
      r?.isValid === 1 ||
      r?.isValid === 'true' ||
      r?.isValid === '1';

    if (isValid) valid++;
    else invalid++;
  }

  return { valid, invalid };
}

/** Build one stats row for a given epoch by calling getSPOS(epoch). */
async function buildRowForEpoch(cardanoEpochNo) {
  const resp = await REQUEST.getSPOS(cardanoEpochNo);
  const result = resp?.result || {};

  // Federated (permissionedCandidates)
  const perm = Array.isArray(result.permissionedCandidates) ? result.permissionedCandidates : [];
  let federatedValid = 0, federatedInvalid = 0;
  for (const pc of perm) {
    const isValid =
      pc?.isValid === true ||
      pc?.isValid === 1 ||
      pc?.isValid === 'true' ||
      pc?.isValid === '1';
    if (isValid) federatedValid++; else federatedInvalid++;
  }

  // Registered, scoped to this epoch
  const { valid: registeredValid, invalid: registeredInvalid } =
    countRegisteredForEpoch(result.candidateRegistrations);

  // dparam ratio (numRegisteredCandidates / numPermissionedCandidates)
  let dparam = null;
  const numPerm = Number(result?.dParameter?.numPermissionedCandidates);
  const numReg  = Number(result?.dParameter?.numRegisteredCandidates);
  if (Number.isFinite(numPerm) && numPerm > 0 && Number.isFinite(numReg)) {
    dparam = numReg / numPerm;
  }

  return {
    cardano_epoch_no: cardanoEpochNo,
    federated_valid_count: federatedValid,
    federated_invalid_count: federatedInvalid,
    registered_valid_count: registeredValid,
    registered_invalid_count: registeredInvalid,
    dparam
  };
}

/** Upsert rows into validator_registrations_stat. */
async function upsertStats(rows) {
  if (!rows.length) return 0;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const params = [];
      const values = [];
      let p = 1;

      for (const r of batch) {
        values.push(`($${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++})`);
        params.push(
          r.cardano_epoch_no,
          r.federated_valid_count,
          r.federated_invalid_count,
          r.registered_valid_count,
          r.registered_invalid_count,
          r.dparam
        );
      }

      const sql = `
        INSERT INTO public.validator_registrations_stat
          (cardano_epoch_no, federated_valid_count, federated_invalid_count,
           registered_valid_count, registered_invalid_count, dparam)
        VALUES ${values.join(',')}
        ON CONFLICT (cardano_epoch_no) DO UPDATE
        SET federated_valid_count    = EXCLUDED.federated_valid_count,
            federated_invalid_count  = EXCLUDED.federated_invalid_count,
            registered_valid_count   = EXCLUDED.registered_valid_count,
            registered_invalid_count = EXCLUDED.registered_invalid_count,
            dparam                   = EXCLUDED.dparam
      `;
      await client.query(sql, params);
    }

    await client.query('COMMIT');
    return rows.length;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

async function main() {
  const client = await pool.connect();
  try {
    // Determine starting epoch
    const latestStat = await getLatestStatEpoch(client);
    const latestChain = await getLatestMainchainEpoch();

    let startEpoch;
    if (latestStat != null) {
      startEpoch = nzInt(latestStat) + 1;
    } else {
      startEpoch = await getStartEpochWhenEmpty(client);
    }

    client.release(); // free before RPC loop

    if (startEpoch > latestChain) {
      console.log(`Nothing to do. Already up to date at epoch ${latestChain}.`);
      return;
    }

    const rows = [];
    for (let e = startEpoch; e <= latestChain; e++) {
      try {
        const row = await buildRowForEpoch(e);
        rows.push(row);
        if (rows.length >= BATCH_SIZE) {
          await upsertStats(rows.splice(0, rows.length));
        }
      } catch (err) {
        console.error(`Epoch ${e} failed:`, err?.message || err);
      }
    }

    if (rows.length) {
      const n = await upsertStats(rows);
      console.log(`✅ Upserted ${n} validator_registrations_stat row(s).`);
    } else {
      console.log('No new epochs to insert.');
    }
  } catch (err) {
    console.error('❌ validator_registrations_stat ingest failed:', err);
    process.exit(1);
  } finally {
    pool.end().catch(() => {});
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
