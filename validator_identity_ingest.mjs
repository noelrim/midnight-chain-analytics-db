// Build mapping-only validator_map + validator_lifecycle (epochs) atomically.

import { Pool } from 'pg';
import REQUEST from './public/request.mjs';
import * as cryptoUtils from './public/cryptoutils.mjs';

const PG_URL =
  process.env.DATABASE_URL || 'postgresql://indexer:REDACTED@localhost:5432/indexer';
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 1000);
const pool = new Pool({ connectionString: PG_URL });

const strip0xLc = (s) => (s || '').toString().trim().toLowerCase().replace(/^0x/, '');

/** blake2b-224 (56 hex) → Buffer(28) from mainchain key */
function toPoolHexBuf(mainHex) {
  if (!mainHex) return null;
  try {
    const h = cryptoUtils.hashToBlech2b224(mainHex); // 56-hex chars
    if (h && /^[0-9a-f]{56}$/.test(h)) return Buffer.from(h, 'hex');
  } catch (_) {}
  return null;
}

/** membership = permissioned ∪ registered; rows = distinct per-aura enrichment */
function mergeEpochSPOs(sposResponse) {
  const membership = new Set();
  const recs = new Map(); // aura -> { aura_pub_key, sidechain_pub_key?, mainchain_pub_key?, cardano_pool_hex? }

  const perm = Array.isArray(sposResponse?.result?.permissionedCandidates)
    ? sposResponse.result.permissionedCandidates : [];
  for (const pc of perm) {
    const aura = strip0xLc(pc?.auraPublicKey);
    if (!aura) continue;
    membership.add(aura);
    const side = strip0xLc(pc?.sidechainPublicKey);
    const rec = recs.get(aura) || { aura_pub_key: aura };
    if (side) rec.sidechain_pub_key = side;
    recs.set(aura, rec);
  }

  const regs = sposResponse?.result?.candidateRegistrations || {};
  for (const k of Object.keys(regs)) {
    const entries = Array.isArray(regs[k]) ? regs[k] : [];
    for (const r of entries) {
      const aura = strip0xLc(r?.auraPubKey);
      if (!aura) continue;
      membership.add(aura);

      const side = strip0xLc(r?.sidechainPubKey || r?.crossChainPubKey);
      const main = strip0xLc(r?.mainchainPubKey);
      const poolHexBuf = main ? toPoolHexBuf(main) : null;

      const rec = recs.get(aura) || { aura_pub_key: aura };
      if (side) rec.sidechain_pub_key = side;
      if (main) rec.mainchain_pub_key = main;
      if (poolHexBuf) rec.cardano_pool_hex = poolHexBuf;
      recs.set(aura, rec);
    }
  }

  return { rows: Array.from(recs.values()), membership };
}

function toPoolId(mainHex) {
  if (!mainHex) return null;
  try {
    const id = cryptoUtils.hashToBlech2b224(strip0xLc(mainHex)); // 56-hex chars
    return id && /^[0-9a-f]{56}$/.test(id) ? id : null;
  } catch (_) {
    return null;
  }
}


/** First Cardano epoch via block × epoch_schedule */
async function getFirstCardanoEpoch(client) {
  const res = await client.query(`
    SELECT MIN(es.cardano_epoch_no) AS first_cardano_epoch
    FROM public.block b
    JOIN public.epoch_schedule es ON es.epoch_no = b.epoch_no
    WHERE b.epoch_no IS NOT NULL;
  `);
  const v = res.rows?.[0]?.first_cardano_epoch;
  if (v == null) throw new Error('No epochs found via block × epoch_schedule');
  return Number(v);
}

/** Walk epochs and build in-memory state */
async function buildState() {
  const client = await pool.connect();
  try {
    const first = await getFirstCardanoEpoch(client);
    const epochResp = await REQUEST.getEpoch();
    const last = epochResp?.result?.mainchain?.epoch;
    if (last == null) throw new Error('getEpoch() missing mainchain.epoch');

    console.log(`Walking Cardano epochs ${first}..${last}`);

    /** @type {Map<string, {
     *  aura_pub_key: string,
     *  sidechain_pub_key: string|null,
     *  mainchain_pub_key: string|null,
     *  cardano_pool_hex: Buffer|null,
     *  reg_epoch: number|null,
     *  dereg_epoch: number|null
     * }>} */
    const state = new Map();
    let prevMembership = new Set();

    for (let e = first; e <= last; e++) {
      const spos = await REQUEST.getSPOS(e);
      const { rows, membership: currMembership } = mergeEpochSPOs(spos);

      // enrich mapping + set first-seen reg epoch
      for (const r of rows) {
        const aura = r.aura_pub_key;
        const existing = state.get(aura);
        if (!existing) {
          state.set(aura, {
            aura_pub_key: aura,
            sidechain_pub_key: r.sidechain_pub_key || null,
            mainchain_pub_key: r.mainchain_pub_key || null,
            cardano_pool_hex: r.cardano_pool_hex || null,
            reg_epoch: e,
            dereg_epoch: null
          });
        } else {
          if (r.sidechain_pub_key) existing.sidechain_pub_key = r.sidechain_pub_key;

          if (r.mainchain_pub_key) {
            existing.mainchain_pub_key = r.mainchain_pub_key;

            // Derive pool *id* (56-hex string) from mainchain key, if we don't have it yet
            if (!existing.cardano_pool_id) {
              const pid = toPoolId(r.mainchain_pub_key);
              if (pid) existing.cardano_pool_id = pid;
            }

            // Derive pool *hex* (28-byte BYTEA) from mainchain key, if we don't have it yet
            if (!existing.cardano_pool_hex) {
              const buf = toPoolHexBuf(r.mainchain_pub_key);
              if (buf) existing.cardano_pool_hex = buf;
            }
          } else {
            // No main in this row — accept any precomputed fields if present
            if (!existing.cardano_pool_id && r.cardano_pool_id)   existing.cardano_pool_id = r.cardano_pool_id;
            if (!existing.cardano_pool_hex && r.cardano_pool_hex) existing.cardano_pool_hex = r.cardano_pool_hex;
          }
        }
      }

      // set dereg when an aura disappears at epoch e
      for (const aura of prevMembership) {
        if (!currMembership.has(aura)) {
          const rec = state.get(aura);
          if (rec && rec.dereg_epoch == null) {
            rec.dereg_epoch = e;
          }
        }
      }

      prevMembership = currMembership;
    }

    return state;
  } finally {
    client.release();
  }
}

/** Upsert mapping-only into validator_map using a TEMP staging table */
async function mergeValidatorMap(stateMap, client) {
  await client.query(`
    CREATE TEMP TABLE IF NOT EXISTS staging_validator_map
    (LIKE public.validator_map INCLUDING DEFAULTS);
  `);
  await client.query(`TRUNCATE staging_validator_map;`);

  const rows = Array.from(stateMap.values());
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);

    const values = [];
    const params = [];
    let p = 1;

    for (const r of batch) {
      values.push(`($${p++}, $${p++}, $${p++}, $${p++}, $${p++})`);
      params.push(
        r.aura_pub_key || null,
        r.sidechain_pub_key || null,
        r.mainchain_pub_key || null,
        r.cardano_pool_hex || null,   // BYTEA(28)
        r.cardano_pool_id || null,                        
      );
    }

    await client.query(
      `
      INSERT INTO staging_validator_map
        (aura_pub_key, sidechain_pub_key, mainchain_pub_key, cardano_pool_hex, cardano_pool_id)
      VALUES ${values.join(',')};
      `,
      params
    );
  }

  await client.query(`
    INSERT INTO public.validator_map AS vm
      (aura_pub_key, sidechain_pub_key, mainchain_pub_key, cardano_pool_hex, cardano_pool_id)
    SELECT
      s.aura_pub_key, s.sidechain_pub_key, s.mainchain_pub_key, s.cardano_pool_hex, s.cardano_pool_id
    FROM staging_validator_map s
    ON CONFLICT (aura_pub_key) DO UPDATE
    SET
      sidechain_pub_key = COALESCE(EXCLUDED.sidechain_pub_key, vm.sidechain_pub_key),
      mainchain_pub_key = COALESCE(EXCLUDED.mainchain_pub_key, vm.mainchain_pub_key),
      cardano_pool_hex  = COALESCE(EXCLUDED.cardano_pool_hex,  vm.cardano_pool_hex),
      cardano_pool_id   = COALESCE(EXCLUDED.cardano_pool_id,   vm.cardano_pool_id);
  `);

  return rows.length;
}

/** Upsert lifecycle facts into validator_lifecycle */
async function mergeValidatorLifecycle(stateMap, client) {
  const rows = Array.from(stateMap.values());

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);

    const values = [];
    const params = [];
    let p = 1;

    for (const r of batch) {
      values.push(`($${p++}, $${p++}, $${p++})`);
      params.push(
        r.aura_pub_key || null,
        r.reg_epoch ?? null,
        r.dereg_epoch ?? null
      );
    }

    await client.query(
      `
      INSERT INTO public.validator_lifecycle
        (aura_pub_key, cardano_epoch_no_registration, cardano_epoch_no_deregistration)
      VALUES ${values.join(',')}
      ON CONFLICT (aura_pub_key) DO UPDATE
      SET
        -- keep earliest registration epoch
        cardano_epoch_no_registration =
          LEAST(public.validator_lifecycle.cardano_epoch_no_registration,
                COALESCE(EXCLUDED.cardano_epoch_no_registration,
                         public.validator_lifecycle.cardano_epoch_no_registration)),
        -- set dereg only once (first time we observed drop)
        cardano_epoch_no_deregistration =
          COALESCE(public.validator_lifecycle.cardano_epoch_no_deregistration,
                   EXCLUDED.cardano_epoch_no_deregistration);
      `,
      params
    );
  }

  return rows.length;
}

async function main() {
  const stateMap = await buildState();

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const nMap = await mergeValidatorMap(stateMap, client);
    const nLife = await mergeValidatorLifecycle(stateMap, client);
    await client.query('COMMIT');

    console.log(`✅ Committed mapping rows: ${nMap}, lifecycle rows: ${nLife}.`);
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .catch((err) => {
      console.error('❌ validator_identity ingest failed:', err);
      process.exit(1);
    })
    .finally(() => pool.end().catch(() => {}));
}
