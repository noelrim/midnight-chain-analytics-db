// validator_ingest.mjs
// Ingest federated and registered validators from SPOS into validator table
// Also fetches and calls upsertValidatorMetadata() for validator_metadata

// @ts-check
import { Pool } from 'pg';
import REQUEST from './public/request.mjs';
import * as cryptoUtils from './public/cryptoutils.mjs';
import { upsertValidatorMetadata } from './validator_metadata_ingest.mjs';

const PG_URL = process.env.DATABASE_URL
  || 'postgresql://indexer:REDACTED@localhost:5432/indexer';
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 1000);
const pool = new Pool({ connectionString: PG_URL });

const strip0xLc = (s) => (s || '').toString().trim().toLowerCase().replace(/^0x/, '');

/**
 * Build arrays for federated and registered validators
 */
function parseSPOS(sposResponse) {
  const federated = [];
  const registered = [];

  const perm = Array.isArray(sposResponse?.result?.permissionedCandidates)
    ? sposResponse.result.permissionedCandidates
    : [];

  for (const pc of perm) {
    const aura = strip0xLc(pc?.auraPublicKey);
    if (!aura) continue;
    federated.push({
      aura_pub_key: aura,
      type: 'federated'
    });
  }

  const regs = sposResponse?.result?.candidateRegistrations || {};
  for (const entries of Object.values(regs)) {
    if (!Array.isArray(entries)) continue;

    for (const r of entries) {
      const aura = strip0xLc(r?.auraPubKey);
      if (!aura) continue;
      const main = strip0xLc(r?.mainchainPubKey);

      registered.push({
        aura_pub_key: aura,
        mainchain_pub_key: main,
        registered_on_cardano_epoch:  r?.utxo?.epochNumber || null,
        type: 'registered'
      });
    }
  }

  return { federated, registered };
}

/**
 * Upsert into validator table
 */
async function upsertValidators(rows) {
  if (!rows.length) return 0;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const values = [];
      const params = [];
      let p = 1;

      for (const r of batch) {
        values.push(`($${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++})`);
        params.push(
          r.aura_pub_key || null,
          r.registered_on_cardano_epoch || null, // integer now
          r.live_stake || null,
          r.live_saturation || null,
          r.live_delegators || null,
          r.active_stake || null,
          r.declared_pledge || null,
          r.live_pledge || null,
          r.margin_cost || null,
          r.fixed_cost || null,
          r.type || null
        );
      }

      const sql = `
        INSERT INTO public.validator
          (aura_pub_key, registered_on_cardano_epoch, live_stake, live_saturation, live_delegators, active_stake,
           declared_pledge, live_pledge, margin_cost, fixed_cost, type)
        VALUES ${values.join(',')}
        ON CONFLICT (aura_pub_key) DO UPDATE
        SET registered_on_cardano_epoch   = COALESCE(EXCLUDED.registered_on_cardano_epoch, public.validator.registered_on_cardano_epoch),
            live_stake      = COALESCE(EXCLUDED.live_stake, public.validator.live_stake),
            live_saturation = COALESCE(EXCLUDED.live_saturation, public.validator.live_saturation),
            live_delegators = COALESCE(EXCLUDED.live_delegators, public.validator.live_delegators),
            active_stake    = COALESCE(EXCLUDED.active_stake, public.validator.active_stake),
            declared_pledge = COALESCE(EXCLUDED.declared_pledge, public.validator.declared_pledge),
            live_pledge     = COALESCE(EXCLUDED.live_pledge, public.validator.live_pledge),
            margin_cost     = COALESCE(EXCLUDED.margin_cost, public.validator.margin_cost),
            fixed_cost      = COALESCE(EXCLUDED.fixed_cost, public.validator.fixed_cost),
            type            = COALESCE(EXCLUDED.type, public.validator.type)
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
  // 1) Get SPOS
  const epochResp = await REQUEST.getEpoch();
  const mainEpoch = epochResp?.result?.mainchain?.epoch;
  if (mainEpoch == null) throw new Error('getEpoch() missing mainchain.epoch');

  const sposResponse = await REQUEST.getSPOS(mainEpoch);
  const { federated, registered } = parseSPOS(sposResponse);

  // 2) Prepare federated rows (no pool calls)
  const validatorRows = [...federated];

  // 3) Process registered
  const metadataRows = [];
  for (const reg of registered) {

    // Pool ID & data
    let poolData = {};
    let poolMeta = {};
    try {
      const poolID = cryptoUtils.hashToBlech2b224(reg.mainchain_pub_key);
      
      poolData = await REQUEST.getPoolData(poolID);
      poolMeta = await REQUEST.getPoolMetaData(poolID);
    } catch (err) {
      console.error(`Pool data/meta fetch failed for ${reg.aura_pub_key}:`, err.message);
    }
 console.log(poolData);
    validatorRows.push({
      aura_pub_key: reg.aura_pub_key,
      registered_on_cardano_epoch: reg.registered_on_cardano_epoch,
      live_stake: poolData?.live_stake || null,
      live_saturation: poolData?.live_saturation || null,
      live_delegators: poolData?.live_delegators || null,
      active_stake: poolData?.active_stake || null,
      declared_pledge: poolData?.declared_pledge || null,
      live_pledge: poolData?.live_pledge || null,
      margin_cost: poolData?.margin_cost || null,
      fixed_cost: poolData?.fixed_cost || null,
      type: 'registered'
    });

    metadataRows.push({
      aura_pub_key: reg.aura_pub_key,
      url: poolMeta?.url || null,
      ticker: poolMeta?.ticker || null,
      name: poolMeta?.name || null,
      description: poolMeta?.description || null,
      homepage: poolMeta?.homepage || null,
      updated_at: new Date()
    });
  }

  // 4) Upsert into validator
  const nVal = await upsertValidators(validatorRows);
  console.log(`✅ Upserted ${nVal} validator row(s).`);

  // 5) Upsert metadata
  const nMeta = await upsertValidatorMetadata(metadataRows);
  console.log(`✅ Upserted ${nMeta} validator_metadata row(s).`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .catch(err => {
      console.error('❌ validator ingest failed:', err);
      process.exit(1);
    })
    .finally(() => pool.end().catch(() => {}));
}
