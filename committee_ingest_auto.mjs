// committee_ingest_auto.mjs
// Fetch unprocessed finalized epochs from DB -> stage ordered committees -> finalize stats

// @ts-check
import { Pool } from 'pg';
import REQUEST from './public/request.mjs';

const PG_URL = process.env.DATABASE_URL
  || 'postgresql://localhost:5432/indexer';

const BATCH_SIZE = 1000;                 // batch rows per insert
const MAX_EPOCHS_PER_RUN = Number(process.env.MAX_EPOCHS_PER_RUN || 1000); // safety cap

const pool = new Pool({ connectionString: PG_URL });

/** get epochs that are finalized, have blocks, and aren’t in epoch_committee_stat yet */
async function findPendingEpochs() {
  const sql = `
    WITH have_blocks AS (
      SELECT DISTINCT epoch_no
      FROM public."block"
      WHERE epoch_no IS NOT NULL
    ),
    max_epoch AS (
      SELECT MAX(epoch_no) AS max_e
      FROM public."block"
      WHERE epoch_no IS NOT NULL
    ),
    processed AS (
      SELECT DISTINCT epoch_no
      FROM public.epoch_committee_stat
    )
    SELECT hb.epoch_no
    FROM have_blocks hb
    CROSS JOIN max_epoch m
    LEFT JOIN processed p ON p.epoch_no = hb.epoch_no
    WHERE hb.epoch_no < m.max_e           -- only epochs strictly before the latest seen
      AND p.epoch_no IS NULL              -- not processed yet
    ORDER BY hb.epoch_no
    LIMIT $1
  `;
  const { rows } = await pool.query(sql, [MAX_EPOCHS_PER_RUN]);
  return rows.map(r => Number(r.epoch_no));
}

async function fetchCommittee(epochNo) {
  const resp = await REQUEST.getCommittee(epochNo);
  const raw = resp?.result?.committee;
  if (!Array.isArray(raw) || raw.length === 0) {
    console.warn(`⚠️  No committee for epoch ${epochNo} (skipping)`);
    return [];
  }
  const rows = [];
  for (let i = 0; i < raw.length; i++) {
    const sc = raw[i]?.sidechainPubKey || raw[i]?.sidechain_public_key || raw[i];
    if (!sc) continue;
    rows.push({
      epoch_no: epochNo,
      position: i,                          // preserve order
      sidechain_pub_key: String(sc).toLowerCase().substring(2) ,
    });
  }
  return rows;
}

async function stageCommittees(allRows) {
  if (allRows.length === 0) return;

  const epochs = Array.from(new Set(allRows.map(r => r.epoch_no))).sort((a, b) => a - b);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // ensure staging has the right shape (no-op if already done)
    await client.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema='public'
            AND table_name='epoch_committee_stage'
            AND column_name='position'
        ) THEN
          ALTER TABLE public.epoch_committee_stage ADD COLUMN position integer;
        END IF;
      END$$;
    `);

    // remove any rows for epochs we're about to insert
    await client.query(
      'DELETE FROM public.epoch_committee_stage WHERE epoch_no = ANY($1::int[])',
      [epochs]
    );

    // insert in batches
    for (let i = 0; i < allRows.length; i += BATCH_SIZE) {
      const batch = allRows.slice(i, i + BATCH_SIZE);
      const values = [];
      const params = [];
      let p = 1;

      for (const r of batch) {
        values.push(`($${p++}, $${p++}, $${p++})`);
        params.push(r.epoch_no, r.position, r.sidechain_pub_key);
      }

      const sql = `
        INSERT INTO public.epoch_committee_stage (epoch_no, position, sidechain_pub_key)
        VALUES ${values.join(',')}
      `;
      await client.query(sql, params);
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function finalizeInDb() {
  // This proc should: map sidechain->aura (validator_identity), compute expected via round-robin,
  // count produced from public."block" grouped by (epoch_no, aura_pub_key), and upsert into epoch_committee_stat.
  await pool.query('CALL public.finalize_epoch_committee_stats_from_stage()');
  console.log(`✅ Finalized epoch_committee_stat`);
}

async function main() {
  const epochs = await findPendingEpochs();
  if (epochs.length === 0) {
    console.log('✅ No pending finalized epochs to process.');
    return;
  }
  console.log(`📥 Pending epochs: ${epochs[0]}..${epochs[epochs.length - 1]} (${epochs.length})`);

  // fetch committees
  const staged = [];
  for (const e of epochs) {
    const rows = await fetchCommittee(e);
    if (rows.length) staged.push(...rows);
  }
  if (staged.length === 0) {
    console.warn('⚠️ No committees fetched; nothing to stage.');
    return;
  }

  // stage and finalize
  await stageCommittees(staged);
  await finalizeInDb();
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .catch(err => {
      console.error('❌ committee auto ingest failed:', err);
      process.exit(1);
    })
    .finally(() => pool.end().catch(() => {}));
}
