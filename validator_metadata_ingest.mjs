// validator_metadata_ingest.mjs
// Upsert validator_metadata from already-fetched pool metadata
// Exports only a function — no standalone main()

// @ts-check
import { Pool } from 'pg';

const PG_URL = process.env.DATABASE_URL
  || 'postgresql://localhost:5432/indexer';
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 1000);
const pool = new Pool({ connectionString: PG_URL });

/**
 * Upserts validator_metadata rows into the database.
 * @param {Array<{
 *   aura_pub_key: string,
 *   url?: string,
 *   ticker?: string,
 *   name?: string,
 *   description?: string,
 *   homepage?: string,
 *   updated_at?: string | Date
 * }>} rows
 */
export async function upsertValidatorMetadata(rows) {
  if (!rows?.length) return 0;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const values = [];
      const params = [];
      let p = 1;

      for (const r of batch) {
        values.push(`($${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++})`);
        params.push(
          r.aura_pub_key || null,
          r.url || null,
          r.ticker || null,
          r.name || null,
          r.description || null,
          r.homepage || null,
          r.updated_at ? new Date(r.updated_at) : null
        );
      }

      const sql = `
        INSERT INTO public.validator_metadata
          (aura_pub_key, url, ticker, name, description, homepage, updated_at)
        VALUES ${values.join(',')}
        ON CONFLICT (aura_pub_key) DO UPDATE
        SET url = COALESCE(EXCLUDED.url, public.validator_metadata.url),
            ticker = COALESCE(EXCLUDED.ticker, public.validator_metadata.ticker),
            name = COALESCE(EXCLUDED.name, public.validator_metadata.name),
            description = COALESCE(EXCLUDED.description, public.validator_metadata.description),
            homepage = COALESCE(EXCLUDED.homepage, public.validator_metadata.homepage),
            updated_at = COALESCE(EXCLUDED.updated_at, public.validator_metadata.updated_at)
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
