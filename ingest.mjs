// @ts-check
import { createClient } from 'graphql-ws';
import WebSocket from 'ws';
import { Pool } from 'pg';
import REQUEST from './public/request.mjs'

// ---------- Config ----------
const GRAPHQL_WS_URL = 'wss://indexer-rs.testnet-02.midnight.network/api/v1/graphql/ws';
const BUFFER_LIMIT = 500;  // max blocks per batch commit
const RETRIES = 3;         // retry attempts for DB ops
const RETRY_BASE_DELAY_MS = 200;
const SLOT_DURATION_MS = 6000;
const SLOTS_PER_EPOCH = 1200;
const EPOCH_MS = SLOT_DURATION_MS * SLOTS_PER_EPOCH;

// 🔎 Debug block (set via env DEBUG_BLOCK=738). Defaults to 738 if not set.
const DEBUG_BLOCK = Number(process.env.DEBUG_BLOCK || 738);

// ---------- PG ----------
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://localhost:5432/indexer',
});

const resp = await REQUEST.getEpoch();
if (!resp?.result?.sidechain?.nextEpochTimestamp) {
  throw new Error('getEpoch() missing sidechain epoch info');
}
const sideRef = resp.result.sidechain;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function retry(fn, attempts = RETRIES, baseDelayMs = RETRY_BASE_DELAY_MS) {
  let delay = baseDelayMs;
  for (let i = 1; i <= attempts; i++) {
    try {
      return await fn();
    } catch (e) {
      if (i === attempts) throw e;
      console.warn(`Retry ${i} failed, retrying in ${delay}ms...`);
      await sleep(delay);
      delay *= 2;
    }
  }
}

// hex -> Buffer for bytea columns
function hexToBuffer(hex) {
  if (!hex) return null;
  const h = hex.startsWith('0x') ? hex.slice(2) : hex;
  return Buffer.from(h, 'hex');
}

// pretty hex for logs
function normHex(h) {
  if (!h) return String(h);
  return (h.startsWith('0x') ? h : '0x' + h).toLowerCase();
}

async function getHighestBlockHeight() {
  const { rows } = await pool.query('SELECT COALESCE(MAX(height), 0) AS maxh FROM block');
  const start = Number(rows[0].maxh) + 1;
  console.log('Starting ingestion from block height:', start);
  return start;
}

// ---------- Main ingest ----------
export default async function ingestRecentBlocks(startParam = null, endParam = null) {
  const startExecutionTime = Date.now();
  const startHeight = startParam ?? await getHighestBlockHeight();
  const endHeight = endParam ?? Number.POSITIVE_INFINITY;

  const query = `
    subscription {
      blocks(offset: { height: ${startHeight} }) {
        author
        height
        timestamp
        parent { hash }
        hash
        protocolVersion
        transactions {
          hash
          identifiers
          merkleTreeRoot
          applyStage
          contractActions {
            __typename
            address
          }
        }
      }
    }
  `;

  const wsClient = createClient({
    url: GRAPHQL_WS_URL,
    webSocketImpl: WebSocket,
  });

  let finished = false;
  let inserting = false;
  let poolEnded = false;
  let shutdownPromise = null; // debounce

  const blockBuffer = new Map();  // height -> block
  let expectedHeight = startHeight;

  async function shutdown() {
    if (shutdownPromise) return shutdownPromise;
    shutdownPromise = (async () => {
      if (poolEnded) return;
      finished = true;

      try { wsClient.dispose?.(); } catch {}

      // wait for any in-flight batch
      while (inserting) await sleep(25);

      // one last contiguous flush if anything remains
      if (blockBuffer.size) {
        await insertBufferedBlocks();
      }

      await pool.end();
      poolEnded = true;
      console.log('🛑 Closed WS and DB pool.');
    })();
    return shutdownPromise;
  }

  async function insertBufferedBlocks() {
    if (inserting || poolEnded) return;
    inserting = true;

    // Build a contiguous window from expectedHeight
    const heights = [];
    for (let h = expectedHeight; heights.length < BUFFER_LIMIT && blockBuffer.has(h); h++) {
      heights.push(h);
    }
    if (heights.length === 0) { inserting = false; return; }

    // Snapshot (do NOT delete from blockBuffer yet)
    const snapshot = [];
    for (const h of heights) {
      const b = blockBuffer.get(h);
      if (!b || !b.timestamp) continue;
      const t = new Date(b.timestamp);
      if (Number.isNaN(t.getTime())) continue;
      snapshot.push({ h, b, t });
    }
    if (snapshot.length === 0) { inserting = false; return; }

    let processed = 0;
    const toDelete = [];
    let shouldStop = false;

    try {
      await retry(async () => {
        const db = await pool.connect();
        try {
          await db.query('BEGIN');

          for (const { h, b, t } of snapshot) {
            // End conditions: stop at endHeight or "now"
            if (b.height > endHeight || t.getTime() >= startExecutionTime) {
              finished = true;
              shouldStop = true;
              break;
            }


const { epoch: sideEpoch /*, slot */ } = deriveEpochAndSlot(t, sideRef);
            // ---- Insert block (epoch_no/slot_no left NULL by design)
            await db.query(
              `INSERT INTO block (
                 block_hash, height, protocol_version, slot_no, epoch_no,
                 time, prev_hash, tx_count, size, fee_dust, aura_pub_key
               )
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
               ON CONFLICT (block_hash) DO NOTHING`,
              [
                hexToBuffer(b.hash),
                b.height,
                b.protocolVersion,               // protocol_version
                null,               // ← computed slot_no
                sideEpoch,          // ← computed epoch_no
                t,
                b.parent.hash,               // prev_hash
                Array.isArray(b.transactions) ? b.transactions.length : 0,
                null,               // size
                null,               // fee_dust
                b.author
              ]
            );
            // ---- Insert txs
            const txs = Array.isArray(b.transactions) ? b.transactions : [];

            // [DBG] show what we got for the debug block before assigning indices
            if (b.height === DEBUG_BLOCK) {
              console.log(`[DBG ${DEBUG_BLOCK}] pre-insert txs (${txs.length}):`);
              txs.forEach((txx, idx) => console.log(`  recv[${idx}] -> ${normHex(txx.hash)}`));
            }

            for (let i = 0; i < txs.length; i++) {
              const tx = txs[i];

              // [DBG] log the assigned index we’re about to write
              if (b.height === DEBUG_BLOCK) {
                console.log(`[DBG ${DEBUG_BLOCK}] assign index ${i} -> ${normHex(tx.hash)}`);
              }
              // Per-tx smart contract action counts
              const actions = Array.isArray(tx.contractActions) ? tx.contractActions : [];
              let deployCount = 0, updateCount = 0, callCount = 0;

              for (const a of actions) {
                if (!a || !a.__typename) continue;
                if (a.__typename === 'ContractDeploy') deployCount++;
                else if (a.__typename === 'ContractUpdate') updateCount++;
                else if (a.__typename === 'ContractCall')   callCount++;
              }

              await db.query(
                `INSERT INTO tx (
                   tx_hash, block_hash, block_height, index_in_block,
                   status_reason,
                   timestamp, fee_dust, merkle_tree_root,
                   deploy_count, update_count, call_count
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                 ON CONFLICT (tx_hash, block_hash) DO UPDATE
                 SET
                   index_in_block = EXCLUDED.index_in_block,
                   timestamp      = EXCLUDED.timestamp,
                   status_reason  = EXCLUDED.status_reason,
                   deploy_count   = EXCLUDED.deploy_count,
                   update_count   = EXCLUDED.update_count,
                   call_count     = EXCLUDED.call_count
                 WHERE
                   tx.index_in_block IS DISTINCT FROM EXCLUDED.index_in_block OR
                   tx.timestamp      IS DISTINCT FROM EXCLUDED.timestamp      OR
                   tx.status_reason  IS DISTINCT FROM EXCLUDED.status_reason  OR
                   tx.deploy_count   IS DISTINCT FROM EXCLUDED.deploy_count   OR
                   tx.update_count   IS DISTINCT FROM EXCLUDED.update_count   OR
                   tx.call_count     IS DISTINCT FROM EXCLUDED.call_count
                `,
                [
                  hexToBuffer(tx.hash),
                  hexToBuffer(b.hash),
                  b.height,
                  i,
                  tx.applyStage || null,   // "SucceedEntirely"/"FailEntirely"
                  t,
                  0,
                  hexToBuffer(tx.merkleTreeRoot),
                  deployCount,
                  updateCount,
                  callCount
                ]
              );


              // ---- Contract action rollup per address (idempotent via (height,index))
              //const actions = Array.isArray(tx.contractActions) ? tx.contractActions : [];
              const byAddr = new Map();
              for (const a of actions) {
                if (!a || !a.__typename) continue;
                const addr = a.address || null;
                if (!addr) continue;
                if (!byAddr.has(addr)) byAddr.set(addr, { deploy: 0, update: 0, call: 0 });
                const rec = byAddr.get(addr);
                if (a.__typename === 'ContractDeploy') rec.deploy++;
                else if (a.__typename === 'ContractUpdate') rec.update++;
                else if (a.__typename === 'ContractCall')   rec.call++;
              }
              for (const [addr, c] of byAddr.entries()) {
                await db.query(
                  `
                  INSERT INTO smart_contract (
                    contract_addr, deploy_total, call_total, update_total,
                    last_block_height, last_tx_index
                  ) VALUES ($1,$2,$3,$4,$5,$6)
                  ON CONFLICT (contract_addr) DO UPDATE
                  SET
                    deploy_total      = smart_contract.deploy_total + EXCLUDED.deploy_total,
                    call_total        = smart_contract.call_total   + EXCLUDED.call_total,
                    update_total      = smart_contract.update_total + EXCLUDED.update_total,
                    last_block_height = EXCLUDED.last_block_height,
                    last_tx_index     = EXCLUDED.last_tx_index
                  WHERE
                    EXCLUDED.last_block_height > smart_contract.last_block_height
                    OR (EXCLUDED.last_block_height = smart_contract.last_block_height
                        AND EXCLUDED.last_tx_index > COALESCE(smart_contract.last_tx_index, -1))
                  `,
                  [addr, c.deploy, c.call, c.update, b.height, i]
                );
              }
            }

            processed += 1;
            toDelete.push(h);
          }

          await db.query('COMMIT');

          // [DBG] after commit, show what the DB has for the debug block
          if (snapshot.some(s => s.b.height === DEBUG_BLOCK)) {
            const db2 = await pool.connect();
            try {
              const { rows } = await db2.query(
                `SELECT block_height, index_in_block, encode(tx_hash,'hex') AS txh
                 FROM tx
                 WHERE block_height = $1
                 ORDER BY index_in_block`,
                [DEBUG_BLOCK]
              );
              console.log(`[DBG ${DEBUG_BLOCK}] DB now has ${rows.length} tx(s):`);
              rows.forEach(r => console.log(`  ${r.index_in_block} -> 0x${r.txh}`));
            } finally {
              db2.release();
            }
          }
        } catch (e) {
          await db.query('ROLLBACK');
          throw e;
        } finally {
          db.release();
        }
      });

      // ✅ Only after COMMIT mutate shared state
      for (const h of toDelete) blockBuffer.delete(h);
      expectedHeight += processed;
      if (processed > 0) console.log(`✅ Committed ${processed} blocks (with tx & action rollups).`);

      if (shouldStop) {
        await shutdown();
        resolveOuter?.();   // ensure outer promise resolves so script can exit
      }
    } catch (err) {
      console.error('❌ Error committing batch:', err);
      await shutdown();
      resolveOuter?.();
      throw err;
    } finally {
      inserting = false;
    }
  }

  console.log(`🚀 Starting block ingestion from height ${startHeight}` + (endParam ? ` to ${endParam}` : ''));

  let resolveOuter; // allow shutdown to resolve the outer promise

  try {
    await new Promise((resolve, reject) => {
      resolveOuter = resolve;

      wsClient.subscribe({ query }, {
        next: async (payload) => {
          if (finished) return;

          const b = payload?.data?.blocks;
          if (!b) return;

          const blocks = Array.isArray(b) ? b : [b];
          for (const blk of blocks) {
            if (!blk || blk.height == null || !blk.timestamp) continue;

            // [DBG] when the debug block arrives, show hashes as received
            if (blk.height === DEBUG_BLOCK) {
              const txs = Array.isArray(blk.transactions) ? blk.transactions : [];
              console.log(`[DBG ${DEBUG_BLOCK}] received block with ${txs.length} tx(s):`);
              txs.forEach((t, idx) => console.log(`  recv[${idx}] -> ${normHex(t.hash)}`));
            }

            // cutoffs — don't buffer beyond requested range / now
            if (blk.height > endHeight || new Date(blk.timestamp).getTime() >= startExecutionTime) {
              await shutdown();
              resolveOuter?.();
              return;
            }

            blockBuffer.set(blk.height, blk);
          }

          while (!finished && blockBuffer.has(expectedHeight) && !inserting) {
            await insertBufferedBlocks();
          }
          if (!finished && blockBuffer.size >= BUFFER_LIMIT && !inserting) {
            await insertBufferedBlocks();
          }
        },
        error: async (err) => {
          if (!finished) {
            console.error('❌ Subscription error:', err);
            await shutdown();
            reject(err);
          }
        },
        complete: async () => {
          if (!finished) console.log('✔️ Subscription complete.');
          await shutdown();
          resolveOuter?.();
        }
      });
    });
  } finally {
    await shutdown();
  }
}

// Replace your getEpochFromTs with this
function getEpochFromTs(blockTimestamp, knownEpoch, nextEpochTimestamp) {
  const ts = blockTimestamp instanceof Date ? blockTimestamp.getTime() : Number(blockTimestamp);
  // If ts === nextEpochTimestamp  → epochsBeforeNext = 0 → epoch = knownEpoch + 1  ✅
  const epochsBeforeNext = Math.ceil((Number(nextEpochTimestamp) - ts) / EPOCH_MS);
  return Number(knownEpoch) + 1 - epochsBeforeNext;
}

// Replace your deriveEpochAndSlot with this (keeps slot math consistent)
function deriveEpochAndSlot(blockTime, ref) {
  const t     = blockTime instanceof Date ? blockTime.getTime() : Number(blockTime);
  const Tnext = Number(ref.nextEpochTimestamp);  // start of (ref.epoch + 1)
  const known = Number(ref.epoch);

  // If t === Tnext → epochsAfterNextStart = 0 + 1 = 1 → epoch = known + 1  ✅
  const epochsAfterNextStart = Math.floor((t - Tnext) / EPOCH_MS) + 1;
  const epoch = known + epochsAfterNextStart;

  // Start time of that computed epoch
  const epochStart = Tnext + (epochsAfterNextStart - 1) * EPOCH_MS;

  // Slot within the epoch (0..1199)
  let slot = Math.floor((t - epochStart) / SLOT_DURATION_MS);
  if (slot < 0) slot = 0;
  if (slot >= SLOTS_PER_EPOCH) slot = SLOTS_PER_EPOCH - 1;

  return { epoch, slot };
}


// ---- CLI entrypoint ----
if (import.meta.url === `file://${process.argv[1]}`) {
  const start = process.argv[2] ? Number(process.argv[2]) : null;
  const end = process.argv[3] ? Number(process.argv[3]) : null;
  ingestRecentBlocks(start, end).catch(e => {
    console.error(e);
    process.exit(1);
  });
}
