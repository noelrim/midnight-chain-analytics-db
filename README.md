# midnight-chain-analytics-db
 
> A Postgres-backed indexer for the Midnight Network testnet — blocks, transactions, smart contracts, validators, and cross-chain validator identity mapping with Cardano.
 
This is a custom analytics database for Midnight Network (a ZK-privacy Layer 1 on Substrate). Midnight's stock indexer exposes some chain state via GraphQL, but doesn't surface everything needed for operational analytics — validator lifecycle, per-epoch committee composition, Cardano-Midnight identity mapping. This indexer fills that gap by subscribing to Midnight's GraphQL WebSocket, cross-referencing with Cardano SPO data, and persisting it all into a queryable Postgres schema.
 
Used to back operational dashboards, validator performance reporting, and ad-hoc analytics queries against Midnight chain state.
 
## What it indexes
 
- **Blocks** — height, hash, parent hash, author, timestamp, protocol version
- **Transactions** — per-block transaction records with hash, index, and content
- **Smart contracts** — contract deployments by address, with upsert on conflict
- **Validators** — federated + registered (permissionedCandidates + candidateRegistrations from SPOS)
- **Validator metadata** — pool name, ticker, URL, description (fetched from Cardanoscan / Blockfrost)
- **Validator identity** — `validator_map` mapping Cardano mainchain keys to Midnight sidechain keys via Blake2b-224 hashing
- **Validator lifecycle** — per-epoch participation history
- **Committee composition** — ordered committee membership per finalized epoch
- **Registration statistics** — per-epoch counts of federated, registered (valid/invalid), and d-parameter
## Architecture
 
- **Real-time ingestion** — `graphql-ws` subscription to `wss://indexer-rs.testnet-02.midnight.network/api/v1/graphql/ws`, batching up to 500 blocks per commit
- **Sidechain RPC** — JSON-RPC calls to `rpc.testnet-02.midnight.network` for epoch status (`sidechain_getStatus`), committee membership (`sidechain_getEpochCommittee`), and SPO state (`sidechain_getAriadneParameters`)
- **Cardano integration** — Cardanoscan and Blockfrost APIs for validator pool metadata and identity resolution
- **Retry with exponential backoff** — DB operations retry 3 times with base delay of 200ms
- **Idempotent upserts** — `ON CONFLICT DO UPDATE` patterns for safe re-ingestion
- **Resume from last height** — `getHighestBlockHeight()` query restarts ingestion from the latest persisted block
## Components
 
- `ingest.mjs` — main block / transaction / smart-contract ingestion loop driven by GraphQL subscription
- `validator_ingest.mjs` — federated and registered validator ingestion from SPOS
- `validator_identity_ingest.mjs` — `validator_map` + `validator_lifecycle` (Cardano ↔ Midnight identity mapping)
- `validator_metadata_ingest.mjs` — pool metadata upserts (name, ticker, URL, description)
- `validator_registrations_stat_ingest.mjs` — per-epoch validator registration statistics
- `committee_ingest_auto.mjs` — finds unprocessed finalized epochs, stages ordered committees, computes stats
- `public/request.mjs` — JSON-RPC and REST clients for Midnight node, Cardanoscan, Blockfrost
- `public/cryptoutils.mjs` — Blake2b-224 hashing for mainchain-to-sidechain key derivation
- `docker-compose.yml` — Postgres 16 + pgAdmin
## Stack
 
- **Postgres 16** with `bytea` columns for hash storage
- **Node.js** (`node-fetch`, `pg`, `graphql-ws`, `ws`)
- **Blake2b-224** for Cardano pool hash derivation
- **Docker Compose** for local development
## Environment variables
 
```bash
DATABASE_URL=postgresql://indexer:REDACTED@localhost:5432/indexer
CSCAN_API_KEY=<Cardanoscan API key>
BLOCKFROST_KEY=<Blockfrost project ID>
DEBUG_BLOCK=<optional block height for debug logging>
MAX_EPOCHS_PER_RUN=<optional cap for committee ingest>
```
 
## Getting started
 
```bash
# Start Postgres + pgAdmin
docker-compose up -d
 
# Install dependencies
npm install
 
# Run main ingester (subscribes from the latest persisted block)
node ingest.mjs
 
# Ingest validator data from current epoch
node validator_ingest.mjs
 
# Ingest committee + stats for finalized epochs
node committee_ingest_auto.mjs
```
 
pgAdmin is available at `http://localhost:5050` with credentials `admin@example.com` / `admin`.
 
## Context
 
Part of a broader set of Midnight Network tooling I built while contributing to the mainnet launch — see also `midnight-explorer`, `midnight-mcp`, `midnight-chain-to-nl`. This indexer specifically backs operational analytics and reporting workflows that the stock Midnight indexer doesn't surface directly.
 
## Author
 
Noel Rimbert — [LinkedIn](https://www.linkedin.com/in/noelrimbert/)
 
## License
 
Apache License 2.0
