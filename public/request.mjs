import fetch from 'node-fetch';

const REQUEST = {
  API_ENDPOINT: "https://rpc.testnet-02.midnight.network/",
  INDEXER_ENDPOINT: "https://indexer-rs.testnet-02.midnight.network/api/v1/graphql/",
  CSCAN_API_KEY: process.env.CSCAN_API_KEY,
  BLOCKFROST_API: "https://cardano-preview.blockfrost.io/api/v0/",
  BLOCKFROST_KEY: process.env.BLOCKFROST_KEY,

  async getEpoch() {
    const payload = {
      jsonrpc: "2.0",
      method: "sidechain_getStatus",
      params: [],
      id: 1
    };
    return this.fetchPOSTResponse(this.API_ENDPOINT, payload);
  },
  async getCommittee(epoch){
   const payload = {
        jsonrpc: "2.0",
        method: "sidechain_getEpochCommittee",
        params: [epoch],
        id: 1
      };
      return this.fetchPOSTResponse(this.API_ENDPOINT, payload);
  },
  async getSPOS(epoch) {
    const payload = {
      jsonrpc: "2.0",
      method: "sidechain_getAriadneParameters",
      params: [epoch],
      id: 1
    };
    return this.fetchPOSTResponse(this.API_ENDPOINT, payload);
  },

  async getCardanoPoolData(poolID) {
    const url = `https://api.cardanoscan.io/api/v1/pool?poolId=${poolID}`;
    return this.fetchGETResponse(url, {
      method: "GET",
      headers: { "apiKey": this.CSCAN_API_KEY }
    });
  },

  async getPoolsList(pageID) {
    const url = `${this.BLOCKFROST_API}pools?count=100&page=${pageID}&order=desc`;
    return this.fetchGETResponse(url, {
      method: "GET",
      headers: { "project_id": this.BLOCKFROST_KEY }
    });
  },

  async getPoolMetaData(poolID) {
    const url = `${this.BLOCKFROST_API}pools/${poolID}/metadata`;
    return this.fetchGETResponse(url, {
      method: "GET",
      headers: { "project_id": this.BLOCKFROST_KEY }
    });
  },

  async getPoolData(poolID) {
    const url = `${this.BLOCKFROST_API}pools/${poolID}`;
    return this.fetchGETResponse(url, {
      method: "GET",
      headers: { "project_id": this.BLOCKFROST_KEY }
    });
  },

  async getEpochData() {
    const url = `${this.BLOCKFROST_API}epochs/latest`;
    return this.fetchGETResponse(url, {
      method: "GET",
      headers: { "project_id": this.BLOCKFROST_KEY }
    });
  },

  async fetchPOSTResponse(url, payload) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      return await res.json();
    } catch (e) {
      console.error('POST request error:', e);
      return null;
    }
  },
  // New method for GraphQL query POST
  async getBlockAtHeight(height) {
    const payload = {
      query:`
        query {
          block(offset: { height: ${height} }) {
            timestamp
          }
        }
      `
    };
    return this.fetchPOSTResponse(this.INDEXER_ENDPOINT, payload);

  },

  async fetchGETResponse(url, options) {
    try {
      const res = await fetch(url, options);
      return await res.json();
    } catch (e) {
      console.error('GET request error:', e);
      return null;
    }
  }
};

export default REQUEST;