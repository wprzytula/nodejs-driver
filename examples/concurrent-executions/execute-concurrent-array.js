"use strict";
const cassandra = require('cassandra-driver');
const executeConcurrent = cassandra.concurrent.executeConcurrent;
const Uuid = cassandra.types.Uuid;

const client = new cassandra.Client({ contactPoints: ['172.42.0.4'], localDataCenter: "datacenter1", queryOptions: { consistency: cassandra.types.consistencies.quorum } });

/**
 * Inserts multiple rows in a table from an Array using the built in method <code>executeConcurrent()</code>,
 * limiting the amount of parallel requests.
 */
async function example() {
  await client.connect();
  await client.execute(`DROP KEYSPACE IF EXISTS bench`);
  await client.execute(`CREATE KEYSPACE bench
                        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '3' }`);
  await client.execute(`USE bench`);
  await client.execute(`CREATE TABLE t (a int, b int, c text, d set<ascii>, primary key (a, b))`);

  // The maximum amount of async executions that are going to be launched in parallel
  // at any given time
  const concurrencyLevel = 256;

  const insertsPerFiber = 20000;

  let next_a = 0;

  const values = Array.from(new Array(insertsPerFiber * concurrencyLevel).keys()).map(x => [next_a += 1, 41, "Ala ma kota.", new Array("xd", "XD")]);

  try {

    const query = "INSERT INTO t (a, b, c, d) VALUES (?, ?, ?, ?)";
    await executeConcurrent(client, query, values, { prepare: true, concurrencyLevel: concurrencyLevel });

    console.log(`Finished executing ${insertsPerFiber} requests per fiber, with a concurrency level of ${concurrencyLevel}.`);

  } finally {
    await client.shutdown();
  }
}

example();

// Exit on unhandledRejection
process.on('unhandledRejection', (reason) => { throw reason; });