import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { config, initializeRuntimeSchema, withConnection, tableExists, exec } = require('../lib/core/db.js');

if (!config(process.env)) {
  console.log('[runtime-schema] HANA not configured; skipping bootstrap.');
  process.exit(0);
}

try {
  const result = await initializeRuntimeSchema(process.env);
  await withConnection(async (conn) => {
    if (await tableExists(conn, 'ADMIN_SSO_SESSIONS')) {
      // The V1 table stored bearer JWTs in plaintext. V2 is encrypted-at-rest;
      // invalidate and purge all V1 rows during deployment rather than carrying
      // reusable bearer tokens forward indefinitely.
      await exec(conn, 'DELETE FROM ADMIN_SSO_SESSIONS');
    }
  });
  console.log(`[runtime-schema] ready: ${result.tables.join(', ')}`);
} catch (err) {
  console.error(`[runtime-schema] failed: ${err.message}`);
  process.exit(1);
}
