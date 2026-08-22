import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { config, initializeRuntimeSchema } = require('../lib/core/db.js');

if (!config(process.env)) {
  console.log('[runtime-schema] HANA not configured; skipping bootstrap.');
  process.exit(0);
}

try {
  const result = await initializeRuntimeSchema(process.env);
  console.log(`[runtime-schema] ready: ${result.tables.join(', ')}`);
} catch (err) {
  console.error(`[runtime-schema] failed: ${err.message}`);
  process.exit(1);
}
