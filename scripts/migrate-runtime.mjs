import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { config, initializeRuntimeSchema } = require('../lib/core/db.js');
const { getXsuaaConfig } = require('../shared/xsuaa.js');
const { sessionKeyRing } = require('../lib/core/auth.js');

if (!config(process.env)) {
  console.log('[runtime-schema] HANA not configured; skipping bootstrap.');
  process.exit(0);
}

try {
  if (getXsuaaConfig()) sessionKeyRing(process.env);
  const result = await initializeRuntimeSchema(process.env);
  console.log(`[runtime-schema] ready: ${result.tables.join(', ')}`);
} catch (err) {
  console.error(`[runtime-schema] failed: ${err.message}`);
  process.exit(1);
}