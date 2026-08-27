'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '..');
const read = (p) => fs.readFileSync(path.join(root, p), 'utf8');

test('package exposes explicit unit, E2E, smoke, CI and post-deploy test entrypoints', () => {
  const pkg = JSON.parse(read('package.json'));
  for (const name of ['test:unit', 'test:e2e', 'test:smoke', 'test:postdeploy', 'test:ci', 'scan:secrets']) {
    assert.equal(typeof pkg.scripts[name], 'string', `missing npm script ${name}`);
    assert.ok(pkg.scripts[name].length > 0, `empty npm script ${name}`);
  }
  assert.match(pkg.scripts['test:e2e:setup'], /@playwright\/test@1\.55\.0/);
  assert.match(pkg.scripts['test:e2e:setup'], /--package-lock=false/);
});

test('CI uses shared scanner and pinned Playwright setup without write permissions', () => {
  const workflow = read('.github/workflows/ci.yml');
  assert.match(workflow, /permissions:\s*\n\s*contents: read/);
  assert.match(workflow, /npm run scan:secrets/);
  assert.match(workflow, /npm run test:e2e:setup/);
  assert.match(workflow, /git diff --exit-code -- package\.json package-lock\.json/);
  assert.doesNotMatch(workflow, /contents:\s*write/);
});

test('post-deploy workflow is manual, read-only and keeps access code in Actions secrets', () => {
  const workflow = read('.github/workflows/post-deploy-smoke.yml');
  assert.match(workflow, /workflow_dispatch:/);
  assert.match(workflow, /contents: read/);
  assert.match(workflow, /secrets\.POST_DEPLOY_SMOKE_ACCESS_CODE/);
  assert.doesNotMatch(workflow, /contents:\s*write/);
  assert.doesNotMatch(workflow, /access_code:\s*\n\s*description:/);
});

test('pre-commit and CI share one secret scanner implementation', () => {
  const hook = read('.githooks/pre-commit');
  const workflow = read('.github/workflows/ci.yml');
  assert.match(hook, /scan:secrets:staged/);
  assert.match(workflow, /scan:secrets/);
  assert.doesNotMatch(hook, /awk -v combined/);
  assert.doesNotMatch(workflow, /git grep -n -E/);
});
