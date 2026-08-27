#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const DEFAULT_PATTERNS = path.join(ROOT, '.githooks', 'secret-patterns.txt');
const IGNORE_PREFIXES = ['.githooks/'];
const IGNORE_FILES = new Set(['package-lock.json']);
const MAX_FILE_BYTES = 2 * 1024 * 1024;

export function loadPatterns(text) {
  return String(text || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'))
    .map((source) => {
      try { return { source, regex: new RegExp(source) }; }
      catch (err) { throw new Error(`invalid_secret_pattern:${source}:${err.message}`); }
    });
}

export function scanText(text, patterns) {
  const hits = [];
  String(text || '').split(/\r?\n/).forEach((line, index) => {
    if (line.includes('secretscan: ok')) return;
    for (const { source, regex } of patterns) {
      regex.lastIndex = 0;
      if (regex.test(line)) {
        hits.push({ line: index + 1, text: line, pattern: source });
        break;
      }
    }
  });
  return hits;
}

function ignored(file) {
  return IGNORE_FILES.has(file) || IGNORE_PREFIXES.some((prefix) => file.startsWith(prefix));
}

function trackedFiles() {
  const output = execFileSync('git', ['ls-files', '-z'], { cwd: ROOT });
  return output.toString('utf8').split('\0').filter(Boolean);
}

function scanRepository(patterns) {
  const hits = [];
  for (const file of trackedFiles()) {
    if (ignored(file)) continue;
    const full = path.join(ROOT, file);
    let stat;
    try { stat = fs.statSync(full); } catch (_e) { continue; }
    if (!stat.isFile() || stat.size > MAX_FILE_BYTES) continue;
    const buf = fs.readFileSync(full);
    if (buf.includes(0)) continue;
    for (const hit of scanText(buf.toString('utf8'), patterns)) hits.push({ file, ...hit });
  }
  return hits;
}

export function parseAddedLines(diff) {
  const rows = [];
  let file = null;
  let newLine = 0;
  for (const raw of String(diff || '').split(/\r?\n/)) {
    const header = raw.match(/^\+\+\+ b\/(.+)$/);
    if (header) { file = header[1]; continue; }
    if (/^\+\+\+ /.test(raw)) { file = null; continue; }
    const hunk = raw.match(/^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@/);
    if (hunk) { newLine = Number(hunk[1]); continue; }
    if (!file || raw.startsWith('---')) continue;
    if (raw.startsWith('+') && !raw.startsWith('+++')) {
      rows.push({ file, line: newLine, text: raw.slice(1) });
      newLine += 1;
    } else if (!raw.startsWith('-')) {
      newLine += 1;
    }
  }
  return rows;
}

function scanStaged(patterns) {
  const diff = execFileSync('git', ['diff', '--cached', '--no-color', '-U0'], { cwd: ROOT, encoding: 'utf8' });
  const hits = [];
  for (const row of parseAddedLines(diff)) {
    if (ignored(row.file) || row.text.includes('secretscan: ok')) continue;
    for (const { source, regex } of patterns) {
      regex.lastIndex = 0;
      if (regex.test(row.text)) {
        hits.push({ ...row, pattern: source });
        break;
      }
    }
  }
  return hits;
}

function report(hits) {
  if (!hits.length) {
    console.log('secretscan: clean');
    return 0;
  }
  console.error('Possible secret pattern(s) detected:');
  for (const hit of hits.slice(0, 20)) console.error(`  ${hit.file}:${hit.line}:${hit.text}`);
  if (hits.length > 20) console.error(`  ...and ${hits.length - 20} more`);
  return 1;
}

export function run(argv = process.argv.slice(2)) {
  const mode = argv.includes('--staged') ? 'staged' : 'repo';
  const patterns = loadPatterns(fs.readFileSync(DEFAULT_PATTERNS, 'utf8'));
  if (!patterns.length) throw new Error('no_secret_patterns_configured');
  return report(mode === 'staged' ? scanStaged(patterns) : scanRepository(patterns));
}

const invoked = process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;
if (invoked) {
  try { process.exitCode = run(); }
  catch (err) {
    console.error(`Secret scan failed: ${err.message}`);
    process.exitCode = 2;
  }
}
