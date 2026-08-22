'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..', '..');
const port = Number(process.env.E2E_PORT || 4173);
const types = { '.js': 'application/javascript', '.html': 'text/html; charset=utf-8', '.svg': 'image/svg+xml' };

function send(res, status, body, type = 'application/json') {
  res.writeHead(status, { 'Content-Type': type, 'Cache-Control': 'no-store' });
  res.end(body);
}

http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (url.pathname.startsWith('/api/')) {
    if (url.pathname === '/api/status') return send(res, 200, JSON.stringify({ examActive: true, examName: 'E2E Exam', totalQuestions: 3, durationSecs: 2700, passPct: 80, proctorEnabled: false }));
    if (url.pathname === '/api/admin/auth-methods') return send(res, 200, JSON.stringify({ password: true, xsuaa: { enabled: false } }));
    if (url.pathname === '/api/admin/me') return send(res, 401, JSON.stringify({ ok: false, error: 'unauthorized' }));
    return send(res, 404, JSON.stringify({ error: 'fixture_not_implemented' }));
  }

  let file;
  if (url.pathname === '/' || url.pathname === '') file = path.join(root, 'index.html');
  else if (/^\/client\/[A-Za-z0-9_-]+\.js$/.test(url.pathname)) file = path.join(root, url.pathname);
  else if (url.pathname === '/shared/constants.js') file = path.join(root, 'shared', 'constants.js');
  else if (url.pathname === '/favicon.svg') file = path.join(root, 'favicon.svg');
  else file = path.join(root, 'index.html');

  if (!file.startsWith(root) || !fs.existsSync(file)) return send(res, 404, 'Not found', 'text/plain');
  send(res, 200, fs.readFileSync(file), types[path.extname(file)] || 'application/octet-stream');
}).listen(port, '127.0.0.1', () => console.log(`E2E fixture listening on ${port}`));
