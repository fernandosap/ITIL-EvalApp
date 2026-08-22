'use strict';

module.exports = {
  testDir: './tests/e2e',
  timeout: 30000,
  retries: 0,
  use: {
    baseURL: 'http://127.0.0.1:4173',
    browserName: 'chromium',
    headless: true
  },
  webServer: {
    command: 'node tests/e2e/static-server.cjs',
    url: 'http://127.0.0.1:4173',
    reuseExistingServer: false,
    timeout: 15000
  }
};
