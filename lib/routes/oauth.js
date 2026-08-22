'use strict';

const auth = require('../core/auth.js');
const { findRoute, replaceLast } = require('./helpers.js');

function migrate(app) {
  const callback = findRoute(app, 'get', '/oauth/callback');
  if (!callback) throw new Error('oauth_callback_route_missing');
  replaceLast(callback, auth.oauthCallbackHandler);
}

module.exports = { migrate };
