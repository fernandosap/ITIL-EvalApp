'use strict';

// In-memory rate limiter. Sliding-window-by-count: each (bucket, key)
// pair has a counter that resets after `windowMs`. When the count
// exceeds `max`, checkRateLimit() returns false and the caller
// responds with 429.
//
// Memory: bounded by an explicit sweep when the Map grows past 5000
// entries. In practice this is a few KB even at 10 RPS for a day.
//
// For multi-process deploys (BTP CF can scale `instances` past 1),
// this is per-process only. A cross-process limit would need
// a shared store (HANA, Redis). Not done yet — single-instance
// deploys are the current reality and the limit is per-IP, which
// already partitions the load.

const _buckets = new Map();
const MAX_BUCKETS = 5000;

function checkRateLimit(bucket, key, max, windowMs) {
  const now = Date.now();
  const bucketKey = `${bucket}:${key}`;
  let entry = _buckets.get(bucketKey);
  if (!entry || entry.resetAt < now) {
    entry = { count: 0, resetAt: now + windowMs };
    _buckets.set(bucketKey, entry);
  }
  entry.count += 1;
  // Periodic sweep so the map doesn't grow unbounded under churn.
  if (_buckets.size > MAX_BUCKETS) {
    for (const [storedKey, value] of _buckets.entries()) {
      if (value.resetAt < now) _buckets.delete(storedKey);
    }
  }
  return entry.count <= max;
}

// Inspect a bucket's current state without incrementing the counter.
// Used by endpoints that want to surface "X of N attempts" to the
// user before they fail.
function peekRateLimit(bucket, key) {
  const now = Date.now();
  const entry = _buckets.get(`${bucket}:${key}`);
  if (!entry || entry.resetAt < now) return { count: 0, resetAt: 0 };
  return { count: entry.count, resetAt: entry.resetAt };
}

// For tests.
function _resetForTests() { _buckets.clear(); }
function _size() { return _buckets.size; }

module.exports = {
  checkRateLimit,
  peekRateLimit,
  _resetForTests,
  _size
};
