'use strict';

// Tests for client/dispatcher.js. Verifies the argument parsing and
// sentinel resolution, plus a DOM-level click dispatch test to
// confirm the wiring actually fires the right handler.
//
// The dispatcher is browser-only (uses document.addEventListener), so
// we need a DOM. We use jsdom if available, or skip the DOM test if
// not installed. For now we just test the pure parsing helpers, which
// is the riskiest part of the implementation.

const test = require('node:test');
const assert = require('node:assert/strict');

// The dispatcher module is a browser IIFE. It calls
// document.addEventListener on load. To require it from Node, we need
// a global `document`. We use a tiny stub that records listeners.
const recordedListeners = [];
global.document = {
  addEventListener(type, fn) { recordedListeners.push({ type, fn }); }
};
global.window = global;

// Now require the module. It will run the IIFE, attach to window.IE,
// and register the two listeners.
require('../client/dispatcher.js');

const dispatcher = global.window.IE.dispatcher;

test('coerce: numbers, booleans, null, strings', () => {
  assert.equal(dispatcher.coerce('0'), 0);
  assert.equal(dispatcher.coerce('42'), 42);
  assert.equal(dispatcher.coerce('-3.14'), -3.14);
  assert.equal(dispatcher.coerce('true'), true);
  assert.equal(dispatcher.coerce('false'), false);
  assert.equal(dispatcher.coerce('null'), null);
  assert.equal(dispatcher.coerce('undefined'), undefined);
  assert.equal(dispatcher.coerce('ABC123'), 'ABC123');
  assert.equal(dispatcher.coerce(''), '');
  assert.equal(dispatcher.coerce(null), null);
  assert.equal(dispatcher.coerce(undefined), undefined);
});

test('splitArgs: empty / single / multi', () => {
  assert.deepEqual(dispatcher.splitArgs(''), []);
  assert.deepEqual(dispatcher.splitArgs(null), []);
  assert.deepEqual(dispatcher.splitArgs(undefined), []);
  assert.deepEqual(dispatcher.splitArgs('abc'), ['abc']);
  assert.deepEqual(dispatcher.splitArgs('abc,def'), ['abc', 'def']);
  // Type coercion happens via coerce(); splitArgs also applies it
  // per cell so callers can use the raw output.
  assert.deepEqual(dispatcher.splitArgs('0,abc,true,null'), [0, 'abc', true, null]);
  // Whitespace around cells is trimmed.
  assert.deepEqual(dispatcher.splitArgs(' 0 , abc , 1 '), [0, 'abc', 1]);
});

test('resolveSentinels: __value__ and __checked__ are read from el', () => {
  const inputLike = { value: 'hello', checked: true };
  assert.deepEqual(
    dispatcher.resolveSentinels(['__value__'], inputLike),
    ['hello']
  );
  assert.deepEqual(
    dispatcher.resolveSentinels(['code', '__checked__'], inputLike),
    ['code', true]
  );
  // Non-sentinel args pass through unchanged (even if they're
  // strings that LOOK like sentinels — we only resolve the
  // sentinels, not arbitrary values).
  assert.deepEqual(
    dispatcher.resolveSentinels(['a', 'b', 'c'], inputLike),
    ['a', 'b', 'c']
  );
  // Sentinels at the front, middle, and back all resolve.
  assert.deepEqual(
    dispatcher.resolveSentinels(['__value__', 'mid', '__checked__'], inputLike),
    ['hello', 'mid', true]
  );
});

test('resolveSentinels: __el__ passes the element itself (not its value)', () => {
  const fakeEl = { id: 'xsuaa-login', tagName: 'A' };
  const result = dispatcher.resolveSentinels(['__el__'], fakeEl);
  assert.equal(result.length, 1);
  assert.equal(result[0], fakeEl, '__el__ must resolve to the same element reference');
  assert.equal(result[0].id, 'xsuaa-login');
});

test('lookupAction: finds the function in any IE.* module', () => {
  // window.IE is the namespace populated by the modules. The
  // dispatcher walks it and returns the first match. We need a
  // mock function in one of the modules.
  global.window.IE = {
    testMod: { myAction: () => 'found it' },
    otherMod: { unrelated: () => 'no' }
  };
  const r = dispatcher.lookupAction('myAction');
  assert.ok(r);
  assert.equal(r.mod, 'testMod');
  assert.equal(typeof r.fn, 'function');
  assert.equal(r.fn(), 'found it');
  assert.equal(dispatcher.lookupAction('doesNotExist'), null);
});

test('lookupAction: skips util and state namespaces', () => {
  global.window.IE = {
    util: { onlyInUtil: () => 'should not match' },
    state: { onlyInState: () => 'should not match' },
    dispatcher: { onlyInDispatcher: () => 'should not match' }
  };
  assert.equal(dispatcher.lookupAction('onlyInUtil'), null);
  assert.equal(dispatcher.lookupAction('onlyInState'), null);
  assert.equal(dispatcher.lookupAction('onlyInDispatcher'), null);
});

test('onClick and onChange listeners are registered on document', () => {
  const types = recordedListeners.map((l) => l.type);
  assert.ok(types.includes('click'), 'should register a click listener');
  assert.ok(types.includes('change'), 'should register a change listener');
});

// End-to-end dispatch via a real DOM. Use a minimal manual DOM stub
// to avoid pulling in jsdom as a dependency.
test('dispatchClick: data-action routes to the right function', () => {
  let called = null;
  global.window.IE = {
    testMod: { handleIt: (a, b) => { called = { a, b }; } }
  };
  // Build a fake element with .closest() and .dataset.
  const el = {
    dataset: { action: 'handleIt', args: 'foo,42' },
    value: '', checked: false,
    closest: () => el
  };
  const event = { target: el };
  dispatcher.onClick(event);
  assert.deepEqual(called, { a: 'foo', b: 42 });
});

test('dispatchClick: no data-action ancestor => no-op', () => {
  let called = false;
  global.window.IE = {
    testMod: { handleIt: () => { called = true; } }
  };
  // closest returns null when there's no match.
  const el = { dataset: {}, closest: () => null };
  dispatcher.onClick({ target: el });
  assert.equal(called, false);
});

test('dispatchClick: unknown action logs a warning (does not throw)', () => {
  global.window.IE = { testMod: {} };
  const el = {
    dataset: { action: 'doesNotExist' },
    closest: () => el
  };
  // Should not throw.
  dispatcher.onClick({ target: el });
});

test('dispatchClick: handler that throws is caught and logged', () => {
  let called = false;
  global.window.IE = {
    testMod: { explodes: () => { called = true; throw new Error('boom'); } }
  };
  const el = {
    dataset: { action: 'explodes' },
    closest: () => el
  };
  // Should not propagate the throw.
  dispatcher.onClick({ target: el });
  assert.equal(called, true);
});

test('dispatchClick: __value__ sentinel reads from element.value', () => {
  let got = null;
  global.window.IE = {
    testMod: { pickValue: (v) => { got = v; } }
  };
  const el = {
    dataset: { action: 'pickValue', args: '__value__' },
    value: 'ABC123',
    checked: false,
    closest: () => el
  };
  dispatcher.onClick({ target: el });
  assert.equal(got, 'ABC123');
});

test('dispatchClick: __checked__ sentinel reads from element.checked', () => {
  let got = null;
  global.window.IE = {
    testMod: { pickChecked: (v) => { got = v; } }
  };
  const el = {
    dataset: { action: 'pickChecked', args: '__checked__' },
    value: '',
    checked: true,
    closest: () => el
  };
  dispatcher.onClick({ target: el });
  assert.equal(got, true);
});

test('dispatchClick: closest() finds an ancestor with data-action', () => {
  // The DOM is structured so a child click bubbles up; the
  // dispatcher's .closest() walks the chain to find the action.
  // We simulate this by giving the event target a parent that
  // has the data-action.
  let called = false;
  global.window.IE = {
    testMod: { bubbleMe: () => { called = true; } }
  };
  const actionEl = {
    dataset: { action: 'bubbleMe' },
    value: '',
    checked: false
  };
  const childEl = { closest(sel) { return sel === '[data-action]' ? actionEl : null; } };
  dispatcher.onClick({ target: childEl });
  assert.equal(called, true);
});

test('dispatchClick: handler that returns a Promise catches async errors', async () => {
  let warned = null;
  const origConsole = console.error;
  console.error = (msg, err) => { warned = { msg, err }; };
  global.window.IE = {
    testMod: { asyncBoom: async () => { throw new Error('async boom'); } }
  };
  const el = {
    dataset: { action: 'asyncBoom' },
    closest: () => el,
    value: '',
    checked: false
  };
  dispatcher.onClick({ target: el });
  // Let the microtask queue drain.
  await new Promise((r) => setImmediate(r));
  console.error = origConsole;
  assert.ok(warned, 'async error should be logged');
  assert.match(warned.msg, /asyncBoom/);
  assert.equal(warned.err.message, 'async boom');
});
