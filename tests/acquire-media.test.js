'use strict';

// Tests for the acquireWebcam / acquireScreen split in
// client/code-entry.js. The original code-entry.js had
// reqWebcam() and reqScreen() which conflated two
// responsibilities:
//
//   1. acquire the MediaStream via getUserMedia /
//      getDisplayMedia, store it on S, and wire the
//      hidden-cam + exam-cam elements
//   2. update the Tech Check screen's status pills +
//      buttons (st-cam, btn-cam, btn-screen, etc.)
//
// The in-exam reconnect path (the `ended` handler on a
// MediaStreamTrack) needed to call back into the same code
// to re-acquire the stream. But the Tech Check DOM does
// not exist during the exam, so the original conflated
// functions would throw on null refs and the candidate
// would see no recovery.
//
// The fix splits these into:
//   - acquireWebcam() / acquireScreen()  — pure media
//     acquisition. Touches ONLY hidden-cam + exam-cam,
//     which exist on every screen.
//   - reqWebcam() / reqScreen()          — Tech Check
//     wrappers. Call acquire*, then update the Tech
//     Check DOM.
//
// These tests verify the contract of acquireWebcam /
// acquireScreen: they do NOT touch the Tech Check DOM,
// and they do not require any Tech Check elements to
// exist on the page.

const test = require('node:test');
const assert = require('node:assert/strict');

function makeFakeTrack() {
  const state = { readyState: 'live', listeners: {} };
  return {
    readyState: state.readyState,
    stop: () => { state.readyState = 'ended'; },
    addEventListener: (event, fn) => {
      state.listeners[event] = state.listeners[event] || [];
      state.listeners[event].push(fn);
    },
    _fire: (event) => {
      state.readyState = 'ended';
      for (const fn of (state.listeners[event] || [])) fn();
    },
    _getState: () => state
  };
}

function makeFakeStream(tracks) {
  return {
    getTracks: () => tracks,
    getVideoTracks: () => tracks
  };
}

function makeFakeVideo() {
  return { srcObject: null };
}

function loadCodeEntryModule({ S, domIds, mediaDevices } = {}) {
  delete require.cache[require.resolve('../client/code-entry.js')];
  // The state IIFE runs at load time and expects root.S to
  // be a global object with the right shape. We pre-populate
  // it; the code-entry IIFE only reads from it.
  const S_default = {
    screen: 'tech-check',
    code: 'ABC2DE',
    webcamOk: false,
    screenOk: false,
    webcamStream: null,
    screenStream: null
  };
  const S_final = Object.assign(S_default, S || {});

  // Build a minimal document with the elements the
  // module will look up. domIds is a list of which
  // elements exist; missing IDs return null from
  // getElementById, mimicking the exam view where the
  // Tech Check elements don't exist.
  const elements = {};
  for (const id of (domIds || [])) elements[id] = makeFakeVideo();

  // Track every getElementById call so we can assert
  // acquireWebcam / acquireScreen never touch Tech
  // Check IDs (st-cam, btn-cam, st-screen, btn-screen,
  // btn-start, cam-preview, preview-vid).
  const requestedIds = [];
  global.document = {
    getElementById: (id) => {
      requestedIds.push(id);
      return elements[id] || null;
    }
  };

  // Stub navigator.mediaDevices.getUserMedia /
  // getDisplayMedia with controllable behavior.
  let nextStream = null;
  let getUserMediaError = null;
  let getDisplayMediaError = null;
  const calls = { getUserMedia: [], getDisplayMedia: [] };
  const md = {
    getUserMedia: async (constraints) => {
      calls.getUserMedia.push(constraints);
      if (getUserMediaError) throw getUserMediaError;
      return nextStream;
    },
    getDisplayMedia: async (constraints) => {
      calls.getDisplayMedia.push(constraints);
      if (getDisplayMediaError) throw getDisplayMediaError;
      return nextStream;
    }
  };

  // The code-entry module reads navigator.mediaDevices at
  // call time (inside the function bodies), so we can swap
  // it per test by setting it on the global before loading.
  Object.defineProperty(global, 'navigator', {
    value: { mediaDevices: mediaDevices || md },
    writable: true, configurable: true
  });

  // Window stub: S + IE.state + IE.util stubs.
  const logIncidentCalls = [];
  global.window = {
    S: S_final,
    IE: {
      state: {
        logIncident: (type, detail) => logIncidentCalls.push({ type, detail })
      },
      util: {
        modal: (_icon, _title, _body, _buttons) => {}
      }
    }
  };

  // Trigger the IIFE so it attaches IE.codeEntry to the
  // global window. require() returns the module.exports
  // (which may be empty for UMD/IIFE modules); we read
  // the API from the global namespace instead.
  require('../client/code-entry.js');
  const codeEntry = global.window.IE.codeEntry;
  return { codeEntry, S: S_final, calls, logIncidentCalls, requestedIds };
}

// We don't actually invoke the IIFE methods directly
// (they're private). Instead we test through the surface
// that matters: does acquireWebcam touch Tech Check DOM?
// We can read code-entry.js source and grep for the
// expected behavior, then verify by inspecting the
// module's exposed functions exist.

test('code-entry.js: reqWebcam + reqScreen exist as exports', () => {
  const { codeEntry } = loadCodeEntryModule();
  assert.equal(typeof codeEntry.reqWebcam, 'function',
    'reqWebcam must be exposed for the Tech Check button data-action="reqWebcam"');
  assert.equal(typeof codeEntry.reqScreen, 'function',
    'reqScreen must be exposed for the Tech Check button data-action="reqScreen"');
});

test('code-entry.js: source confirms acquireWebcam exists separately from reqWebcam', () => {
  // The split is the whole point. If a future refactor
  // re-conflates them, this test catches it. We don't
  // run the functions (they need a real getUserMedia +
  // DOM); we just verify the symbols are present in the
  // module source.
  const fs = require('node:fs');
  const path = require('node:path');
  const src = fs.readFileSync(
    path.join(__dirname, '..', 'client', 'code-entry.js'),
    'utf8'
  );
  assert.ok(/function acquireWebcam\s*\(/.test(src),
    'client/code-entry.js must define acquireWebcam() separately from reqWebcam()');
  assert.ok(/function acquireScreen\s*\(/.test(src),
    'client/code-entry.js must define acquireScreen() separately from reqScreen()');
  assert.ok(/function onWebcamTrackEnded\s*\(/.test(src),
    'client/code-entry.js must define onWebcamTrackEnded() as a separate function');
  assert.ok(/function onScreenTrackEnded\s*\(/.test(src),
    'client/code-entry.js must define onScreenTrackEnded() as a separate function');
});

test('code-entry.js: reqWebcam calls acquireWebcam, not the other way around', () => {
  // The Tech Check wrapper should delegate to the pure
  // media function. We grep for the call relationship.
  const fs = require('node:fs');
  const path = require('node:path');
  const src = fs.readFileSync(
    path.join(__dirname, '..', 'client', 'code-entry.js'),
    'utf8'
  );
  // Find reqWebcam body and confirm it calls acquireWebcam.
  const reqWebcamMatch = src.match(/async function reqWebcam\s*\(\)\s*\{([\s\S]*?)\n\s*\}/);
  assert.ok(reqWebcamMatch, 'reqWebcam not found');
  assert.ok(reqWebcamMatch[1].includes('acquireWebcam()'),
    'reqWebcam should delegate to acquireWebcam()');
  // And the opposite direction must NOT happen — look only
  // inside the acquireWebcam function body, not anywhere in
  // the file (a comment elsewhere is fine).
  const acquireWebcamMatch = src.match(/async function acquireWebcam\s*\(\)\s*\{([\s\S]*?)\n  \}/);
  assert.ok(acquireWebcamMatch, 'acquireWebcam not found');
  assert.ok(!acquireWebcamMatch[1].includes('reqWebcam('),
    'acquireWebcam must not call reqWebcam (would re-introduce the conflation)');
});

test('code-entry.js: in-exam ended handler does not touch Tech Check DOM', () => {
  // The most important regression test: when the webcam
  // track ends, the recovery path must NEVER touch
  // st-cam / btn-cam / cam-preview / preview-vid (those
  // elements don't exist on the exam view).
  const fs = require('node:fs');
  const path = require('node:path');
  const src = fs.readFileSync(
    path.join(__dirname, '..', 'client', 'code-entry.js'),
    'utf8'
  );
  const match = src.match(/function onWebcamTrackEnded\s*\(\)\s*\{([\s\S]*?)\n  \}/);
  assert.ok(match, 'onWebcamTrackEnded not found');
  const body = match[1];
  // Must touch safe elements (these run on every screen).
  assert.ok(body.includes("'exam-cam'"),
    'onWebcamTrackEnded should clear exam-cam (the candidate-facing corner feed)');
  assert.ok(body.includes("'hidden-cam'"),
    'onWebcamTrackEnded should clear hidden-cam so drawImage() returns null');
  // st-cam / btn-cam are only safe to touch in the
  // tech-check branch. The in-exam branch (after the
  // early return) must NOT touch them.
  const parts = body.split(/return;\s*\n/);
  assert.ok(parts.length >= 2, 'expected the early return + mid-exam branch split');
  const inExamBranch = parts.slice(1).join('return;\n');
  assert.ok(!inExamBranch.includes("'st-cam'"),
    'in-exam branch of onWebcamTrackEnded must not touch st-cam (Tech Check only)');
  assert.ok(!inExamBranch.includes("'btn-cam'"),
    'in-exam branch of onWebcamTrackEnded must not touch btn-cam (Tech Check only)');
  assert.ok(!inExamBranch.includes("'cam-preview'"),
    'in-exam branch of onWebcamTrackEnded must not touch cam-preview (Tech Check only)');
  assert.ok(!inExamBranch.includes("'preview-vid'"),
    'in-exam branch of onWebcamTrackEnded must not touch preview-vid (Tech Check only)');
  // The in-exam branch should call acquireWebcam() (the
  // pure media function, not the Tech Check wrapper).
  // We match a call site, not just any mention in a
  // comment — `acquireWebcam().catch(` is the actual
  // invocation pattern in the source.
  assert.ok(/acquireWebcam\(\)\.catch/.test(inExamBranch),
    'in-exam branch should call acquireWebcam() directly (safe during exam)');
});

test('code-entry.js: in-exam screen ended handler does not allow I-understand bypass', () => {
  // The previous behavior showed a modal with "I understand"
  // as the only action — which let the candidate continue
  // without screen sharing. The fix must require an actual
  // Reconnect action.
  const fs = require('node:fs');
  const path = require('node:path');
  const src = fs.readFileSync(
    path.join(__dirname, '..', 'client', 'code-entry.js'),
    'utf8'
  );
  const match = src.match(/function onScreenTrackEnded\s*\(\)\s*\{([\s\S]*?)\n  \}/);
  assert.ok(match, 'onScreenTrackEnded not found');
  const body = match[1];
  // S.screenOk = false is set BEFORE the early return
  // (applies to both the tech-check and the in-exam paths
  // — both want the proctor to know the track is gone).
  assert.ok(body.includes('S.screenOk = false'),
    'onScreenTrackEnded must set S.screenOk = false (regardless of screen)');
  // The in-exam branch (after the early return) is what
  // shows the recovery modal.
  const parts = body.split(/return;\s*\n/);
  assert.ok(parts.length >= 2, 'expected early return + mid-exam branch');
  const inExamBranch = parts.slice(1).join('return;\n');
  assert.ok(inExamBranch.includes('Reconnect Screen'),
    'in-exam branch of onScreenTrackEnded must offer a Reconnect Screen action');
  // Must NOT have an "I understand" / "OK" bypass as the
  // only action in the in-exam modal.
  assert.ok(!/label:\s*'I understand'/.test(inExamBranch),
    'in-exam branch of onScreenTrackEnded must not offer an I-understand bypass');
});
