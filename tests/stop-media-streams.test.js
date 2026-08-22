'use strict';

// Tests for client/state.js#stopMediaStreams — the cleanup
// helper that stops live MediaStream tracks when the
// candidate leaves the exam. Without it, the webcam +
// screen share keep running after submit/reset and burn
// memory + keep the "REC" indicator on.
//
// We stub the browser MediaStream / Track surface in plain
// JS and verify the helper calls .stop() on every live
// track, clears srcObject on the live <video> elements,
// and nulls the references on root.S.

const test = require('node:test');
const assert = require('node:assert/strict');

function makeFakeTrack(opts = {}) {
  const state = { readyState: opts.alreadyStopped ? 'ended' : 'live' };
  return {
    readyState: state.readyState,
    stop: () => { state.readyState = 'ended'; },
    _getState: () => state
  };
}
function makeFakeStream(tracks) {
  return { getTracks: () => tracks };
}
function makeFakeVideo() {
  return { srcObject: null };
}
function loadStateModule(S) {
  // Reset module cache so the IIFE re-reads root.S on load.
  delete require.cache[require.resolve('../client/state.js')];
  // Install a minimal window with S. The state IIFE
  // attaches to root.S at the top.
  Object.defineProperty(global, 'window', {
    value: { S },
    writable: true,
    configurable: true
  });
  // Also install a minimal document for the getElementById
  // lookups inside stopMediaStreams. We deliberately do
  // NOT restore global.document in a finally — the test
  // calls state.stopMediaStreams() AFTER loadStateModule
  // returns, and we still need the stub to be live at
  // that point. Tests that explicitly delete document
  // ("never throws even if document.getElementById is
  // missing") clear it themselves after loading.
  const videoEls = { 'hidden-cam': makeFakeVideo(), 'preview-vid': makeFakeVideo() };
  global.document = { getElementById: (id) => videoEls[id] || null };
  return { state: require('../client/state.js'), videoEls };
}

test('isProctorRecoveryRequired: remains true until both required streams recover', () => {
  const S = {
    screen: 'exam',
    proctorOn: true,
    webcamOk: false,
    screenOk: false
  };
  const { state } = loadStateModule(S);

  assert.equal(state.isProctorRecoveryRequired(), true, 'both streams missing blocks the exam');
  S.screenOk = true;
  assert.equal(state.isProctorRecoveryRequired(), true, 'recovering screen must not clear missing webcam');
  S.webcamOk = true;
  assert.equal(state.isProctorRecoveryRequired(), false, 'exam unblocks only after both streams recover');
});

test('isProctorRecoveryRequired: does not apply when proctoring is disabled', () => {
  const { state } = loadStateModule({
    screen: 'exam',
    proctorOn: false,
    webcamOk: false,
    screenOk: false
  });
  assert.equal(state.isProctorRecoveryRequired(), false);
});

test('stopMediaStreams: stops every live track on the webcam + screen streams', () => {
  const camTrack1 = makeFakeTrack();
  const camTrack2 = makeFakeTrack();
  const screenTrack = makeFakeTrack();
  const S = {
    webcamStream: makeFakeStream([camTrack1, camTrack2]),
    screenStream: makeFakeStream([screenTrack])
  };
  const { state } = loadStateModule(S);
  state.stopMediaStreams();
  assert.equal(camTrack1._getState().readyState, 'ended');
  assert.equal(camTrack2._getState().readyState, 'ended');
  assert.equal(screenTrack._getState().readyState, 'ended');
});

test('stopMediaStreams: does not call .stop() on already-ended tracks', () => {
  let stopCallCount = 0;
  const liveTrack = { readyState: 'live', stop: () => { stopCallCount += 1; } };
  const endedTrack = { readyState: 'ended', stop: () => { stopCallCount += 1; } };
  const S = {
    webcamStream: makeFakeStream([liveTrack, endedTrack]),
    screenStream: null
  };
  const { state } = loadStateModule(S);
  state.stopMediaStreams();
  assert.equal(stopCallCount, 1, 'only the live track should be stopped');
});

test('stopMediaStreams: nulls the stream references on root.S', () => {
  let trackStopped = false;
  const track = { readyState: 'live', stop: () => { trackStopped = true; } };
  const S = {
    webcamStream: { getTracks: () => [track] },
    screenStream: { getTracks: () => [track] },
    webcamOk: true,
    screenOk: true,
    screen: 'results',
    submitted: true
  };
  const { state } = loadStateModule(S);
  state.stopMediaStreams();
  assert.equal(trackStopped, true);
  assert.equal(S.webcamStream, null);
  assert.equal(S.screenStream, null);
  assert.equal(S.webcamOk, false);
  assert.equal(S.screenOk, false);
});

test('stopMediaStreams: clears srcObject on the hidden-cam and preview-vid <video> elements', () => {
  const camVideo = { srcObject: 'live-stream' };
  const previewVideo = { srcObject: 'live-stream' };
  const S = {
    webcamStream: makeFakeStream([makeFakeTrack()]),
    screenStream: null
  };
  const prevWindow = global.window;
  const prevDocument = global.document;
  Object.defineProperty(global, 'window', { value: { S }, writable: true, configurable: true });
  global.document = {
    getElementById: (id) => id === 'hidden-cam' ? camVideo : (id === 'preview-vid' ? previewVideo : null)
  };
  delete require.cache[require.resolve('../client/state.js')];
  const state = require('../client/state.js');
  try {
    state.stopMediaStreams();
  } finally {
    if (prevWindow) Object.defineProperty(global, 'window', prevWindow);
    if (prevDocument) global.document = prevDocument;
    else delete global.document;
  }
  assert.equal(camVideo.srcObject, null);
  assert.equal(previewVideo.srcObject, null);
});

test('stopMediaStreams: is a no-op when no streams are set', () => {
  const S = { webcamStream: null, screenStream: null };
  const { state } = loadStateModule(S);
  // Should not throw.
  assert.doesNotThrow(() => state.stopMediaStreams());
  assert.equal(S.webcamStream, null);
  assert.equal(S.screenStream, null);
});

test('stopMediaStreams: handles a stream with no getTracks gracefully', () => {
  const brokenStream = { /* no getTracks */ };
  const S = { webcamStream: brokenStream, screenStream: null };
  const { state } = loadStateModule(S);
  assert.doesNotThrow(() => state.stopMediaStreams());
});

test('stopMediaStreams: one track throwing .stop() does not block the others', () => {
  const goodTrack = makeFakeTrack();
  const badTrack = { readyState: 'live', stop: () => { throw new Error('hardware failure'); } };
  const S = {
    webcamStream: makeFakeStream([goodTrack, badTrack]),
    screenStream: makeFakeStream([makeFakeTrack()])
  };
  const { state } = loadStateModule(S);
  assert.doesNotThrow(() => state.stopMediaStreams());
  assert.equal(goodTrack._getState().readyState, 'ended', 'good track should still be stopped');
  // S.webcamStream and S.screenStream are nulled in the
  // final pass regardless of which tracks threw.
  assert.equal(S.webcamStream, null);
  assert.equal(S.screenStream, null);
});

test('stopMediaStreams: never throws even if document.getElementById is missing', () => {
  // Some test environments have no document at all.
  const S = { webcamStream: makeFakeStream([makeFakeTrack()]), screenStream: null };
  const prevWindow = global.window;
  const prevDocument = global.document;
  Object.defineProperty(global, 'window', { value: { S }, writable: true, configurable: true });
  delete global.document;
  delete require.cache[require.resolve('../client/state.js')];
  const state = require('../client/state.js');
  try {
    assert.doesNotThrow(() => state.stopMediaStreams());
  } finally {
    if (prevWindow) Object.defineProperty(global, 'window', prevWindow);
    if (prevDocument) global.document = prevDocument;
  }
});
