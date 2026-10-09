'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

// These cases exercise pure validation/grading plus the SQL calls issued by
// persistence helpers. Keep them runnable without a HANA client installation.
const dbPath = require.resolve('../lib/core/db.js');
const dbMock = { withConnection: null, exec: null, currentIdentity: null, tableExists: null };
require.cache[dbPath] = {
  id: dbPath,
  filename: dbPath,
  loaded: true,
  exports: {
    withConnection: (...args) => dbMock.withConnection(...args),
    exec: (...args) => dbMock.exec(...args),
    currentIdentity: (...args) => dbMock.currentIdentity(...args),
    tableExists: (...args) => dbMock.tableExists(...args)
  }
};

// The unit runner preloads bootstrap.js, which may load these modules first.
// Reload them against the controlled DB seam above instead of retaining a
// production connection helper captured during bootstrap.
delete require.cache[require.resolve('../lib/core/question-sets.js')];
delete require.cache[require.resolve('../lib/core/submit.js')];

const { normalizeUpload } = require('../lib/core/question-sets.js');
const {
  resultRecord,
  sanitizeCandidateResult,
  writeImmutableResult,
  submitHandler
} = require('../lib/core/submit.js');

function uploadQuestion(qNum, section = {}) {
  return {
    qNum,
    stem: `Question ${qNum}`,
    opts: ['A', 'B', 'C'],
    correctIndices: [0],
    ...section
  };
}

function responseRecorder() {
  return {
    statusCode: 200,
    body: null,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; return this; }
  };
}

function setupDbMock({ exec, identities = [], existingResult = null } = {}) {
  const calls = [];
  let identityIndex = 0;
  let transactionOptions = null;
  const conn = {
    exec(sql, params, callback) {
      calls.push({ sql, params });
      if (sql.includes('SELECT RESULT_JSON FROM EXAM_RESULTS')) {
        callback(null, existingResult ? [{ RESULT_JSON: JSON.stringify(existingResult) }] : []);
      } else if (exec) {
        try { callback(null, exec(sql, params)); }
        catch (err) { callback(err); }
      } else callback(null, []);
    }
  };
  dbMock.withConnection = async (fn, options) => {
    transactionOptions = options;
    return fn(conn);
  };
  dbMock.exec = (connection, sql, params = []) => new Promise((resolve, reject) => {
    connection.exec(sql, params, (err, rows) => err ? reject(err) : resolve(rows || []));
  });
  dbMock.currentIdentity = async () => identities[identityIndex++];
  dbMock.tableExists = async () => false;
  return {
    conn,
    calls,
    get transactionOptions() { return transactionOptions; }
  };
}

test('section upload groups matching section metadata and preserves unsectioned questions', () => {
  const result = normalizeUpload([
    uploadQuestion(1, { sectionName: 'Basics', sectionDescription: 'Core ideas', sectionOrder: 2, sectionDrawCount: 1 }),
    uploadQuestion(2, { sectionName: ' basics ', sectionDescription: 'Core ideas', sectionOrder: 2, sectionDrawCount: 1 }),
    uploadQuestion(3)
  ]);

  assert.equal(result.ok, true);
  assert.equal(result.sections.length, 1);
  assert.equal(result.sections[0].name, 'Basics');
  assert.equal(result.sections[0].questionCount, 2);
  assert.equal(result.normalized[0].sectionKey, 'basics');
  assert.equal(result.normalized[2].sectionKey, null);
});

test('section upload rejects conflicting metadata and draw quotas above available questions', () => {
  const conflict = normalizeUpload([
    uploadQuestion(1, { sectionName: 'Basics', sectionDescription: 'First description' }),
    uploadQuestion(2, { sectionName: 'basics', sectionDescription: 'Different description' })
  ]);
  assert.equal(conflict.ok, false);
  assert.match(conflict.errors.join(' '), /metadata conflicts/i);

  const quota = normalizeUpload([
    uploadQuestion(1, { sectionName: 'Basics', sectionDrawCount: 2 })
  ]);
  assert.equal(quota.ok, false);
  assert.match(quota.errors.join(' '), /exceeds its question count/i);
});

test('result record includes section aggregation computed from the exam session snapshot', () => {
  const questions = [
    { opts: ['A', 'B'], answer: [0], multi: false, sectionId: 8, sectionName: 'Basics', sectionOrder: 1 },
    { opts: ['A', 'B'], answer: [1], multi: false, sectionId: 9, sectionName: 'Advanced', sectionOrder: 2 }
  ];
  const req = {
    examSession: {
      code: 'AB1234', questionSetId: 42, questionSetName: 'Sample', examMode: 'GRADED',
      countsTowardResults: true, durationSecs: 900, passPct: 50,
      questions, qOrder: [0, 1], optOrders: [[0, 1], [0, 1]], total: 2,
      answerKey: [[0], [1]]
    },
    body: { code: 'AB1234', answers: [[0], [0]], durationSecs: 300 }
  };

  const record = resultRecord(req);
  assert.equal(record.score, 1);
  assert.equal(record.total, 2);
  assert.deepEqual(record.sectionResults.map((section) => [section.sectionId, section.correct, section.total]), [
    [8, 1, 1], [9, 0, 1]
  ]);
});

test('candidate result keeps section breakdown when correct answers are hidden', () => {
  const result = sanitizeCandidateResult({
    examMode: 'GRADED',
    questionResults: [{ correct: true }],
    sectionResults: [{ sectionId: 8, correct: 1, total: 1 }]
  });

  assert.equal(Object.hasOwn(result, 'questionResults'), false);
  assert.deepEqual(result.sectionResults, [{ sectionId: 8, correct: 1, total: 1 }]);
});

test('result persistence writes section breakdown and completes the access code', async () => {
  const db = setupDbMock();
  const record = {
    code: 'AB1234', score: 1, total: 2, pct: 50, pass: false, countsTowardResults: true,
    autoSubmit: false, durationSecs: 300, tabSwitches: 0, incidentCount: 0,
    sectionResults: [{ sectionId: 8, name: 'Basics', correct: 1, total: 1, pct: 100 }]
  };

  await writeImmutableResult(db.conn, record);

  assert.match(db.calls[0].sql, /INSERT INTO EXAM_RESULTS/);
  assert.deepEqual(JSON.parse(db.calls[0].params[9]).sectionResults, record.sectionResults);
  assert.match(db.calls[1].sql, /UPDATE ACCESS_CODES/);
  assert.deepEqual(db.calls[1].params.slice(0, 4), [1, 50, 0, 'AB1234']);
  assert.match(db.calls[2].sql, /DELETE FROM EXAM_SESSIONS/);
});

test('upload handler inserts sections first and links each question to its section identity', async () => {
  const db = setupDbMock({ identities: [101, 202] });
  const res = responseRecorder();

  await require('../lib/core/question-sets.js').uploadHandler({
    body: {
      name: 'Sectioned exam',
      questions: [uploadQuestion(1, { sectionName: 'Basics', sectionDrawCount: 1 })]
    },
    adminRole: 'admin',
    ip: '127.0.0.1'
  }, res);

  assert.equal(res.statusCode, 200, JSON.stringify(res.body));
  assert.equal(res.body.sectionCount, 1);
  assert.deepEqual(db.transactionOptions, { transaction: true });
  const sectionInsert = db.calls.findIndex(({ sql }) => sql.includes('INSERT INTO QUESTION_SECTIONS'));
  const questionInsert = db.calls.findIndex(({ sql }) => sql.includes('INSERT INTO QUESTION_SET_QUESTIONS'));
  assert.ok(sectionInsert >= 0 && questionInsert > sectionInsert);
  assert.equal(db.calls[questionInsert].params[1], 202);
});

test('submission replay returns the existing section result without inserting a duplicate', async () => {
  const existingResult = {
    code: 'AB1234', score: 1, total: 1,
    sectionResults: [{ sectionId: 8, name: 'Basics', correct: 1, total: 1, pct: 100 }]
  };
  const db = setupDbMock({ existingResult });
  const res = responseRecorder();

  await submitHandler({
    examSession: { code: 'AB1234' },
    body: { code: 'AB1234', answers: [] }
  }, res);

  assert.equal(res.statusCode, 200, JSON.stringify(res.body));
  assert.equal(res.body.idempotentReplay, true);
  assert.deepEqual(res.body.result.sectionResults, existingResult.sectionResults);
  assert.deepEqual(db.transactionOptions, { transaction: true });
  assert.equal(db.calls.some(({ sql }) => sql.includes('INSERT INTO EXAM_RESULTS')), false);
});
