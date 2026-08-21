'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const config = require('../xs-security.json');

test('XSUAA role templates grant exactly one matching scope', () => {
  const expected = {
    Admin: '$XSAPPNAME.admin',
    Manager: '$XSAPPNAME.manager',
    Reviewer: '$XSAPPNAME.reviewer',
    ContentEditor: '$XSAPPNAME.content_editor'
  };
  assert.equal(config.roleTemplateList.length, Object.keys(expected).length);
  for (const template of config.roleTemplateList) {
    assert.deepEqual(template.scopeReferences, [expected[template.name]]);
  }
});
