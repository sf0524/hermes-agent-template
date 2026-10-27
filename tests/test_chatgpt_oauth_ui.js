/* Async UI regressions without a browser or live OAuth account. */
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const html = fs.readFileSync(path.join(__dirname, '../templates/index.html'), 'utf8');
const source = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(m => m[1]).join('\n');
function fixture(fetch) {
  const context = {window: {}, clearTimeout() {}, setTimeout() {}, confirm: () => true, fetch};
  vm.runInNewContext(source, context);
  const app = context.app();
  app.notifications = [];
  app.notify = (...args) => app.notifications.push(args);
  app.scheduleCodexPoll = () => { app.pollScheduled = true; };
  return app;
}

test('failed cancellation keeps the pending flow and never announces success', async () => {
  const app = fixture();
  app.codex.status = 'waiting';
  app.codexRequest = async () => { throw Error('connection lost'); };
  await app.cancelCodex();
  assert.equal(app.codex.status, 'waiting');
  assert.equal(app.codex.busy, false);
  assert.equal(app.pollScheduled, true);
  assert.equal(app.notifications.length, 1);
  assert.equal(app.notifications[0][1], false);
});

test('a stale poll response cannot revive a cancelled flow', async () => {
  const app = fixture();
  let resolve;
  app.codexRequest = () => new Promise(done => { resolve = done; });
  const pending = app.loadCodexStatus();
  ++app.codex.sequence;
  resolve({status: 'waiting', user_code: 'STALE'});
  await pending;
  assert.equal(app.codex.status, 'not_connected');
  assert.equal(app.codex.user_code, undefined);
});

test('reloading a pending login restores the provider without reusing another model', async () => {
  const app = fixture();
  app.vars.LLM_MODEL = 'another-providers-model';
  app.codexRequest = async () => ({status: 'waiting', user_code: 'ABC-123'});
  await app.loadCodexStatus();
  assert.equal(app.selectedProvider, 'ChatGPT (OAuth)');
  assert.equal(app.vars.LLM_MODEL, '');
  app.selectedProvider = 'MiniMax';
  await app.loadCodexStatus();
  assert.equal(app.selectedProvider, 'MiniMax');
});

test('transient polling failures retain the code and keep checking', async () => {
  const app = fixture();
  app.codex.status = 'waiting';
  app.codex.user_code = 'ABC-123';
  app.codexRequest = async () => { throw Error('temporary failure'); };
  await app.loadCodexStatus();
  assert.equal(app.codex.status, 'waiting');
  assert.equal(app.codex.user_code, 'ABC-123');
  assert.equal(app.pollScheduled, true);
});

test('failed disconnect does not hide the existing connection', async () => {
  const app = fixture();
  app.codex.connected = true;
  app.codexRequest = async () => { throw Error('stop not confirmed'); };
  await app.disconnectCodex();
  assert.equal(app.codex.connected, true);
  assert.equal(app.codex.busy, false);
  assert.equal(app.notifications[0][1], false);
});

test('save refuses an unauthorized or unlisted ChatGPT model', async () => {
  const app = fixture();
  app.selectedProvider = 'ChatGPT (OAuth)';
  app.vars.LLM_MODEL = 'unlisted';
  await app.saveConfig();
  assert.equal(app.saving, false);
  assert.equal(app.notifications[0][1], false);
});

test('confirm model persists only the model and keeps other drafts without starting', async () => {
  let body;
  const app = fixture(async (_, options) => {
    body = JSON.parse(options.body);
    return {ok: true, json: async () => ({ok: true, config_revision: 'new', config_complete: true})};
  });
  app.configRevision = 'old';
  app.vars = {LLM_MODEL: 'chosen', EMAIL_ADDRESS: 'unsaved-draft@example.invalid'};
  app.codex.connected = true;
  app.codex.models = ['chosen'];
  await app.confirmCodexModel();
  assert.deepEqual(body.vars, {LLM_MODEL: 'chosen'});
  assert.equal(body._restart, false);
  assert.equal(body._active_oauth_provider, 'openai-codex');
  assert.equal(app.codex.confirmedModel, 'chosen');
  assert.equal(app.configRevision, 'new');
  assert.equal(app.vars.EMAIL_ADDRESS, 'unsaved-draft@example.invalid');
});

test('failed confirmation cannot announce a model as saved', async () => {
  const app = fixture(async () => ({ok: false, json: async () => ({error: 'Settings changed; reload.'})}));
  app.configRevision = 'old';
  app.vars.LLM_MODEL = 'chosen';
  app.codex.connected = true;
  app.codex.models = ['chosen'];
  await app.confirmCodexModel();
  assert.equal(app.codex.confirmedModel, '');
  assert.equal(app.configRevision, 'old');
  assert.equal(app.notifications[0][1], false);
});

test('the configured-provider summary shows the saved model rather than a draft', () => {
  const app = fixture();
  app.codex.connected = true;
  app.codex.confirmedModel = 'saved';
  app.vars.LLM_MODEL = 'unconfirmed-draft';
  assert.equal(app.configuredProviders.find(p => p.providerKey === 'ChatGPT (OAuth)').model, 'saved');
});

function savingFixture() {
  const sent = [];
  const app = fixture(async (url, options) => {
    if (options && options.method === 'PUT') sent.push(JSON.parse(options.body));
    return {ok: true, json: async () => ({ok: true})};
  });
  app.loadConfig = async () => {};
  app.savedConfigComplete = true;
  app.hasSavedAssignment = true;
  app.modelProvider = 'google';
  app.configRevision = 'rev';
  return {app, sent};
}

test('API-key-only save with a stale auto-filled hint does not request a model assignment', async () => {
  const {app, sent} = savingFixture();
  app.vars._MODEL_GEMINI_API_KEY = 'gemini-old-hint';
  app.vars.LLM_MODEL = 'gemini-newer-native';
  app.selectedProvider = 'Google AI Studio';
  app.onProviderChange();
  assert.equal(app.vars.LLM_MODEL, 'gemini-old-hint');
  app.vars.GEMINI_API_KEY = 'AIza-new-key';
  await app.saveConfig();
  assert.equal(sent.length, 1);
  assert.equal(sent[0]._model_assignment, false);
  assert.equal(sent[0]._active_provider_key, 'GEMINI_API_KEY');
  assert.equal(sent[0].vars.LLM_MODEL, 'gemini-old-hint');
});

test('switching to a provider without a hint cannot pin the carried-over model', async () => {
  const {app, sent} = savingFixture();
  app.vars.LLM_MODEL = 'model-of-the-current-provider';
  app.selectedProvider = 'DeepSeek';
  app.onProviderChange();
  app.vars.DEEPSEEK_API_KEY = 'sk-new';
  await app.saveConfig();
  assert.equal(sent[0]._model_assignment, false);
});

test('a provider change discards an earlier deliberate edit because the field is re-filled', async () => {
  const {app, sent} = savingFixture();
  app.selectedProvider = 'DeepSeek';
  app.modelIntent = true;
  app.selectedProvider = 'Google AI Studio';
  app.onProviderChange();
  await app.saveConfig();
  assert.equal(sent[0]._model_assignment, false);
});

test('a deliberate model edit or same-value confirmation requests assignment', async () => {
  for (const act of [
    app => { app.vars.LLM_MODEL = 'typed'; app.modelIntent = true; },
    app => { app.modelIntent = true; }, // "Use this model" on the unchanged hint
  ]) {
    const {app, sent} = savingFixture();
    app.vars._MODEL_GEMINI_API_KEY = 'gemini-hint';
    app.selectedProvider = 'Google AI Studio';
    app.onProviderChange();
    act(app);
    await app.saveConfig();
    assert.equal(sent[0]._model_assignment, true);
    assert.equal(sent[0]._active_provider_key, 'GEMINI_API_KEY');
    assert.equal(app.modelIntent, false);
  }
});

test('first setup with no saved pair assigns the model without extra confirmation', async () => {
  const {app, sent} = savingFixture();
  app.savedConfigComplete = false;
  app.hasSavedAssignment = false;
  app.selectedProvider = 'DeepSeek';
  app.vars.LLM_MODEL = 'deepseek-chat';
  await app.saveConfig();
  assert.equal(sent[0]._model_assignment, true);
});

test('an existing native pair with expired authorization is not first setup', async () => {
  // Real loadConfig(): GET /setup/api/config reports the saved Codex pair with
  // config_complete:false (authorization missing); then the user switches to
  // Google, accepts the stale autofilled hint and enters only an API key.
  const sent = [];
  const app = fixture(async (url, options) => {
    if (options && options.method === 'PUT') { sent.push(JSON.parse(options.body)); return {ok: true, json: async () => ({ok: true})}; }
    if (url === '/setup/api/config') return {ok: true, json: async () => ({
      vars: {}, config_revision: 'rev', config_complete: false,
      model_config: {model: 'gpt-codex-native', provider: 'openai-codex'}})};
    return {ok: true, json: async () => ({status: 'none'})};
  });
  app.codexRequest = async () => ({status: 'not_connected'});
  await app.loadConfig();
  assert.equal(app.savedConfigComplete, false);
  assert.equal(app.hasSavedAssignment, true);
  app.vars._MODEL_GEMINI_API_KEY = 'gemini-stale-hint';
  app.selectedProvider = 'Google AI Studio';
  app.onProviderChange();
  assert.equal(app.vars.LLM_MODEL, 'gemini-stale-hint');
  app.vars.GEMINI_API_KEY = 'AIza-new-key';
  app.loadConfig = async () => {};
  await app.saveConfig();
  assert.equal(sent.length, 1);
  assert.equal(sent[0]._model_assignment, false);
  assert.equal(sent[0]._active_provider_key, 'GEMINI_API_KEY');
  // Deliberate input remains available in the same state.
  app.selectedProvider = 'Google AI Studio';
  app.onProviderChange();
  app.modelIntent = true;
  await app.saveConfig();
  assert.equal(sent[1]._model_assignment, true);
});

test('loadConfig records saved-assignment absence from the native model_config, not from readiness or env', async () => {
  for (const [model_config, vars, expected] of [
    [{model: '', provider: ''}, {LLM_MODEL: 'env-only-model'}, false], // env fallback is not a saved pair
    [{model: 'm', provider: ''}, {}, false],
    [{model: 'm', provider: 'google'}, {}, true],
  ]) {
    const app = fixture(async url => ({ok: true, json: async () => url === '/setup/api/config'
      ? {vars, config_revision: 'r', config_complete: false, model_config} : {status: 'none'}}));
    app.codexRequest = async () => ({status: 'not_connected'});
    app.hasSavedAssignment = !expected;
    await app.loadConfig();
    assert.equal(app.hasSavedAssignment, expected, JSON.stringify(model_config));
  }
});

test('the real text input and the Use this model button are wired to the intent flag', () => {
  assert.match(html, /@input="vars\.LLM_MODEL=\$event\.target\.value; modelIntent=true"/);
  assert.match(html, /@click="modelIntent=true"/);
  assert.match(html, /id="codex-model" x-model="vars\.LLM_MODEL" @change="modelIntent = true"/);
});

test('ChatGPT save needs a confirmed or newly picked model unless it is already native', async () => {
  const {app, sent} = savingFixture();
  app.selectedProvider = 'ChatGPT (OAuth)';
  app.codex.status = 'connected';
  app.codex.models = ['gpt-x'];
  app.vars.LLM_MODEL = 'gpt-x';
  await app.saveConfig();
  assert.equal(sent.length, 0);
  assert.equal(app.notifications[0][1], false);
  app.modelIntent = true;
  await app.saveConfig();
  assert.equal(sent[0]._active_oauth_provider, 'openai-codex');
  assert.equal(sent[0]._model_assignment, true);
  const second = savingFixture();
  second.app.modelProvider = 'openai-codex';
  second.app.selectedProvider = 'ChatGPT (OAuth)';
  second.app.codex.status = 'connected';
  second.app.codex.models = ['gpt-x'];
  second.app.vars.LLM_MODEL = 'gpt-x';
  await second.app.saveConfig();
  assert.equal(second.sent[0]._active_oauth_provider, '');
  assert.equal(second.sent[0]._model_assignment, false);
});

test('confirming a ChatGPT model records deliberate intent', async () => {
  const app = fixture(async () => ({ok: true, json: async () => ({ok: true, config_revision: 'n', config_complete: true})}));
  app.vars.LLM_MODEL = 'chosen';
  app.codex.connected = true;
  app.codex.models = ['chosen'];
  await app.confirmCodexModel();
  assert.equal(app.modelIntent, true);
});
