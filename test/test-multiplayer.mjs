// API-only multiplayer contracts. Run: node --test test/test-multiplayer.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import vm from 'node:vm';
import { isDeepStrictEqual } from 'node:util';
import { TestServer, memberIds, persistentView, expectedBalance, hasRuntimeFields } from './test-flow.mjs';

const PASS = 'multiplayer-test-pass';
// Keep this oracle independent of the implementation: legacy normalization is contractual.
const recoveryDigest = (code) => createHash('sha256').update(String(code).replace(/[-\s]/g, '').toUpperCase()).digest('hex');
const numericRecovery = (code) => {
  assert.equal(typeof code, 'string');
  assert.equal(code.length, 8, 'numeric codes must be exactly eight ASCII digits');
  assert.match(code, /^[0-9]{8}$/);
};
function unusedRecoveryCode(f, start = 123456) {
  const hashes = new Set(Object.values(JSON.parse(f.bytes())).flatMap((room) => memberIds(room).map((id) => room.players[id].recoveryHash)));
  while (hashes.has(recoveryDigest(String(start).padStart(8, '0')))) start++;
  return String(start).padStart(8, '0');
}
function recoveryPrivate(f, codes) {
  const text = f.bytes();
  const normalized = codes.map((code) => code.replace(/[-\s]/g, '').toUpperCase());
  for (const code of [...codes, ...normalized]) assert.ok(!text.includes(code), 'plaintext recovery code must not be persisted');
  const visit = (node) => {
    if (!node || typeof node !== 'object') return;
    for (const [key, child] of Object.entries(node)) {
      assert.notEqual(key, 'recoveryCode', 'disk must contain hashes, never recoveryCode fields');
      visit(child);
    }
  };
  visit(JSON.parse(text));
  assert.ok(!hasRuntimeFields(JSON.parse(text)), 'runtime state must not be persisted');
  const secrets = [...f.secrets, ...codes, ...normalized, ...codes.map(recoveryDigest)];
  for (const feed of f.server.streams) for (const frame of feed.frames) secretFree(frame, secrets);
}
async function recoverySaved(f, before, player, recoveryCode) {
  numericRecovery(recoveryCode);
  const saved = f.disk(); // Inspect durability before waiting for the corresponding SSE frame.
  assert.ok(saved.version > before.version);
  const expected = structuredClone(before);
  expected.version = saved.version;
  expected.players[player.role].recoveryHash = recoveryDigest(recoveryCode);
  assert.deepEqual(saved, expected, 'setting a code changes only its owner hash and room version');
  f.secrets.push(recoveryCode, recoveryDigest(recoveryCode));
  f.state = await f.feed.wait((room) => room.version >= saved.version);
  assert.deepEqual(persistentView(f.state, true), persistentView(saved));
  checkBalance(f.state);
  recoveryPrivate(f, [recoveryCode]);
}
const ids = (n) => Array.from({ length: n }, (_, i) => `p${i + 1}`);
const sum = (values) => values.reduce((a, b) => a + b, 0);
// Carry may omit zero entries; unlike balance it is a sparse accumulator.
const carry = (room) => Object.fromEntries(memberIds(room).map((id) => [id, room.repaymentCarry[id] ?? 0]));
const ledger = (room) => structuredClone({ history: room.history, repayments: room.repayments, repaymentCarry: room.temporaryMode ? carry(room) : undefined, balance: room.balance });
const success = (response, label) => {
  assert.equal(response.status, 200, `${label}: ${response.status} ${JSON.stringify(response.data)}`);
  return response.data;
};
function proposal(pending, required, approved, type) {
  assert.ok(typeof pending.id === 'string' && pending.id.length > 0);
  assert.equal(pending.type, type);
  assert.deepEqual([...pending.required].sort(), [...required].sort());
  assert.ok(pending.approvals && !Array.isArray(pending.approvals));
  assert.ok(Object.entries(pending.approvals).every(([id, value]) => required.includes(id) && typeof value === 'boolean'));
  assert.deepEqual(required.filter((id) => pending.approvals[id] === true), required.filter((id) => approved.includes(id)));
}
function privateBill(room) {
  assert.equal(room.phase, 'rolling');
  for (const key of ['roll', 'rolls', 'ratio', 'shares']) assert.equal(room.bill[key], undefined, `rolling must omit ${key}`);
}
// Independent integer oracle: no floating-point rounding or production helper reuse.
function apportioned(total, rolls, participants) {
  const denominator = BigInt(sum(participants.map((id) => rolls[id])));
  const rows = participants.map((id, order) => {
    const numerator = BigInt(total) * BigInt(rolls[id]);
    return { id, order, base: Number(numerator / denominator), remainder: numerator % denominator };
  });
  const remaining = total - sum(rows.map((row) => row.base));
  const ranked = [...rows].sort((a, b) => a.remainder === b.remainder ? a.order - b.order : a.remainder > b.remainder ? -1 : 1);
  for (const row of ranked.slice(0, remaining)) row.base++;
  return Object.fromEntries(rows.map((row) => [row.id, row.base]));
}
function checkBalance(room) {
  assert.deepEqual(Object.keys(room.balance).sort(), memberIds(room).sort());
  assert.ok(Object.values(room.balance).every(Number.isSafeInteger));
  assert.equal(sum(Object.values(room.balance)), 0);
  assert.deepEqual(room.balance, expectedBalance(room));
  if (room.temporaryMode) {
    assert.ok(room.repaymentCarry && typeof room.repaymentCarry === 'object' && !Array.isArray(room.repaymentCarry));
    assert.ok(Object.keys(room.repaymentCarry).every((id) => memberIds(room).includes(id)));
    assert.ok(Object.values(room.repaymentCarry).every(Number.isSafeInteger));
    assert.equal(sum(Object.values(room.repaymentCarry)), 0);
  }
  assert.equal(room.repaymentCarryCents, undefined);
}
function secretFree(value, secrets) {
  const text = JSON.stringify(value);
  for (const secret of secrets) assert.ok(!text.includes(secret), 'snapshot/history must not contain identity secrets');
  const visit = (node) => {
    if (!node || typeof node !== 'object') return;
    for (const [key, child] of Object.entries(node)) {
      assert.ok(!/^(token|recoveryCode|recoveryHash)$/.test(key), `private field leaked: ${key}`);
      visit(child);
    }
  };
  visit(value);
}
async function fixture(t, temporary = false, env = {}) {
  const dir = await mkdtemp(path.join(tmpdir(), 'dice-split-multiplayer-'));
  const file = path.join(dir, temporary ? 'never-created/ledger.json' : 'ledger.json');
  const servers = [];
  const f = { dir, file, temporary, players: [], secrets: [], feed: null, state: null };
  t.after(async () => {
    const failures = [];
    for (const server of servers) {
      try { await server.stop(); } catch (error) { failures.push(error); }
    }
    if (servers.every((server) => server.exit)) await rm(dir, { recursive: true, force: true });
    if (failures.length) throw new AggregateError(failures, 'multiplayer cleanup');
  });
  f.launch = async () => {
    f.server = new TestServer(file, { PASSCODE: temporary ? '' : PASS, ...env });
    servers.push(f.server);
    await f.server.start();
  };
  f.bytes = () => existsSync(file) ? readFileSync(file, 'utf8') : null;
  f.disk = () => JSON.parse(f.bytes())[f.players[0].code];
  f.request = (url, fields) => f.server.request('POST', url, { ...fields, ...(temporary ? {} : { passcode: PASS }) });
  f.get = (url, fields) => f.server.request('GET', `${url}?${new URLSearchParams({ ...fields, ...(temporary ? {} : { passcode: PASS }) })}`);
  f.identity = (response, role) => {
    const player = success(response, 'identity');
    assert.equal(player.role, role);
    assert.match(player.code, /^[A-Z2-9]{5}$/);
    assert.ok(typeof player.token === 'string' && player.token.length > 0);
    numericRecovery(player.recoveryCode);
    assert.notEqual(player.token, player.recoveryCode);
    assert.ok(!f.secrets.includes(player.token), 'new members need distinct tokens');
    assert.ok(f.players.filter((p) => p.code === player.code).every((p) => p.recoveryCode !== player.recoveryCode), 'new recovery codes must be distinct within their room');
    f.secrets.push(player.token, player.recoveryCode, recoveryDigest(player.recoveryCode));
    if (!temporary) {
      const room = JSON.parse(f.bytes())[player.code];
      assert.equal(room.players[role].token, player.token, 'identity must persist before response');
      assert.equal(room.players[role].recoveryHash, recoveryDigest(player.recoveryCode));
      assert.equal(new Set(memberIds(room).map((id) => room.players[id].recoveryHash)).size, memberIds(room).length);
      const { token, recoveryCode, ...publicIdentity } = player;
      secretFree(publicIdentity, f.secrets);
      for (const secret of f.secrets.filter((value) => value !== token && value !== recoveryCode)) {
        assert.ok(!JSON.stringify(player).includes(secret), 'identity response must not expose peer secrets or hashes');
      }
      assert.ok(!f.bytes().includes(player.recoveryCode), 'only recovery hashes may be persisted');
      assert.equal(room.players[role].recoveryCode, undefined);
    }
    return player;
  };
  f.create = async (capacity) => {
    const player = f.identity(await f.request('/api/create', { name: 'Member 1', ...(capacity === undefined ? {} : { capacity }) }), 'p1');
    f.players = [player];
    return player;
  };
  f.connect = async (player = f.players[0]) => {
    f.feed = await f.server.observe(player.code, player.token, temporary ? undefined : PASS);
    f.state = await f.feed.wait((room) => room.players[player.role].online, 'member online');
    checkBalance(f.state);
  };
  f.join = async () => {
    const phase = f.state?.phase;
    const bill = structuredClone(f.state?.bill);
    const index = f.feed?.frames.length;
    const player = f.identity(await f.request('/api/join', { code: f.players[0].code, name: `Member ${f.players.length + 1}` }), `p${f.players.length + 1}`);
    f.players.push(player);
    if (f.feed) {
      f.state = await f.feed.wait((room) => !!room.players[player.role], 'joined member snapshot', index);
      if (bill) {
        assert.equal(f.state.phase, phase, 'joining cannot reset an active bill');
        assert.deepEqual(f.state.bill, bill, 'joining cannot rewrite locked participants or votes');
      }
      checkBalance(f.state);
    }
    return player;
  };
  f.action = (player, type, fields = {}) => f.request('/api/action', { code: player.code, token: player.token, type, ...fields });
  f.change = async (player, type, fields = {}) => {
    const version = f.state.version;
    const index = f.feed.frames.length;
    const result = success(await f.action(player, type, fields), type);
    const saved = temporary ? null : f.disk(); // Deliberately before awaiting SSE.
    if (saved) assert.ok(saved.version > version, `${type} must persist before responding`);
    f.state = await f.feed.wait((room) => saved ? isDeepStrictEqual(persistentView(room, true), persistentView(saved)) : room.version > version, `${type} snapshot`, index);
    checkBalance(f.state);
    secretFree(f.state, f.secrets);
    if (saved) assert.ok(!hasRuntimeFields(saved));
    if (temporary) assert.ok(!existsSync(path.dirname(file)), 'temporary operations must never create a data directory');
    return result;
  };
  f.rejectRequest = async (url, fields) => {
    const bytes = f.bytes();
    const index = f.feed?.frames.length;
    const state = structuredClone(f.state);
    const response = await f.request(url, fields);
    assert.ok(response.status >= 400 && response.status < 500, `${url} must reject: ${response.status} ${JSON.stringify(response.data)}`);
    assert.equal(f.bytes(), bytes, 'rejection must leave the durable file byte-identical');
    if (f.feed) {
      await f.feed.noEventsSince(index);
      assert.deepEqual(f.feed.latest, state, 'rejection must preserve version and all live state');
    }
    return response;
  };
  f.reject = (player, type, fields = {}) => f.rejectRequest('/api/action', { code: player.code, token: player.token, type, ...fields });
  f.fail = async (url, fields) => {
    const bytes = f.bytes();
    const state = persistentView(f.state, true);
    const index = f.feed.frames.length;
    const blocker = `${file}.tmp-${f.server.child.pid}`;
    mkdirSync(blocker);
    try {
      const result = await f.request(url, fields);
      assert.equal(result.status, 500, 'failed persistence must not report success');
      assert.equal(f.bytes(), bytes);
      await f.feed.noEventsSince(index);
      const refresh = await f.server.observe(f.players[0].code, f.players[0].token, PASS);
      assert.deepEqual(persistentView(refresh.latest, true), state, 'failed mutation must not survive in memory');
      await refresh.close();
    } finally { rmSync(blocker, { recursive: true, force: true }); }
  };
  f.rollAll = async () => {
    const ownRolls = {};
    for (const id of f.state.bill.participants) {
      const roll = await f.change(f.players.find((p) => p.role === id), 'roll');
      assert.ok(Number.isInteger(roll.roll) && roll.roll >= 1 && roll.roll <= f.state.bill.faces);
      ownRolls[id] = roll.roll;
      if (f.state.phase === 'rolling') privateBill(f.state);
    }
    assert.equal(f.state.phase, 'result');
    assert.deepEqual(f.state.bill.rolls, ownRolls, 'revealed rolls must match every private response');
    const gcd = (a, b) => b ? gcd(b, a % b) : a;
    const divisor = Object.values(ownRolls).reduce(gcd);
    assert.deepEqual(f.state.bill.ratio, f.state.bill.participants.map((id) => ownRolls[id] / divisor));
    assert.deepEqual(f.state.bill.shares, apportioned(f.state.bill.amountCents, ownRolls, f.state.bill.participants));
    assert.equal(sum(Object.values(f.state.bill.shares)), f.state.bill.amountCents);
  };
  f.confirmAll = async () => {
    const required = [...f.state.bill.required];
    const before = ledger(f.state);
    for (const [i, id] of required.entries()) {
      await f.change(f.players.find((p) => p.role === id), 'confirm');
      if (i < required.length - 1) {
        assert.equal(f.state.phase, 'result');
        assert.deepEqual(ledger(f.state), before, 'no ledger changes until every required member confirms');
      }
    }
    assert.equal(f.state.phase, 'idle');
    assert.equal(f.state.bill, null);
  };
  f.bill = async (participants, payer, amountCents) => {
    await f.change(f.players.find((p) => p.role === payer), 'start', { participants, payer, amountCents });
    await f.rollAll();
    await f.confirmAll();
    return f.state.history[0];
  };
  f.restart = async () => {
    await f.server.stop('SIGKILL');
    await f.launch();
    await f.connect();
  };
  await f.launch();
  return f;
}

for (const field of ['capacity', 'participants', 'payer']) {
  test(`explicit null ${field} is invalid, not the omitted-field default`, async (t) => {
    const f = await fixture(t);
    await f.create(3);
    await f.connect();
    await f.join();
    if (field === 'capacity') await f.rejectRequest('/api/create', { capacity: null });
    else await f.reject(f.players[0], 'start', { amountCents: 100, payer: 'p1', participants: ['p1', 'p2'], [field]: null });
  });
}

test('capacity is strict, immutable seats include offline members, and 8-member bills need all confirmations', async (t) => {
  const f = await fixture(t);
  for (const capacity of [0, 1, 9, -2, 2.5, '3', true, [], {}, Number.MAX_SAFE_INTEGER]) {
    await f.rejectRequest('/api/create', { capacity });
  }
  await f.create();
  assert.equal(f.disk().capacity, 2);
  await f.join();
  assert.equal((await f.rejectRequest('/api/join', { code: f.players[0].code })).status, 409);
  for (let capacity = 3; capacity <= 8; capacity++) {
    await f.create(capacity);
    assert.equal(f.disk().capacity, capacity);
    while (f.players.length < capacity) await f.join();
    assert.deepEqual(memberIds(f.disk()), ids(capacity));
    const full = await f.rejectRequest('/api/join', { code: f.players[0].code });
    assert.equal(full.status, 409);
    assert.equal(full.data.error, 'full');
    for (const claimRole of ['p1', `p${capacity}`, '__proto__', 'constructor']) {
      await f.rejectRequest('/api/join', { code: f.players[0].code, claimRole });
    }
  }
  await f.connect();
  assert.equal(f.state.capacity, 8);
  assert.ok(ids(8).slice(1).every((id) => f.state.players[id].online === false));
  await f.change(f.players[0], 'start', { amountCents: 1000000000, payer: 'p8' });
  assert.deepEqual(f.state.bill.participants, ids(8));
  assert.deepEqual(f.state.bill.required, ids(8));
  await f.rollAll();
  await f.confirmAll();
  assert.deepEqual(f.state.history[0].participants, ids(8));
});

test('invalid participants, payer, prototype IDs and non-integer cents do not mutate a room', async (t) => {
  const f = await fixture(t);
  await f.create(8);
  await f.connect();
  for (const claimRole of ['p1', 'p2', 'p8', '__proto__', null, '', false, []]) {
    await f.rejectRequest('/api/join', { code: f.players[0].code, claimRole });
  }
  await f.join();
  await f.join();
  const [p1, , p3] = f.players;
  for (const participants of [[], ['p1'], ['p1', 'p1'], ['p1', 'p2', 'p2'], ['p1', 'p4'], ['p1', '__proto__'], ['p1', 'constructor'], ['p1', 'toString'], ['p1', 2], 'p1,p2', {}, ['p1', null]]) {
    await f.reject(p1, 'start', { amountCents: 100, payer: 'p1', participants });
  }
  for (const payer of ['p3', 'p4', 'p8', '__proto__', 'constructor', 1, {}, []]) {
    await f.reject(p1, 'start', { amountCents: 100, payer, participants: ['p1', 'p2'] });
  }
  await f.reject(p3, 'start', { amountCents: 100, payer: 'p1', participants: ['p1', 'p2'] });
  for (const amountCents of [undefined, null, 0, -1, 0.5, 1.5, '1', '100', true, [], {}, 1000000001, Number.MAX_SAFE_INTEGER]) {
    await f.reject(p1, 'start', { amountCents, payer: 'p1' });
  }
  await f.change(p1, 'start', { amountCents: 1, payer: 'p2', participants: ['p3', 'p2', 'p1'] });
  assert.deepEqual(f.state.bill.participants, ['p1', 'p2', 'p3']);
  await f.rollAll();
  await f.confirmAll();
});

test('three-member reroll/void proposals require every vote and reject stale IDs without side effects', async (t) => {
  const f = await fixture(t);
  await f.create(3);
  await f.connect();
  await f.join();
  await f.join();
  const [p1, p2, p3] = f.players;
  await f.change(p1, 'start', { amountCents: 999, payer: 'p1' });
  await f.rollAll();
  await f.change(p1, 'confirm');
  await f.change(p2, 'reroll');
  const oldId = f.state.bill.pending.id;
  proposal(f.state.bill.pending, ids(3), ['p2'], 'reroll');
  await f.change(p1, 'respond', { proposalId: oldId, approve: true });
  proposal(f.state.bill.pending, ids(3), ['p1', 'p2'], 'reroll');
  assert.equal(f.state.phase, 'result');
  await f.reject(p1, 'respond', { proposalId: oldId, approve: true });
  await f.reject(p3, 'confirm');
  await f.change(p3, 'respond', { proposalId: oldId, approve: false });
  assert.equal(f.state.bill.pending, null);
  assert.equal(f.state.phase, 'result');
  await f.change(p1, 'reroll');
  const newId = f.state.bill.pending.id;
  assert.notEqual(newId, oldId);
  await f.reject(p3, 'respond', { proposalId: oldId, approve: true });
  await f.reject(p2, 'withdraw', { proposalId: oldId });
  await f.reject(p3, 'reroll', { proposalId: oldId });
  await f.change(p2, 'reroll', { proposalId: newId });
  assert.equal(f.state.phase, 'result');
  await f.change(p3, 'respond', { proposalId: newId, approve: true });
  privateBill(f.state);
  assert.equal(f.state.bill.rerolls, 1);
  assert.deepEqual(f.state.bill.rolled, { p1: false, p2: false, p3: false });
  assert.ok(ids(3).every((id) => !f.state.bill.confirms[id]));
  await f.change(p3, 'void');
  const voidId = f.state.bill.pending.id;
  proposal(f.state.bill.pending, ids(3), ['p3'], 'void');
  await f.change(p1, 'void', { proposalId: voidId });
  assert.equal(f.state.phase, 'rolling');
  await f.change(p2, 'respond', { proposalId: voidId, approve: true });
  assert.equal(f.state.phase, 'idle');
  assert.equal(f.state.history.length, 0);
  await f.reject(p1, 'respond', { proposalId: voidId, approve: true });
  await f.bill(ids(3), 'p1', 999);
});

test('observers and late joiners cannot roll or vote on a locked persistent bill', async (t) => {
  const f = await fixture(t);
  await f.create(5);
  await f.connect();
  await f.join();
  await f.join();
  const [p1, p2, p3] = f.players;
  const observer = await f.server.observe(p3.code, p3.token, PASS);
  f.state = await f.feed.wait((room) => room.players.p3.online);
  await f.change(p1, 'start', { participants: ['p2', 'p1'], payer: 'p1', amountCents: 137 });
  assert.deepEqual(f.state.bill.required, ['p1', 'p2']);
  await f.change(p2, 'roll');
  privateBill(f.state);
  const refresh = await f.server.observe(p3.code, p3.token, PASS);
  privateBill(refresh.latest);
  assert.deepEqual(refresh.latest.bill.rolled, { p1: false, p2: true });
  await refresh.close();
  await f.join(); // rolling
  await f.reject(p3, 'roll');
  await f.reject(f.players[3], 'roll');
  await f.change(p1, 'roll');
  await f.join(); // result
  for (const player of f.players.slice(2)) {
    for (const type of ['confirm', 'reroll', 'void']) await f.reject(player, type);
  }
  await f.change(p1, 'reroll');
  const id = f.state.bill.pending.id;
  for (const player of f.players.slice(2)) {
    await f.reject(player, 'respond', { proposalId: id, approve: true });
    await f.reject(player, 'reroll', { proposalId: id });
  }
  await f.change(p2, 'respond', { proposalId: id, approve: true });
  await f.rollAll();
  await f.confirmAll();
  assert.deepEqual(f.state.history[0].participants, ['p1', 'p2']);
  assert.ok(['p3', 'p4', 'p5'].every((id) => f.state.balance[id] === 0));
  for (const frame of observer.frames.filter((room) => room.phase === 'rolling')) privateBill(frame);
});

test('custom recovery preserves leading zeroes, token ownership, durability and private state across restart', async (t) => {
  const f = await fixture(t);
  await f.create(3);
  await f.connect();
  await f.join();
  await f.join();
  await f.bill(ids(3), 'p3', 1273);
  const [p1, p2] = f.players;
  const peerFeed = await f.server.observe(p2.code, p2.token, PASS);
  f.state = await f.feed.wait((room) => room.players.p2.online);
  const custom = unusedRecoveryCode(f);
  assert.ok(custom.startsWith('0'));
  const fields = { code: p2.code, token: p2.token, recoveryCode: custom, role: p1.role };
  await f.fail('/api/recovery', fields);
  const before = f.disk();
  const response = success(await f.request('/api/recovery', fields), 'custom leading-zero recovery code');
  assert.deepEqual(response, { recoveryCode: custom }, 'only the authenticated caller receives their plaintext code');
  await recoverySaved(f, before, p2, custom);
  await peerFeed.wait((room) => room.version >= f.disk().version);
  assert.ok(!peerFeed.ended, 'changing a recovery code must not revoke its owner token or streams');
  assert.equal((await f.rejectRequest('/api/recover', { code: p2.code, recoveryCode: p2.recoveryCode })).status, 401);
  for (const player of f.players) {
    const ping = success(await f.get('/api/ping', { code: player.code, token: player.token }), 'unchanged token');
    assert.equal(ping.role, player.role);
    secretFree(ping, f.secrets);
  }
  const durable = f.disk();
  recoveryPrivate(f, [custom, ...f.players.map((p) => p.recoveryCode)]);
  await f.restart();
  assert.deepEqual(f.disk(), durable, 'restart must not rewrite hashes, tokens or ledger');
  assert.deepEqual(ledger(f.state), ledger({ ...durable, balance: expectedBalance(durable) }));
  await f.change(p2, 'setMe', { name: 'Same identity' });
  assert.equal(f.disk().players.p2.recoveryHash, recoveryDigest(custom));
  assert.equal(f.disk().players.p2.token, p2.token);
  assert.equal((await f.rejectRequest('/api/recover', { code: p2.code, recoveryCode: p2.recoveryCode })).status, 401);
  const beforeRecover = f.disk();
  const recovered = success(await f.request('/api/recover', { code: p2.code, recoveryCode: custom, role: p1.role }), 'custom code after restart');
  assert.equal(recovered.role, p2.role);
  assert.equal(recovered.code, p2.code);
  assert.equal(recovered.recoveryCode, custom);
  assert.notEqual(recovered.token, p2.token);
  const afterRecover = f.disk();
  assert.deepEqual(afterRecover, {
    ...beforeRecover, version: afterRecover.version,
    players: { ...beforeRecover.players, p2: { ...beforeRecover.players.p2, token: recovered.token } },
  });
  f.secrets.push(recovered.token);
  f.players[1] = recovered;
  f.state = await f.feed.wait((room) => room.version >= afterRecover.version);
  assert.equal((await f.get('/api/ping', { code: p2.code, token: p2.token })).status, 401);
  const beforeReset = f.disk();
  const reset = success(await f.request('/api/recovery', { code: recovered.code, token: recovered.token, role: p1.role }), 'automatic reset');
  assert.notEqual(reset.recoveryCode, custom);
  assert.ok(f.players.every((p) => p.recoveryCode !== reset.recoveryCode));
  await recoverySaved(f, beforeReset, recovered, reset.recoveryCode);
  assert.equal((await f.rejectRequest('/api/recover', { code: p2.code, recoveryCode: custom })).status, 401);
  recoveryPrivate(f, [custom, reset.recoveryCode, p2.recoveryCode]);
});

test('custom recovery rejects non-string and non-ASCII eight-digit formats without changing state', async (t) => {
  const f = await fixture(t);
  const p1 = await f.create();
  await f.connect();
  await f.join();
  const invalid = [null, 12345678, ['00123456'], [], {}, true, '', '1234567', '123456789',
    'abcdefgh', '１２３４５６７８', '١٢٣٤٥٦٧٨', ' 12345678', '12345678 ', '1234 5678', '1234-5678', '12345678\n', '\t12345678'];
  for (const recoveryCode of invalid) {
    const response = await f.rejectRequest('/api/recovery', { code: p1.code, token: p1.token, recoveryCode });
    assert.equal(response.status, 400, `invalid custom format: ${JSON.stringify(recoveryCode)}`);
    secretFree(response.data, f.secrets);
  }
  const before = f.disk();
  const custom = unusedRecoveryCode(f);
  const result = success(await f.request('/api/recovery', { code: p1.code, token: p1.token, recoveryCode: custom }), 'valid code after format errors');
  assert.deepEqual(result, { recoveryCode: custom });
  await recoverySaved(f, before, p1, custom);
});

test('custom recovery rejects every occupied room code and atomically resolves concurrent claims', async (t) => {
  const f = await fixture(t);
  await f.create(8);
  await f.connect();
  while (f.players.length < 8) await f.join();
  const p1 = f.players[0];
  for (const owner of f.players) {
    const response = await f.rejectRequest('/api/recovery', {
      code: p1.code, token: p1.token, role: owner.role, recoveryCode: owner.recoveryCode,
    });
    assert.equal(response.status, 409, 'even own or offline members\' codes are conflicts');
    secretFree(response.data, f.secrets);
  }
  const custom = unusedRecoveryCode(f);
  const before = f.disk();
  const contenders = [f.players[1], f.players[7]];
  const results = await Promise.all(contenders.map((player) => f.request('/api/recovery', {
    code: player.code, token: player.token, recoveryCode: custom, role: p1.role,
  })));
  assert.deepEqual(results.map((r) => r.status).sort(), [200, 409]);
  const winner = contenders[results.findIndex((r) => r.status === 200)];
  const loser = contenders[results.findIndex((r) => r.status === 409)];
  assert.deepEqual(results.find((r) => r.status === 200).data, { recoveryCode: custom });
  secretFree(results.find((r) => r.status === 409).data, [...f.secrets, custom]);
  await recoverySaved(f, before, winner, custom);
  assert.equal(f.disk().version, f.state.version);
  assert.equal(memberIds(f.disk()).filter((id) => f.disk().players[id].recoveryHash === recoveryDigest(custom)).length, 1);
  assert.equal((await f.rejectRequest('/api/recovery', { code: loser.code, token: loser.token, recoveryCode: custom })).status, 409);
  assert.equal((await f.rejectRequest('/api/recover', { code: winner.code, recoveryCode: winner.recoveryCode })).status, 401);
  const recovered = success(await f.request('/api/recover', { code: winner.code, recoveryCode: custom, role: loser.role }), 'concurrent winner owns the code');
  assert.equal(recovered.role, winner.role);
  assert.equal(f.disk().players[loser.role].token, loser.token);
  assert.equal(f.disk().players[loser.role].recoveryHash, before.players[loser.role].recoveryHash);
  assert.equal(success(await f.get('/api/ping', { code: loser.code, token: loser.token }), 'loser token remains valid').role, loser.role);
  f.state = await f.feed.wait((room) => room.version >= f.disk().version);
  recoveryPrivate(f, [custom, ...f.players.map((p) => p.recoveryCode)]);
});

test('identical custom recovery codes are allowed across rooms but tokens are room-bound', async (t) => {
  const f = await fixture(t);
  const first = await f.create();
  await f.connect();
  const other = f.identity(await f.request('/api/create', { name: 'Other room' }), 'p1');
  const second = f.identity(await f.request('/api/join', { code: other.code, name: 'Other member' }), 'p2');
  const custom = unusedRecoveryCode(f);
  for (const token of [undefined, '', 'invalid-token', other.token, second.token]) {
    for (const extra of [{}, { recoveryCode: custom }]) {
      assert.equal((await f.rejectRequest('/api/recovery', { code: first.code, token, role: first.role, ...extra })).status, 401);
    }
  }
  const before = f.disk();
  assert.deepEqual(success(await f.request('/api/recovery', { code: first.code, token: first.token, recoveryCode: custom }), 'first room custom code'), { recoveryCode: custom });
  await recoverySaved(f, before, first, custom);
  const firstSaved = f.disk();
  const otherSaved = JSON.parse(f.bytes())[second.code];
  assert.deepEqual(success(await f.request('/api/recovery', { code: second.code, token: second.token, recoveryCode: custom }), 'second room identical code'), { recoveryCode: custom });
  const secondSaved = JSON.parse(f.bytes())[second.code];
  assert.deepEqual(f.disk(), firstSaved);
  assert.deepEqual(secondSaved, {
    ...otherSaved, version: secondSaved.version,
    players: { ...otherSaved.players, p2: { ...otherSaved.players.p2, recoveryHash: recoveryDigest(custom) } },
  });
  f.feed.expectServerClose();
  for (const player of [first, second]) {
    const recovered = success(await f.request('/api/recover', { code: player.code, recoveryCode: custom, role: 'p8' }), 'room-scoped recovery');
    assert.equal(recovered.role, player.role);
    assert.equal(recovered.code, player.code);
    assert.equal(recovered.recoveryCode, custom);
    assert.notEqual(recovered.token, player.token);
  }
  await f.feed.waitForServerClose();
  recoveryPrivate(f, [custom, first.recoveryCode, second.recoveryCode]);
});

for (const legacy of ['0123456789ABCDEF', '0123456789ABCDEFFEDCBA9876543210']) {
  test(`legacy ${legacy.length}-hex recovery keeps old hashes/tokens until explicit recovery or reset`, async (t) => {
    const f = await fixture(t);
    await f.create();
    await f.connect();
    await f.join();
    await f.bill(ids(2), 'p1', 1037);
    const [p1, p2] = f.players;
    await f.server.stop('SIGKILL');
    const data = JSON.parse(f.bytes());
    data[p1.code].players.p2.recoveryHash = recoveryDigest(legacy);
    writeFileSync(f.file, JSON.stringify(data));
    const original = f.bytes();
    const grouped = legacy.match(/.{8}/g).join('-');
    f.secrets.push(legacy, grouped, recoveryDigest(legacy));
    await f.launch();
    await f.connect();
    assert.equal(f.bytes(), original, 'startup must not rewrite legacy credentials');
    for (const player of f.players) assert.equal((await f.get('/api/ping', { code: player.code, token: player.token })).status, 200);
    await f.change(p2, 'setMe', { name: 'Legacy token' });
    assert.equal(f.disk().players.p2.token, p2.token);
    assert.equal(f.disk().players.p2.recoveryHash, recoveryDigest(legacy));
    const saved = f.disk();
    await f.restart();
    assert.deepEqual(f.disk(), saved);
    const recovered = success(await f.request('/api/recover', {
      code: p2.code, recoveryCode: ` \t${legacy.toLowerCase().match(/.{4}/g).join(' - ')}\n`, role: p1.role,
    }), 'legacy normalization and grouping');
    assert.equal(recovered.role, p2.role);
    assert.equal(recovered.recoveryCode, grouped);
    assert.notEqual(recovered.token, p2.token);
    assert.equal(f.disk().players.p2.recoveryHash, recoveryDigest(legacy), 'recovering an old code must not migrate its hash');
    assert.equal(f.disk().players.p1.token, p1.token);
    assert.deepEqual(persistentView(f.disk()), persistentView(saved));
    assert.equal((await f.get('/api/ping', { code: p2.code, token: p2.token })).status, 401);
    f.players[1] = recovered;
    f.secrets.push(recovered.token);
    f.state = await f.feed.wait((room) => room.version >= f.disk().version);
    await f.change(recovered, 'setMe', { name: 'Recovered legacy' });
    const beforeRestart = f.disk();
    await f.restart();
    assert.deepEqual(f.disk(), beforeRestart);
    assert.equal((await f.get('/api/ping', { code: recovered.code, token: recovered.token })).status, 200);
    const beforeReset = f.disk();
    const reset = success(await f.request('/api/recovery', { code: recovered.code, token: recovered.token }), 'explicit legacy reset');
    await recoverySaved(f, beforeReset, recovered, reset.recoveryCode);
    for (const recoveryCode of [legacy, grouped.toLowerCase()]) {
      assert.equal((await f.rejectRequest('/api/recover', { code: p2.code, recoveryCode })).status, 401);
    }
    const again = success(await f.request('/api/recover', { code: p2.code, recoveryCode: reset.recoveryCode }), 'numeric replacement authenticates');
    assert.equal(again.role, 'p2');
    assert.equal(again.recoveryCode, reset.recoveryCode);
    f.state = await f.feed.wait((room) => room.version >= f.disk().version);
    recoveryPrivate(f, [legacy, grouped, reset.recoveryCode]);
  });
}

test('recover and recovery share a 20-request room budget after authentication/format checks, not spoofed IPs', async (t) => {
  const f = await fixture(t);
  await f.create();
  await f.connect();
  await f.join();
  await f.bill(ids(2), 'p1', 1301);
  const [p1, p2] = f.players;
  const bytes = f.bytes();
  const index = f.feed.frames.length;
  const custom = unusedRecoveryCode(f);
  const post = (url, fields, attempt = 0) => f.server.request('POST', url, { passcode: PASS, ...fields }, {
    'X-Forwarded-For': `198.51.100.${attempt + 1}`,
    'X-Real-IP': `203.0.113.${attempt + 1}`,
    Forwarded: `for=192.0.2.${attempt + 1}`,
  });
  // More than a complete room budget of each pre-validation failure: none may consume it.
  for (let i = 0; i < 21; i++) {
    for (const [url, fields, status] of [
      ['/api/recover', { code: p1.code, recoveryCode: p1.recoveryCode, passcode: 'wrong-passcode' }, 401],
      ['/api/recover', { code: 'INVALID', recoveryCode: p1.recoveryCode }, 401],
      ['/api/recovery', { code: p1.code, token: 'wrong-token', recoveryCode: custom }, 401],
      ['/api/recovery', { code: p1.code, token: p1.token, recoveryCode: i % 2 ? null : '1234-5678' }, 400],
    ]) {
      const response = await post(url, fields, i);
      assert.equal(response.status, status, `${url} must validate before counting`);
      secretFree(response.data, f.secrets);
      assert.equal(f.bytes(), bytes);
    }
  }
  await f.feed.noEventsSince(index);
  assert.deepEqual(f.feed.latest, f.state);
  // Successes count too, and recover must not refresh the shared room budget when rotating a token.
  const beforeSet = f.disk();
  assert.deepEqual(success(await post('/api/recovery', { code: p1.code, token: p1.token, recoveryCode: custom }), 'budget request 1'), { recoveryCode: custom });
  await recoverySaved(f, beforeSet, p1, custom);
  const recovered = success(await post('/api/recover', { code: p2.code, recoveryCode: p2.recoveryCode }, 1), 'budget request 2');
  assert.equal(recovered.role, p2.role);
  assert.notEqual(recovered.token, p2.token);
  f.players[1] = recovered;
  f.secrets.push(recovered.token);
  f.state = await f.feed.wait((room) => room.version >= f.disk().version);
  const limitedBytes = f.bytes();
  const limitedState = structuredClone(f.state);
  const limitedIndex = f.feed.frames.length;
  for (let i = 2; i < 20; i++) {
    const code = i % 2 ? ` ${p1.code.toLowerCase()} ` : p1.code;
    const response = i % 2
      ? await post('/api/recovery', { code, token: p1.token, recoveryCode: p2.recoveryCode }, i)
      : await post('/api/recover', { code, recoveryCode: i === 2 ? null : 'not-a-recovery-code' }, i);
    assert.equal(response.status, i % 2 ? 409 : 401, `request ${i + 1} still has room budget`);
    assert.equal(f.bytes(), limitedBytes);
    secretFree(response.data, f.secrets);
  }
  for (const [url, fields] of [
    ['/api/recover', { code: p1.code, recoveryCode: custom }],
    ['/api/recovery', { code: p1.code, token: p1.token }],
    ['/api/recovery', { code: p2.code.toLowerCase(), token: recovered.token, recoveryCode: unusedRecoveryCode(f) }],
    ['/api/recover', { code: p2.code, recoveryCode: p2.recoveryCode }],
  ]) {
    const response = await post(url, fields, 99);
    assert.equal(response.status, 429, 'both paths and all members share the exhausted room budget');
    assert.equal(f.bytes(), limitedBytes, '429 cannot change hashes, tokens, ledger or version');
    secretFree(response.data, f.secrets);
  }
  assert.equal((await post('/api/recovery', { code: p1.code, token: 'wrong-token', recoveryCode: custom })).status, 401);
  assert.equal((await post('/api/recovery', { code: p1.code, token: p1.token, recoveryCode: '' })).status, 400);
  await f.feed.noEventsSince(limitedIndex);
  assert.deepEqual(f.feed.latest, limitedState);
  for (const player of [p1, recovered]) {
    assert.equal(success(await f.get('/api/ping', { code: player.code, token: player.token }), 'ping is not recovery-limited').role, player.role);
  }
  await f.change(recovered, 'setMe', { name: 'Actions still work' });
  assert.deepEqual(ledger(f.state), ledger(limitedState));
  const limitedRoom = f.disk();
  const other = f.identity(await f.request('/api/create', { name: 'Independent budget' }), 'p1');
  const otherCode = unusedRecoveryCode(f);
  assert.deepEqual(success(await post('/api/recovery', { code: other.code, token: other.token, recoveryCode: otherCode }), 'other room reset'), { recoveryCode: otherCode });
  const otherRecovered = success(await post('/api/recover', { code: other.code, recoveryCode: otherCode }), 'other room recovery');
  assert.equal(otherRecovered.role, 'p1');
  assert.equal(otherRecovered.code, other.code);
  assert.deepEqual(f.disk(), limitedRoom, 'other room operations cannot change the limited room');
  recoveryPrivate(f, [custom, otherCode, p2.recoveryCode]);
});

test('concurrent recovery attempts cannot exceed the shared room budget or mutate rejected state', async (t) => {
  const f = await fixture(t);
  const p1 = await f.create();
  await f.connect();
  await f.join();
  const bytes = f.bytes();
  const state = structuredClone(f.state);
  const index = f.feed.frames.length;
  const responses = await Promise.all(Array.from({ length: 24 }, (_, i) => f.request(
    i % 2 ? '/api/recover' : '/api/recovery',
    i % 2 ? { code: p1.code, recoveryCode: 'invalid' }
      : { code: p1.code, token: p1.token, recoveryCode: p1.recoveryCode },
  )));
  assert.equal(responses.filter((r) => r.status === 429).length, 4);
  assert.equal(responses.filter((r) => r.status === 401 || r.status === 409).length, 20);
  for (const response of responses) secretFree(response.data, f.secrets);
  assert.equal(f.bytes(), bytes);
  await f.feed.noEventsSince(index);
  assert.deepEqual(f.feed.latest, state);
  assert.equal((await f.get('/api/ping', { code: p1.code, token: p1.token })).status, 200);
});

// Extract only pure helpers into a VM: deterministic randomness/time without production hooks or files.
function serverHelpers(start, end, globals) {
  const source = readFileSync(new URL('../server.js', import.meta.url), 'utf8');
  const first = source.indexOf(start);
  const last = source.indexOf(end, first);
  assert.ok(first >= 0 && last > first, `server helper boundaries: ${start}`);
  const context = vm.createContext(globals);
  vm.runInContext(source.slice(first, last), context, { timeout: 1000 });
  return context;
}

test('numeric recovery generation keeps zeroes and retries collisions against every existing hash', () => {
  const draws = [0, 1, 42, 1234, 42, 99999999, 7, 0];
  const context = serverHelpers('function recoveryHash(', 'function makeRoom(', {
    crypto: {
      createHash,
      randomInt: (...args) => {
        assert.deepEqual(args, [100000000], 'sample the complete eight-digit space using cryptographic randomness');
        assert.ok(draws.length, 'unexpected extra random draw');
        return draws.shift();
      },
    },
    newToken: () => 'deterministic-token',
    players: {
      p1: { recoveryHash: recoveryDigest('00000000') },
      p2: null,
      p3: { recoveryHash: recoveryDigest('00000001') },
      p8: { recoveryHash: recoveryDigest('00000042') },
    },
  });
  const before = structuredClone(context.players);
  const code = vm.runInContext('newRecoveryCode(players)', context, { timeout: 1000 });
  assert.equal(code, '00001234');
  assert.deepEqual(context.players, before, 'collision retry must not mutate existing members');
  const joined = vm.runInContext('makePlayer("New member", "", players)', context, { timeout: 1000 });
  assert.equal(joined.recoveryCode, '99999999');
  assert.equal(joined.player.recoveryHash, recoveryDigest('99999999'));
  assert.equal(vm.runInContext('newRecoveryCode()', context, { timeout: 1000 }), '00000007');
  assert.equal(vm.runInContext('newRecoveryCode()', context, { timeout: 1000 }), '00000000');
  assert.deepEqual(draws, []);
  for (const legacy of ['abcd ef01-2345\t6789', '01234567-89abcdef-01234567-89abcdef']) {
    context.input = legacy;
    assert.equal(vm.runInContext('recoveryHash(input)', context), recoveryDigest(legacy));
  }
});

test('room recovery limiter restores its 20-attempt allowance after a 60-second window', () => {
  let now = 1700000000000;
  const context = serverHelpers('const rateMap =', 'function clientIp(', {
    RATE_LIMIT: 100000,
    Date: { now: () => now },
  });
  const attempt = (room = 'ROOM1') => {
    context.roomKey = room;
    return vm.runInContext('rateLimit(roomKey, recoveryRateMap, 20)', context);
  };
  for (let i = 0; i < 20; i++) assert.equal(attempt(), true);
  assert.equal(attempt(), false);
  now += 59999;
  assert.equal(attempt(), false, 'budget must last the full minute');
  assert.equal(attempt('ROOM2'), true, 'other rooms have independent windows');
  assert.equal(vm.runInContext('rateLimit("ROOM1")', context), true, 'general API rate map is separate');
  now += 2;
  for (let i = 0; i < 20; i++) assert.equal(attempt(), true);
  assert.equal(attempt(), false);
});

test('recovery rotates only the matching identity, revokes live streams, and supports resetting the secret', async (t) => {
  const f = await fixture(t);
  await f.create(3);
  await f.connect();
  const old = f.players[0];
  const duplicate = await f.server.observe(old.code, old.token, PASS);
  const before = persistentView(f.state, true);
  await f.fail('/api/recover', { code: old.code, recoveryCode: old.recoveryCode });
  await f.fail('/api/recovery', { code: old.code, token: old.token });
  f.feed.expectServerClose();
  duplicate.expectServerClose();
  const recovered = success(await f.request('/api/recover', { code: old.code, recoveryCode: old.recoveryCode, role: 'p8', name: 'must not rename' }), 'lobby recovery');
  assert.equal(f.disk().players.p1.token, recovered.token);
  assert.equal(recovered.code, old.code);
  assert.equal(recovered.role, old.role);
  assert.equal(recovered.recoveryCode, old.recoveryCode);
  assert.notEqual(recovered.token, old.token);
  await Promise.all([f.feed.waitForServerClose(), duplicate.waitForServerClose()]);
  f.players[0] = recovered;
  f.secrets.push(recovered.token);
  await f.connect();
  assert.deepEqual(persistentView(f.state, true), before);
  const revoked = async (player) => {
    assert.equal((await f.get('/api/ping', { code: player.code, token: player.token })).status, 401);
    await f.reject(player, 'setMe', { name: 'stolen' });
    assert.equal((await f.get('/events', { code: player.code, token: player.token })).status, 404);
    await f.rejectRequest('/api/recovery', { code: player.code, token: player.token });
  };
  await revoked(old);
  await f.join();
  await f.join();
  await f.bill(ids(3), 'p2', 1200);
  const peer = f.players[1];
  const history = ledger(f.state);
  const names = Object.fromEntries(ids(3).map((id) => [id, f.state.players[id].name]));
  const peerFeed = await f.server.observe(peer.code, peer.token, PASS);
  f.state = await f.feed.wait((room) => room.players.p2.online);
  peerFeed.expectServerClose();
  const result = success(await f.request('/api/recover', { code: peer.code, recoveryCode: peer.recoveryCode, role: 'p1', name: 'cannot rename' }), 'full-room recovery');
  assert.equal(result.role, 'p2');
  assert.equal(result.code, peer.code);
  assert.equal(result.recoveryCode, peer.recoveryCode);
  assert.notEqual(result.token, peer.token);
  assert.equal(f.disk().players.p2.token, result.token);
  await peerFeed.waitForServerClose();
  f.state = await f.feed.wait((room) => !room.players.p2.online);
  f.players[1] = result;
  f.secrets.push(result.token);
  await revoked(peer);
  assert.deepEqual(ledger(f.state), history);
  assert.deepEqual(Object.fromEntries(ids(3).map((id) => [id, f.state.players[id].name])), names);
  for (const recoveryCode of ['wrong-secret', result.token, '__proto__', null, {}, []]) {
    await f.rejectRequest('/api/recover', { code: result.code, recoveryCode });
  }
  await f.rejectRequest('/api/recover', { code: 'INVALID', recoveryCode: result.recoveryCode });
  const otherRoom = f.identity(await f.request('/api/create', { capacity: 3, name: 'Different room' }), 'p1');
  await f.rejectRequest('/api/recover', { code: otherRoom.code, recoveryCode: result.recoveryCode });
  assert.equal(success(await f.get('/api/ping', { code: otherRoom.code, token: otherRoom.token }), 'other room identity').role, 'p1');
  const originalHash = f.disk().players.p2.recoveryHash;
  const reset = success(await f.request('/api/recovery', { code: result.code, token: result.token }), 'replace recovery secret');
  numericRecovery(reset.recoveryCode);
  assert.notEqual(reset.recoveryCode, result.recoveryCode);
  assert.notEqual(f.disk().players.p2.recoveryHash, originalHash);
  assert.equal(f.disk().players.p2.token, result.token, 'resetting recovery code does not rotate the authenticated token');
  f.secrets.push(reset.recoveryCode);
  assert.ok(!f.bytes().includes(reset.recoveryCode));
  // Wait for the successful identity mutation before asserting failed requests emit no frames.
  f.state = await f.feed.wait((room) => room.version >= f.disk().version);
  await f.rejectRequest('/api/recover', { code: result.code, recoveryCode: result.recoveryCode });
  const again = success(await f.request('/api/recover', { code: result.code, recoveryCode: reset.recoveryCode }), 'new recovery code works');
  assert.equal(again.role, 'p2');
  assert.equal(again.recoveryCode, reset.recoveryCode);
  f.players[1] = again;
  f.secrets.push(again.token);
  f.state = await f.feed.wait((room) => room.version >= f.disk().version);
  await revoked(result);
  secretFree(f.state, f.secrets);
  secretFree(f.state.history, f.secrets);
  await f.restart();
  assert.deepEqual(ledger(f.state), history);
  assert.deepEqual(success(await f.get('/api/ping', { code: again.code, token: again.token }), 'recovered identity after restart'), { ok: true, code: again.code, role: 'p2' });
  await f.rejectRequest('/api/recover', { code: result.code, recoveryCode: result.recoveryCode });
});

test('directed repayments enforce both balances and pair-only consent; history deletion needs all members', async (t) => {
  const f = await fixture(t);
  await f.create(4);
  await f.connect();
  while (f.players.length < 4) await f.join();
  const [p1, p2, p3, p4] = f.players;
  const bill1 = await f.bill(['p1', 'p3'], 'p1', 12000);
  await f.bill(['p2', 'p4'], 'p2', 18000);
  assert.ok(f.state.balance.p1 > 0 && f.state.balance.p2 > 0 && f.state.balance.p3 < 0 && f.state.balance.p4 < 0);
  for (const fields of [
    { from: 'p1', to: 'p3' }, { from: 'p3', to: 'p4' }, { from: 'p3', to: 'p3' },
    { from: '__proto__', to: 'p1' }, { from: 'p3', to: 'constructor' },
    { from: 'p5', to: 'p1' }, { from: ['p3'], to: 'p1' }, {},
  ]) await f.reject(p3, 'repay', { ...fields, amountCents: 1 });
  await f.reject(p4, 'repay', { from: 'p3', to: 'p1', amountCents: 1 });
  for (const amountCents of [0, -1, 1.5, '1', true, null, 1000000001]) {
    await f.reject(p3, 'repay', { from: 'p3', to: 'p1', amountCents });
  }
  const original = ledger(f.state);
  await f.change(p1, 'repay', { from: 'p3', to: 'p1', amountCents: 1 });
  let pending = f.state.ledgerPending;
  proposal(pending, ['p1', 'p3'], ['p1'], 'repay');
  assert.deepEqual(ledger(f.state), original);
  await f.reject(p2, 'ledgerRespond', { proposalId: pending.id, approve: true });
  await f.reject(p4, 'ledgerRespond', { proposalId: pending.id, approve: false });
  await f.change(p3, 'ledgerRespond', { proposalId: pending.id, approve: true });
  assert.equal(f.state.repayments.length, 1);
  const repayment = f.state.repayments[0];
  assert.equal(repayment.from, 'p3');
  assert.equal(repayment.to, 'p1');
  await f.reject(p2, 'deleteRepayment', { id: repayment.id });
  await f.change(p3, 'deleteRepayment', { id: repayment.id });
  pending = f.state.ledgerPending;
  proposal(pending, ['p1', 'p3'], ['p3'], 'deleteRepayment');
  await f.reject(p4, 'ledgerRespond', { proposalId: pending.id, approve: true });
  await f.change(p1, 'ledgerRespond', { proposalId: pending.id, approve: true });
  assert.deepEqual(ledger(f.state), original);
  // Cross-creditor payment tests min(-debtor, creditor), not only an original bill's direction.
  const maxCross = Math.min(-f.state.balance.p3, f.state.balance.p2);
  await f.reject(p3, 'repay', { from: 'p3', to: 'p2', amountCents: maxCross + 1 });
  await f.change(p3, 'repay', { from: 'p3', to: 'p2', amountCents: maxCross });
  await f.change(p2, 'ledgerRespond', { proposalId: f.state.ledgerPending.id, approve: true });
  let transfers = 1;
  while (Object.values(f.state.balance).some((net) => net !== 0)) {
    assert.ok(transfers++ < 5, 'bounded settlement must converge');
    const from = ids(4).find((id) => f.state.balance[id] < 0);
    const to = ids(4).find((id) => f.state.balance[id] > 0);
    const maximum = Math.min(-f.state.balance[from], f.state.balance[to]);
    const sender = f.players.find((p) => p.role === from);
    const recipient = f.players.find((p) => p.role === to);
    await f.reject(sender, 'repay', { from, to, amountCents: maximum + 1 });
    const before = ledger(f.state);
    await f.change(sender, 'repay', { from, to, amountCents: maximum });
    assert.deepEqual(ledger(f.state), before);
    await f.change(recipient, 'ledgerRespond', { proposalId: f.state.ledgerPending.id, approve: true });
    assert.equal(f.state.balance[from], before.balance[from] + maximum);
    assert.equal(f.state.balance[to], before.balance[to] - maximum);
  }
  assert.deepEqual(f.state.balance, { p1: 0, p2: 0, p3: 0, p4: 0 });
  const settled = ledger(f.state);
  await f.change(p4, 'deleteHistory', { id: bill1.id });
  const declined = f.state.ledgerPending.id;
  proposal(f.state.ledgerPending, ids(4), ['p4'], 'deleteHistory');
  await f.change(p1, 'ledgerRespond', { proposalId: declined, approve: true });
  await f.change(p2, 'ledgerRespond', { proposalId: declined, approve: false });
  assert.equal(f.state.ledgerPending, null);
  assert.deepEqual(ledger(f.state), settled);
  await f.change(p4, 'deleteHistory', { id: bill1.id });
  const deletion = f.state.ledgerPending.id;
  await f.reject(p3, 'ledgerRespond', { proposalId: declined, approve: true });
  for (const player of [p1, p2]) {
    await f.change(player, 'ledgerRespond', { proposalId: deletion, approve: true });
    assert.deepEqual(ledger(f.state), settled);
  }
  await f.change(p3, 'ledgerRespond', { proposalId: deletion, approve: true });
  assert.ok(!f.state.history.some((bill) => bill.id === bill1.id));
  assert.deepEqual(f.state.repayments, settled.repayments);
  const beforeClear = ledger(f.state);
  await f.change(p2, 'clearHistory');
  const clear = f.state.ledgerPending.id;
  proposal(f.state.ledgerPending, ids(4), ['p2'], 'clearHistory');
  for (const player of [p1, p3]) {
    await f.change(player, 'ledgerRespond', { proposalId: clear, approve: true });
    assert.deepEqual(ledger(f.state), beforeClear);
  }
  await f.change(p4, 'ledgerRespond', { proposalId: clear, approve: true });
  assert.equal(f.state.history.length, 0);
  assert.equal(f.state.repayments.length, 0);
  assert.deepEqual(f.state.balance, { p1: 0, p2: 0, p3: 0, p4: 0 });
});

test('third-seat and quorum write failures roll back; restart preserves partial approvals and private rolls', async (t) => {
  const f = await fixture(t);
  await f.create(3);
  await f.connect();
  await f.join();
  await f.fail('/api/join', { code: f.players[0].code, name: 'failed third member' });
  assert.deepEqual(memberIds(f.disk()), ['p1', 'p2']);
  await f.join();
  const [p1, p2, p3] = f.players;
  await f.change(p1, 'start', { amountCents: 811, payer: 'p3' });
  await f.change(p2, 'roll');
  const rolling = persistentView(f.state, true);
  await f.restart();
  assert.deepEqual(persistentView(f.state, true), rolling);
  privateBill(f.state);
  await f.reject(p2, 'roll');
  await f.change(p1, 'roll');
  await f.change(p3, 'roll');
  await f.change(p1, 'void');
  const id = f.state.bill.pending.id;
  const vote = { code: p2.code, token: p2.token, type: 'respond', proposalId: id, approve: true };
  await f.fail('/api/action', vote);
  proposal(f.state.bill.pending, ids(3), ['p1'], 'void');
  await f.change(p2, 'respond', { proposalId: id, approve: true });
  const durable = persistentView(f.state, true);
  await f.restart();
  assert.deepEqual(persistentView(f.state, true), durable);
  proposal(f.state.bill.pending, ids(3), ['p1', 'p2'], 'void');
  await f.fail('/api/action', { ...vote, token: p3.token });
  await f.change(p3, 'respond', { proposalId: id, approve: true });
  assert.equal(f.state.history.length, 0);
  await f.change(p1, 'start', { amountCents: 900, payer: 'p1' });
  await f.rollAll();
  await f.change(p1, 'confirm');
  await f.change(p2, 'confirm');
  await f.fail('/api/action', { code: p3.code, token: p3.token, type: 'confirm' });
  assert.equal(f.disk().history.length, 0);
  assert.equal(f.disk().bill.confirms.p3, false);
  await f.change(p3, 'confirm');
  assert.equal(f.state.history.length, 1);
  await f.change(p1, 'clearHistory');
  const clear = f.state.ledgerPending.id;
  await f.change(p2, 'ledgerRespond', { proposalId: clear, approve: true });
  const partial = persistentView(f.state, true);
  await f.restart();
  assert.deepEqual(persistentView(f.state, true), partial);
  await f.fail('/api/action', { code: p3.code, token: p3.token, type: 'ledgerRespond', proposalId: clear, approve: true });
  await f.change(p3, 'ledgerRespond', { proposalId: clear, approve: true });
  assert.equal(f.state.history.length, 0);
});

for (const type of ['repay', 'deleteRepayment', 'deleteHistory', 'clearHistory']) {
  test(`legacy ${type} proposal migration preserves identities, money and consent`, async (t) => {
    const f = await fixture(t);
    await f.create();
    await f.join();
    const [p1, p2] = f.players;
    await f.server.stop('SIGKILL');
    const data = JSON.parse(f.bytes());
    const room = data[p1.code];
    delete room.capacity;
    delete room.repaymentCarry;
    room.repaymentCarryCents = -3;
    for (const player of Object.values(room.players)) delete player.recoveryHash;
    room.history = [{ id: 'legacy-bill', ts: 1700000000000, amountCents: 101, payer: 'p1', note: 'old money', faces: 6, rolls: { p1: 1, p2: 1 }, ratio: [1, 1], shares: { p1: 51, p2: 50 }, rerolls: 0 }];
    room.repayments = [{ id: 'legacy-repayment', ts: 1700000001000, from: 'p2', to: 'p1', amountCents: 7 }];
    const details = type === 'repay' ? { from: 'p2', to: 'p1', amountCents: 5 }
      : type === 'deleteRepayment' ? { repaymentId: 'legacy-repayment' }
        : type === 'deleteHistory' ? { historyId: 'legacy-bill' } : {};
    room.ledgerPending = { id: 'legacy-proposal', type, by: 'p1', ...details };
    writeFileSync(f.file, JSON.stringify(data));
    await f.launch();
    await f.connect();
    assert.equal(f.state.capacity, 2);
    assert.deepEqual(f.state.balance, { p1: 43, p2: -43 }, 'persistent balance ignores old temporary carry');
    assert.deepEqual(f.state.repayments, room.repayments);
    assert.deepEqual(f.state.history, room.history.map((bill) => ({ ...bill, participants: ['p1', 'p2'] })));
    proposal(f.state.ledgerPending, ['p1', 'p2'], ['p1'], type);
    assert.equal(f.state.ledgerPending.id, 'legacy-proposal');
    const reset = success(await f.request('/api/recovery', { code: p1.code, token: p1.token }), 'legacy authenticated recovery setup');
    numericRecovery(reset.recoveryCode);
    assert.equal(f.disk().players.p1.token, p1.token);
    assert.equal(f.disk().players.p2.token, p2.token);
    assert.equal(typeof f.disk().players.p1.recoveryHash, 'string');
    assert.ok(!f.bytes().includes(reset.recoveryCode));
    assert.equal(f.disk().repaymentCarryCents, undefined);
    assert.deepEqual(f.disk().repaymentCarry, { p1: -3, p2: 3 });
    f.state = await f.feed.wait((snapshot) => snapshot.version >= f.disk().version);
    await f.reject(p1, 'ledgerRespond', { proposalId: 'legacy-proposal', approve: true });
    await f.reject(p2, 'ledgerRespond', { proposalId: 'wrong-legacy-id', approve: true });
    await f.change(p2, 'ledgerRespond', { proposalId: 'legacy-proposal', approve: true });
    const expected = { repay: 38, deleteRepayment: 50, deleteHistory: -7, clearHistory: 0 }[type];
    assert.deepEqual(f.state.balance, { p1: expected, p2: expected ? -expected : 0 });
    assert.equal(f.state.ledgerPending, null);
    const durable = ledger(f.state);
    await f.restart();
    assert.deepEqual(ledger(f.state), durable);
  });
}

test('repayment cents stay capped at 1e9 even when both net balances exceed the cap', async (t) => {
  const f = await fixture(t);
  await f.create();
  await f.join();
  const [p1, p2] = f.players;
  await f.server.stop('SIGKILL');
  const data = JSON.parse(f.bytes());
  data[p1.code].history = Array.from({ length: 3 }, (_, i) => ({
    id: `large-bill-${i}`, ts: 1700000000000 + i, amountCents: 1000000000,
    payer: 'p1', note: 'valid large bill', faces: 6, rolls: { p1: 1, p2: 3 },
    ratio: [1, 3], shares: { p1: 250000000, p2: 750000000 }, participants: ['p1', 'p2'], rerolls: 0,
  }));
  writeFileSync(f.file, JSON.stringify(data));
  await f.launch();
  await f.connect();
  assert.deepEqual(f.state.balance, { p1: 2250000000, p2: -2250000000 });
  await f.reject(p2, 'repay', { from: 'p2', to: 'p1', amountCents: 1000000001 });
  await f.change(p2, 'repay', { from: 'p2', to: 'p1', amountCents: 1000000000 });
  await f.change(p1, 'ledgerRespond', { proposalId: f.state.ledgerPending.id, approve: true });
  assert.deepEqual(f.state.balance, { p1: 1250000000, p2: -1250000000 });
});

test('largest-remainder ties use canonical room order, including after restart', async (t) => {
  const f = await fixture(t);
  await f.create(3);
  await f.connect();
  await f.join();
  await f.join();
  await f.change(f.players[0], 'setFaces', { faces: 120 });
  await f.change(f.players[0], 'start', { amountCents: 1, payer: 'p2', participants: ['p2', 'p1', 'p3'] });
  await f.server.stop('SIGKILL');
  const data = JSON.parse(f.bytes());
  const room = data[f.players[0].code];
  // Valid saved private rolls force equal highest remainders regardless of the final random roll (1..120).
  room.bill.rolls.p1 = 120;
  room.bill.rolls.p2 = 120;
  writeFileSync(f.file, JSON.stringify(data));
  await f.launch();
  await f.connect();
  privateBill(f.state);
  await f.change(f.players[2], 'roll');
  assert.deepEqual(f.state.bill.participants, ids(3));
  assert.deepEqual(f.state.bill.shares, { p1: 1, p2: 0, p3: 0 });
  assert.deepEqual(f.state.bill.shares, apportioned(1, f.state.bill.rolls, ids(3)));
  await f.confirmAll();
});

test('temporary recovery grants the sole member reconnection time without persisting a room', async (t) => {
  const f = await fixture(t, true);
  await f.create(3);
  await f.connect();
  const old = f.players[0];
  f.feed.expectServerClose();
  const recovered = success(await f.request('/api/recover', { code: old.code, recoveryCode: old.recoveryCode }), 'sole-member recovery');
  await f.feed.waitForServerClose();
  assert.equal(recovered.role, old.role);
  assert.equal(recovered.recoveryCode, old.recoveryCode);
  assert.notEqual(recovered.token, old.token);
  assert.equal((await f.get('/api/ping', { code: old.code, token: old.token })).status, 401);
  assert.equal((await f.get('/api/ping', { code: recovered.code, token: recovered.token })).status, 200, 'revoking the last stream must not immediately destroy the room');
  f.players[0] = recovered;
  await f.connect();
  assert.equal(f.state.phase, 'lobby');
  await f.join();
  await f.join();
  await f.bill(ids(3), 'p1', 1000);
  assert.ok(!existsSync(path.dirname(f.file)));
  await f.feed.close();
  assert.equal((await f.get('/api/ping', { code: recovered.code, token: recovered.token })).status, 401, 'normal last disconnect still destroys a temporary room');
});

test('temporary multiplayer uses all start-time members for replacement consent and signed carry per member', async (t) => {
  const f = await fixture(t, true, { MAX_HISTORY: '1' });
  await f.create(4);
  await f.connect();
  await f.join();
  await f.join();
  const [p1, p2, p3] = f.players;
  await f.bill(ids(3), 'p1', 12000);
  const pay = async (sender, amountCents) => {
    const before = ledger(f.state);
    await f.change(sender, 'repay', { from: sender.role, to: 'p1', amountCents });
    assert.deepEqual(ledger(f.state), before);
    await f.change(p1, 'ledgerRespond', { proposalId: f.state.ledgerPending.id, approve: true });
    assert.equal(f.state.repayments.length, 1);
  };
  await pay(p2, 11);
  await pay(p3, 17);
  await pay(p2, 23);
  assert.deepEqual(carry(f.state), { p1: -28, p2: 11, p3: 17 });
  const previousCarry = structuredClone(carry(f.state));
  await f.change(p2, 'deleteRepayment', { id: f.state.repayments[0].id });
  await f.reject(p3, 'ledgerRespond', { proposalId: f.state.ledgerPending.id, approve: true });
  await f.change(p1, 'ledgerRespond', { proposalId: f.state.ledgerPending.id, approve: true });
  assert.deepEqual(carry(f.state), previousCarry);
  assert.equal(f.state.repayments.length, 0);
  const oldLedger = ledger(f.state);
  await f.change(p1, 'start', { amountCents: 777, payer: 'p2', participants: ['p1', 'p2'] });
  assert.deepEqual(f.state.bill.required, ['p1', 'p2', 'p3']);
  const p4 = await f.join();
  assert.deepEqual(f.state.bill.required, ['p1', 'p2', 'p3']);
  await f.reject(p3, 'roll');
  await f.reject(p4, 'roll');
  await f.rollAll();
  await f.change(p1, 'reroll');
  const reroll = f.state.bill.pending.id;
  proposal(f.state.bill.pending, ids(3), ['p1'], 'reroll');
  await f.change(p2, 'respond', { proposalId: reroll, approve: true });
  assert.equal(f.state.phase, 'result');
  await f.reject(p4, 'respond', { proposalId: reroll, approve: true });
  await f.change(p3, 'respond', { proposalId: reroll, approve: true });
  await f.rollAll();
  await f.change(p1, 'confirm');
  await f.change(p2, 'confirm');
  assert.deepEqual(f.state.history, oldLedger.history);
  assert.deepEqual(carry(f.state), { ...previousCarry, p4: 0 });
  assert.equal(f.state.phase, 'result', 'non-rolling old member must consent before their old ledger is replaced');
  await f.reject(p4, 'confirm');
  await f.change(p3, 'confirm');
  assert.equal(f.state.history.length, 1);
  assert.equal(f.state.history[0].amountCents, 777);
  assert.deepEqual(f.state.history[0].participants, ['p1', 'p2']);
  assert.deepEqual(carry(f.state), { p1: 0, p2: 0, p3: 0, p4: 0 });
  assert.equal(f.state.repayments.length, 0);
  assert.equal(f.state.balance.p3, 0);
  assert.equal(f.state.balance.p4, 0);
  await f.change(p4, 'clearHistory');
  const clear = f.state.ledgerPending.id;
  proposal(f.state.ledgerPending, ids(4), ['p4'], 'clearHistory');
  for (const player of [p1, p2]) await f.change(player, 'ledgerRespond', { proposalId: clear, approve: true });
  assert.equal(f.state.history.length, 1);
  await f.change(p3, 'ledgerRespond', { proposalId: clear, approve: true });
  assert.deepEqual(f.state.balance, { p1: 0, p2: 0, p3: 0, p4: 0 });
  assert.equal(f.state.history.length, 0);
  assert.ok(!existsSync(path.dirname(f.file)));
});
