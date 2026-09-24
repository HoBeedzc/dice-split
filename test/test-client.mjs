import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const html = readFileSync(new URL('../public/index.html', import.meta.url), 'utf8');
const connectionSource = html.slice(html.indexOf('async function connect('), html.indexOf('/* ============ 首页'));
const bindingsSource = html.slice(html.indexOf('function bindRoomEvents()'), html.indexOf('/* ============ 记一笔'));
const response = (status, data = {}) => ({ status, ok: status >= 200 && status < 300, json: async () => data });

function client(outcomes = []) {
  const identity = { code: 'TEST1', token: 'test-token', role: 'p1' };
  const storage = new Map([['dice-split-me', JSON.stringify(identity)]]);
  const timers = new Map();
  const streams = [];
  const requests = [];
  const messages = [];
  let timerId = 0;
  const context = vm.createContext({
    me: identity, room: null, es: null, passcode: '', ME_KEY: 'dice-split-me',
    connectAttempt: 0, connectTimer: null, myLocalRoll: null, rollAnimUntil: 0,
    animTimer: null, lastPhaseKey: '', temporaryMode: false,
    roomPage: 'dice', pageScroll: { dice: 0, ledger: 0 }, noticeExpanded: false,
    recentRecordedId: null, expandedHistory: new Set(),
    encodeURIComponent,
    AbortSignal: { timeout: (ms) => ({ timeout: ms }) },
    localStorage: { removeItem: (key) => storage.delete(key) },
    setTimeout: (callback, delay) => { timers.set(++timerId, { callback, delay }); return timerId; },
    clearTimeout: (id) => timers.delete(id),
    toast: (message) => messages.push(message),
    staleMessage: () => '身份已失效',
    closeModal: () => {},
    render: () => {},
    askPasscode: async () => { throw new Error('不应要求重新输入口令'); },
    fetch: async (url, options) => {
      requests.push({ url, options });
      assert.ok(outcomes.length, '出现预期外的请求');
      const result = outcomes.shift();
      if (result instanceof Error) throw result;
      return result;
    },
    EventSource: class {
      constructor(url) { this.url = url; this.closed = false; streams.push(this); }
      close() { this.closed = true; }
    },
  });
  vm.runInContext(connectionSource, context);
  return {
    context, identity, storage, timers, streams, requests, messages,
    async retry() {
      assert.equal(timers.size, 1, '只能有一个连接重试定时器');
      const [id, timer] = [...timers][0];
      timers.delete(id);
      await timer.callback();
    },
  };
}

for (const status of [429, 500, 502, 503]) {
  test(`连接检查返回 ${status} 时保留身份并自动恢复`, async () => {
    const c = client([response(status), response(200)]);
    await c.context.connect();
    assert.equal(c.context.me, c.identity);
    assert.equal(c.storage.get('dice-split-me'), JSON.stringify(c.identity));
    assert.equal(c.streams.length, 0);
    assert.equal([...c.timers.values()][0].delay, 1000);
    assert.match(c.messages[0], /自动重试/);
    await c.retry();
    assert.equal(c.streams.length, 1);
    assert.equal(c.timers.size, 0);
    assert.equal(c.context.me, c.identity);
    assert.equal(c.requests[0].options.signal.timeout, 10000);
  });
}

test('HTML 错误页、网络异常及超时都可重试', async () => {
  const badGateway = response(502);
  badGateway.json = async () => { throw new SyntaxError('HTML response'); };
  const c = client([badGateway, new TypeError('Failed to fetch'), new Error('TimeoutError'), response(200)]);
  await c.context.connect();
  for (const delay of [1000, 2000, 4000]) {
    assert.equal([...c.timers.values()][0].delay, delay);
    assert.equal(c.context.me, c.identity);
    await c.retry();
  }
  assert.equal(c.streams.length, 1);
});

test('持续失败指数退避且间隔不超过30秒，恢复后重新从1秒开始', async () => {
  const c = client([...Array.from({ length: 7 }, () => response(429)), response(200), response(503)]);
  await c.context.connect();
  for (const delay of [1000, 2000, 4000, 8000, 16000, 30000, 30000]) {
    assert.equal([...c.timers.values()][0].delay, delay);
    await c.retry();
  }
  assert.equal(c.streams.length, 1);
  await c.context.connect();
  assert.equal([...c.timers.values()][0].delay, 1000);
});

test('只有401 stale才清除身份，服务错误不能伪装为身份失效', async () => {
  const c = client([response(503, { error: 'stale' }), response(401, { error: 'stale' })]);
  await c.context.connect();
  assert.equal(c.context.me, c.identity);
  await c.retry();
  assert.equal(c.context.me, null);
  assert.equal(c.storage.size, 0);
  assert.equal(c.timers.size, 0);
  assert.equal(c.streams.length, 0);
});

test('401口令验证仍能正常恢复连接', async () => {
  const c = client([response(401, { error: 'need-passcode' }), response(200)]);
  c.context.askPasscode = async () => { c.context.passcode = 'test-pass'; return 'test-pass'; };
  await c.context.connect();
  assert.equal(c.context.me, c.identity);
  assert.equal(c.streams.length, 1);
  assert.match(c.streams[0].url, /passcode=test-pass/);
  assert.equal(c.timers.size, 0);
});

test('退出取消已安排的重试，迟到定时器也不能发请求', async () => {
  const c = client([response(429)]);
  await c.context.connect();
  const timer = [...c.timers.values()][0];
  c.context.leaveRoom();
  assert.equal(c.timers.size, 0);
  await timer.callback();
  assert.equal(c.requests.length, 1);
  assert.equal(c.context.me, null);
  assert.equal(c.streams.length, 0);
});

for (const late of [response(200), response(401, { error: 'stale' }), response(503)]) {
  test(`旧连接的迟到 ${late.status} 响应不影响新身份`, async () => {
    let resolveOld;
    const pending = new Promise((resolve) => { resolveOld = resolve; });
    const c = client([pending, response(200)]);
    const oldConnect = c.context.connect();
    c.context.leaveRoom();
    const newIdentity = { code: 'TEST2', token: 'other-test-token', role: 'p2' };
    c.context.me = newIdentity;
    await c.context.connect();
    resolveOld(late);
    await oldConnect;
    assert.equal(c.context.me, newIdentity);
    assert.equal(c.streams.length, 1);
    assert.equal(c.streams[0].closed, false);
    assert.equal(c.timers.size, 0);
  });
}

test('退出后才失败的请求不能重新安排连接', async () => {
  let rejectOld;
  const c = client([new Promise((resolve, reject) => { rejectOld = reject; })]);
  const connecting = c.context.connect();
  c.context.leaveRoom();
  rejectOld(new TypeError('Failed to fetch'));
  await connecting;
  assert.equal(c.timers.size, 0);
  assert.equal(c.streams.length, 0);
  assert.equal(c.context.me, null);
});

test('实时流临时失败仍保留身份，确定失效才关闭', async () => {
  const c = client([response(200), response(429), response(503, { error: 'stale' }), response(401, { error: 'stale' })]);
  await c.context.connect();
  const stream = c.streams[0];
  for (let i = 0; i < 2; i++) {
    await stream.onerror();
    assert.equal(c.context.me, c.identity);
    assert.equal(stream.closed, false);
  }
  await stream.onerror();
  assert.equal(c.context.me, null);
  assert.equal(stream.closed, true);
});

test('按钮携带渲染时的提议ID，而不是点击时的新提议ID', async () => {
  const actions = [];
  const buttons = new Map(['a-approve', 'a-reject', 'a-withdraw', 'a-reroll', 'a-void'].map((id) => [id, {
    addEventListener(event, callback) { this.click = callback; },
  }]));
  const c = client();
  c.context.room = { ledgerPending: null, bill: { pending: { id: 'rendered-proposal' } } };
  c.context.document = { getElementById: (id) => buttons.get(id), querySelectorAll: () => [] };
  c.context.$app = { querySelectorAll: () => [] };
  c.context.api = async (url, body) => { actions.push(body); return { ok: true }; };
  vm.runInContext(bindingsSource, c.context);
  c.context.bindRoomEvents();
  c.context.room.bill.pending = { id: 'new-proposal' };
  for (const button of buttons.values()) await button.click();
  assert.deepEqual(actions.map(({ type, proposalId }) => ({ type, proposalId })), [
    { type: 'respond', proposalId: 'rendered-proposal' },
    { type: 'respond', proposalId: 'rendered-proposal' },
    { type: 'withdraw', proposalId: 'rendered-proposal' },
    { type: 'reroll', proposalId: 'rendered-proposal' },
    { type: 'void', proposalId: 'rendered-proposal' },
  ]);
  assert.equal(actions[0].approve, true);
  assert.equal(actions[1].approve, false);
});

test('金额解析只接受正数且最多两位小数', () => {
  const context = vm.createContext({});
  vm.runInContext(html.slice(html.indexOf('function parseCents('), html.indexOf('function openBillSheet(')), context);
  for (const [input, cents] of [['12.01', 1201], ['.50', 50], ['0.01', 1], [' 20 ', 2000]]) {
    assert.equal(context.parseCents(input), cents);
  }
  for (const input of ['', '0', '-1', '0.001', '1e3', '1.2.3', 'NaN', 'Infinity']) {
    assert.equal(context.parseCents(input), null);
  }
});

test('还款表单按收付双方较小余额及单笔上限计算', () => {
  const context = vm.createContext({ me: { role: 'p2' }, room: { balance: { p1: 500, p2: -300, p3: -200 } } });
  vm.runInContext(html.slice(html.indexOf('function repaymentPair('), html.indexOf('function openRepaymentSheet(')), context);
  assert.deepEqual({ ...context.repaymentPair('p1') }, { from: 'p2', to: 'p1', amount: 300 });
  context.me.role = 'p1';
  assert.deepEqual({ ...context.repaymentPair('p3') }, { from: 'p3', to: 'p1', amount: 200 });
  context.room.balance = { p1: 2000000000, p2: -2000000000 };
  assert.equal(context.repaymentPair('p2').amount, 1000000000);
});

test('未参与成员不计算手气，省略还款按成员净额合并', () => {
  const context = vm.createContext({
    memberIds: () => ['p1', 'p2', 'p3'],
    participantsOf: (h) => h.participants,
    room: {
      repaymentCarry: { p1: -10, p2: 10 },
      balance: { p1: 65, p2: -65, p3: 0 },
      history: [{ amountCents: 100, payer: 'p1', participants: ['p1', 'p2'], shares: { p1: 20, p2: 80 } }],
      repayments: [{ from: 'p2', to: 'p1', amountCents: 5 }],
    },
  });
  vm.runInContext(html.slice(html.indexOf('function calcStats('), html.indexOf('function progressHtml(')), context);
  const stats = context.calcStats();
  assert.equal(stats.total, 100);
  assert.equal(stats.byMember.p1.luck, -30);
  assert.equal(stats.byMember.p2.luck, 30);
  assert.equal(stats.byMember.p3.n, 0);
  assert.equal(stats.byMember.p3.luck, 0);
  assert.equal(stats.byMember.p3.share, 0);
  assert.equal(stats.byMember.p1.repaid, -15);
  assert.equal(stats.byMember.p2.repaid, 15);
});

function roomClient() {
  const elements = new Map();
  const details = [];
  const app = { innerHTML: '', classList: { add() {} }, querySelectorAll: (selector) => selector === '[data-history]' ? details : [] };
  const context = vm.createContext({
    me: { code: 'TEST1', role: 'p1', recoveryCode: 'test-recovery' },
    room: {
      code: 'TEST1', capacity: 3, phase: 'idle', faces: 6, bill: null, ledgerPending: null,
      players: Object.fromEntries(['p1', 'p2', 'p3'].map((id) => [id, { name: id, avatar: '', online: true }])),
      history: [], repayments: [], balance: { p1: 0, p2: 0, p3: 0 },
    },
    roomPage: 'dice', pageScroll: { dice: 0, ledger: 0 }, noticeExpanded: false,
    recentRecordedId: null, temporaryMode: true, rollAnimUntil: 0, myLocalRoll: null,
    $app: app, $modal: { firstElementChild: null },
    window: { scrollY: 0, scrollTo({ top }) { this.scrollY = top; } },
    document: { activeElement: null, getElementById: (id) => elements.get(id), querySelectorAll: () => [] },
  });
  for (const id of ['nav-dice', 'nav-ledger']) elements.set(id, {
    id, addEventListener() {}, focus() { context.document.activeElement = this; },
  });
  vm.runInContext(html.slice(html.indexOf('const fmt ='), html.indexOf('let passcode =')), context);
  vm.runInContext(html.slice(html.indexOf('function temporaryNotice('), html.indexOf('const ME_KEY')), context);
  vm.runInContext(html.slice(html.indexOf('function myRole('), html.indexOf('/* ============ 记一笔')), context);
  return { context, app, elements, details };
}

const activeBill = () => ({
  amountCents: 1000, payer: 'p1', participants: ['p1', 'p2'], required: ['p1', 'p2', 'p3'],
  faces: 6, rolled: {}, confirms: {}, rolls: { p1: 2, p2: 3 }, shares: { p1: 400, p2: 600 }, ratio: [2, 3],
});

test('房间默认只展示投骰子，汇总页承载结算和恢复提醒', () => {
  const c = roomClient();
  c.context.renderRoom();
  assert.match(c.app.innerHTML, /<h1 class="title">投骰子分账/);
  assert.match(c.app.innerHTML, /aria-label="房间成员"/);
  assert.match(c.app.innerHTML, /id="a-new"/);
  assert.doesNotMatch(c.app.innerHTML, /aria-label="我的结算"|大家的账|aria-label="身份恢复提醒"/);
  c.context.switchRoomPage('ledger');
  assert.match(c.app.innerHTML, /<h1 class="title">记账汇总/);
  assert.match(c.app.innerHTML, /aria-label="我的结算"/);
  assert.match(c.app.innerHTML, /大家的账|账本还空着/);
  assert.match(c.app.innerHTML, /aria-label="身份恢复提醒"/);
  assert.doesNotMatch(c.app.innerHTML, /aria-label="房间成员"|id="a-new"/);
  assert.equal(c.context.document.activeElement.id, 'nav-ledger');
});

test('导航分别保存滚动位置，实时重绘不抢页面或滚动位置', () => {
  const c = roomClient();
  c.context.window.scrollY = 180;
  c.context.switchRoomPage('ledger');
  assert.equal(c.context.window.scrollY, 0);
  c.context.window.scrollY = 920;
  c.context.renderRoom();
  assert.equal(c.context.roomPage, 'ledger');
  assert.equal(c.context.window.scrollY, 920);
  c.context.switchRoomPage('dice');
  assert.equal(c.context.window.scrollY, 180);
  c.context.switchRoomPage('ledger');
  assert.equal(c.context.window.scrollY, 920);
  c.context.switchRoomPage('dice');
  c.context.switchRoomPage('ledger', true);
  assert.equal(c.context.window.scrollY, 0);
});

test('重绘前保存账单和临时说明的展开状态，即使 toggle 事件尚未触发', () => {
  const c = roomClient();
  c.context.room.history = [{ ...activeBill(), id: 'bill-1', ts: Date.now() }];
  c.context.switchRoomPage('ledger');
  c.details.push({ dataset: { history: 'bill-1' }, open: true, addEventListener() {} });
  c.elements.set('room-notice', { open: true });
  c.context.switchRoomPage('dice');
  c.details.length = 0;
  c.context.switchRoomPage('ledger');
  assert.match(c.app.innerHTML, /data-history="bill-1" open/);
  assert.match(c.app.innerHTML, /id="room-notice" open/);
  c.details.push({ dataset: { history: 'bill-1' }, open: false, addEventListener() {} });
  c.elements.set('room-notice', { open: false });
  c.context.renderRoom();
  assert.doesNotMatch(c.app.innerHTML, /data-history="bill-1" open|id="room-notice" open/);
});

test('投骰导航区分待掷骰、已操作等待、旁观者和临时模式非参与者确认', () => {
  const c = roomClient(), state = c.context;
  assert.equal(state.roomPageStatus('dice'), '');
  state.room.bill = activeBill();
  state.room.phase = 'rolling';
  assert.equal(state.roomPageStatus('dice'), '待掷骰');
  state.room.bill.rolled.p1 = true;
  assert.equal(state.roomPageStatus('dice'), '进行中');
  state.me.role = 'p3';
  assert.equal(state.roomPageStatus('dice'), '进行中');
  state.room.phase = 'result';
  assert.equal(state.roomPageStatus('dice'), '待确认');
  state.room.bill.confirms.p3 = true;
  assert.equal(state.roomPageStatus('dice'), '进行中');
  state.room.bill.required = ['p1', 'p2'];
  delete state.room.bill.confirms.p3;
  assert.equal(state.roomPageStatus('dice'), '进行中');
});

test('两类提议仅对尚未同意的相关成员显示待确认', () => {
  const { context: c } = roomClient();
  c.room.bill = activeBill();
  c.room.phase = 'result';
  const proposal = { by: 'p2', required: ['p1', 'p2'], approvals: { p2: true } };
  c.room.bill.pending = proposal;
  c.room.ledgerPending = proposal;
  for (const page of ['dice', 'ledger']) {
    c.me.role = 'p1';
    assert.equal(c.roomPageStatus(page), '待确认');
    proposal.approvals.p1 = true;
    assert.equal(c.roomPageStatus(page), '进行中');
    delete proposal.approvals.p1;
    c.me.role = 'p2';
    assert.equal(c.roomPageStatus(page), '进行中');
    c.me.role = 'p3';
    assert.equal(c.roomPageStatus(page), '进行中');
  }
});

test('跨页提示解释互斥操作，并将提议和骰子操作留在对应页面', () => {
  const { context: c, app } = roomClient();
  c.room.ledgerPending = { id: 'pending-1', type: 'repay', by: 'p2', from: 'p2', to: 'p1', amountCents: 100, required: ['p1', 'p2'], approvals: { p2: true } };
  c.renderRoom();
  assert.match(app.innerHTML, /id="a-ledger-pending"/);
  assert.doesNotMatch(app.innerHTML, /id="a-new"|id="l-approve"/);
  c.switchRoomPage('ledger');
  assert.match(app.innerHTML, /id="l-approve"/);
  assert.ok(app.innerHTML.indexOf('待确认提议') < app.innerHTML.indexOf('我的结算'));
  c.room.ledgerPending = null;
  c.room.bill = activeBill();
  c.room.phase = 'rolling';
  c.renderRoom();
  assert.match(app.innerHTML, /id="a-dice-pending"/);
  assert.doesNotMatch(app.innerHTML, /aria-label="当前分账"|id="a-roll"/);
  c.switchRoomPage('dice');
  assert.match(app.innerHTML, /aria-label="当前分账"/);
  assert.match(app.innerHTML, /id="a-roll"/);
});

test('临时模式替换同数量记录仍提示入账，首次连接及作废不会误报', async () => {
  const c = client([response(200)]);
  await c.context.connect();
  const emit = (snap) => c.streams[0].onmessage({ data: JSON.stringify(snap) });
  const snap = { history: [{ id: 'old' }], bill: null, phase: 'idle', temporaryMode: true };
  c.context.roomPage = 'ledger';
  emit(snap);
  assert.equal(c.messages.length, 0);
  emit({ ...snap, history: [{ id: 'new' }] });
  assert.equal(c.context.recentRecordedId, 'new');
  assert.match(c.messages[0], /已记入账本/);
  assert.equal(c.context.roomPage, 'ledger');
  emit({ ...snap, history: [{ id: 'new' }] });
  assert.equal(c.messages.length, 1);
  emit({ ...snap, history: [{ id: 'new' }], phase: 'rolling', bill: { rolled: {} } });
  assert.equal(c.context.recentRecordedId, null);
  emit({ ...snap, history: [{ id: 'new' }] });
  assert.equal(c.messages.length, 1);
});

test('退出重置页面、滚动位置与展开状态，新房间从投骰页开始', () => {
  const c = client();
  c.context.roomPage = 'ledger';
  c.context.pageScroll.ledger = 900;
  c.context.noticeExpanded = true;
  c.context.recentRecordedId = 'old';
  c.context.expandedHistory.add('old');
  c.context.leaveRoom();
  assert.equal(c.context.roomPage, 'dice');
  assert.equal(c.context.pageScroll.ledger, 0);
  assert.equal(c.context.noticeExpanded, false);
  assert.equal(c.context.recentRecordedId, null);
  assert.equal(c.context.expandedHistory.size, 0);
});
