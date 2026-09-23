// 骰子分账 API 全流程测试。自启动随机端口，只读写自己创建的临时账本。
// 用法：node test/test-flow.mjs
import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { mkdirSync, readFileSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual as equal } from 'node:util';

const ROOT = fileURLToPath(new URL('../', import.meta.url));

// 供 API 测试文件共用；import 本文件不会运行全流程测试。
export async function deadline(promise, label, ms = 5000) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`${label}超时 (${ms}ms)`)), ms); }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

export class TestServer {
  constructor(dataFile, env = {}) {
    this.dataFile = dataFile;
    this.base = null;
    this.logs = '';
    this.failure = null;
    this.expectedExit = false;
    this.streams = new Set();
    this.dead = new Promise((_, reject) => { this.rejectDead = reject; });
    this.dead.catch(() => {});
    this.started = new Promise((resolve) => { this.resolveStarted = resolve; });
    this.child = spawn(process.execPath, [path.join(ROOT, 'server.js'), '0'], {
      cwd: ROOT,
      env: {
        ...process.env, PASSCODE: '', MAX_ROOMS: '100', MAX_HISTORY: '1000', RATE_LIMIT: '100000',
        ...env, DICE_DATA: dataFile,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    this.closed = new Promise((resolve) => {
      this.child.once('close', (code, signal) => { this.exit = { code, signal }; resolve(this.exit); });
    });
    this.child.once('error', (error) => this.setFailure(new Error(`子进程 error: ${error.message}`)));
    this.child.once('exit', (code, signal) => {
      if (!this.expectedExit) this.setFailure(new Error(`子进程意外退出: code=${code}, signal=${signal}\n${this.logs}`));
    });
    this.child.stderr.on('data', (chunk) => { this.logs = (this.logs + chunk).slice(-20000); });
    let stdout = '';
    this.child.stdout.on('data', (chunk) => {
      stdout = (stdout + chunk).slice(-20000);
      this.logs = (this.logs + chunk).slice(-20000);
      const match = stdout.match(/http:\/\/(?:localhost|127\.0\.0\.1|\[::\]|0\.0\.0\.0):(\d+)/);
      if (match && Number(match[1]) > 0 && Number(match[1]) <= 65535 && !this.base) {
        this.base = `http://127.0.0.1:${Number(match[1])}`;
        this.resolveStarted();
      }
    });
  }

  setFailure(error) {
    if (!this.failure) { this.failure = error; this.rejectDead(error); }
  }

  guard(promise, label, ms = 5000) {
    return deadline(Promise.race([promise, this.dead]), label, ms);
  }

  async start() {
    await this.guard(this.started, '等待实际监听端口', 10000);
    return this;
  }

  async request(method, url, body, headers = {}) {
    if (this.failure) throw this.failure;
    return this.guard((async () => {
      const response = await fetch(this.base + url, {
        method,
        headers: { ...(body === undefined ? {} : { 'Content-Type': 'application/json' }), ...headers },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(5000),
      });
      const text = await response.text();
      let data = null;
      try { data = JSON.parse(text); } catch { /* 首页返回 HTML。 */ }
      return { status: response.status, data, text, headers: response.headers };
    })(), `${method} ${url}`);
  }

  async observe(code, token, passcode) {
    const feed = new SnapshotFeed(this);
    this.streams.add(feed);
    const query = new URLSearchParams({ code, token });
    if (passcode !== undefined) query.set('passcode', passcode);
    feed.task = (async () => {
      const response = await fetch(`${this.base}/events?${query}`, { signal: feed.controller.signal });
      if (response.status !== 200 || !response.headers.get('content-type')?.includes('text/event-stream')) {
        throw new Error(`SSE 连接失败: ${response.status}`);
      }
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) {
            feed.ended = true;
            if (!feed.closing && !feed.expectEnd) throw new Error('SSE 意外结束');
            return;
          }
          buffer += decoder.decode(value, { stream: true });
          let boundary;
          while ((boundary = buffer.indexOf('\n\n')) !== -1) {
            const event = buffer.slice(0, boundary);
            buffer = buffer.slice(boundary + 2);
            const data = event.split('\n').filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trimStart()).join('\n');
            if (data) {
              feed.frames.push(JSON.parse(data));
              for (const notify of feed.listeners) notify();
            }
          }
        }
      } finally {
        reader.releaseLock();
      }
    })().catch((error) => {
      if (feed.expectEnd && !feed.closing) feed.ended = true;
      else if (!feed.closing) this.setFailure(error);
    });
    await feed.wait(() => true, 'SSE 初始快照');
    return feed;
  }

  async stop(signal = 'SIGTERM') {
    this.expectedExit = true;
    const errors = [];
    for (const feed of [...this.streams]) {
      try { await feed.close(); } catch (error) { errors.push(error); }
    }
    if (!this.exit) {
      this.child.kill(signal);
      try {
        await deadline(this.closed, '等待自己的子进程退出');
      } catch (error) {
        errors.push(error);
        this.child.kill('SIGKILL');
        await deadline(this.closed, '等待 SIGKILL 子进程退出');
      }
    }
    if (this.failure) errors.push(this.failure);
    if (errors.length) throw new AggregateError(errors, '服务实例清理失败');
    return this.exit;
  }
}

class SnapshotFeed {
  constructor(server) {
    this.server = server;
    this.controller = new AbortController();
    this.frames = [];
    this.listeners = new Set();
    this.closing = false;
  }

  get latest() { return this.frames.at(-1); }

  async wait(predicate, label = '等待快照', after = 0) {
    let notify;
    try {
      return await this.server.guard(new Promise((resolve, reject) => {
        notify = () => {
          try {
            if (this.frames.length > after && predicate(this.latest)) resolve(this.latest);
          } catch (error) { reject(error); }
        };
        this.listeners.add(notify);
        notify();
      }), label);
    } finally {
      this.listeners.delete(notify);
    }
  }

  async noEventsSince(index, ms = 150) {
    let notify, timer;
    try {
      await this.server.guard(new Promise((resolve, reject) => {
        notify = () => { if (this.frames.length !== index) reject(new Error('失败请求广播了成功状态')); };
        this.listeners.add(notify);
        notify();
        timer = setTimeout(resolve, ms);
      }), '观察失败请求不广播');
    } finally {
      clearTimeout(timer);
      this.listeners.delete(notify);
    }
  }

  expectServerClose() { this.expectEnd = true; }

  async waitForServerClose() {
    await this.server.guard(this.task, '恢复身份必须关闭旧 SSE');
    if (!this.ended || this.closing || this.controller.signal.aborted) {
      throw new Error('旧 SSE 必须由服务端终止，不能由测试主动关闭');
    }
  }

  async close() {
    this.closing = true;
    this.controller.abort();
    try { await deadline(this.task, '关闭 SSE'); }
    finally { this.server.streams.delete(this); }
  }
}

export function memberIds(room) {
  return Object.keys(room.players).filter((id) => room.players[id]).sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
}

export function persistentView(room, snapshot = false) {
  const bill = room.bill;
  return {
    capacity: room.capacity,
    phase: room.phase,
    faces: room.faces,
    players: Object.fromEntries(Object.keys(room.players).map((role) => [role, room.players[role] && {
      name: room.players[role].name, avatar: room.players[role].avatar,
    }])),
    bill: bill && {
      amountCents: bill.amountCents, note: bill.note, payer: bill.payer, faces: bill.faces,
      participants: bill.participants, required: bill.required,
      rerolls: bill.rerolls, pending: bill.pending || null, confirms: bill.confirms,
      rolled: snapshot ? bill.rolled : Object.fromEntries(bill.participants.map((id) => [id, bill.rolls[id] != null])),
      ...(room.phase === 'result' ? { rolls: bill.rolls, ratio: bill.ratio, shares: bill.shares } : {}),
    },
    history: room.history, repayments: room.repayments, ledgerPending: room.ledgerPending,
    repaymentCarry: room.temporaryMode === true ? room.repaymentCarry : undefined,
  };
}

export function expectedBalance(room) {
  const balance = Object.fromEntries(memberIds(room).map((id) => [id, room.temporaryMode === true ? (room.repaymentCarry[id] ?? 0) : 0]));
  for (const bill of room.history) {
    balance[bill.payer] += bill.amountCents;
    for (const [id, share] of Object.entries(bill.shares)) balance[id] -= share;
  }
  for (const repayment of room.repayments) {
    balance[repayment.from] += repayment.amountCents;
    balance[repayment.to] -= repayment.amountCents;
  }
  return balance;
}

export function hasRuntimeFields(value) {
  if (!value || typeof value !== 'object') return false;
  return Object.entries(value).some(([key, child]) => key.startsWith('_') || key === 'online' || hasRuntimeFields(child));
}

async function runFlow() {
  const PASS = 'flow-测试口令';
  let pass = 0, fail = 0;
  const ok = (condition, name) => {
    if (condition) { pass++; console.log(`  PASS ${name}`); }
    else { fail++; console.error(`  FAIL ${name}`); }
  };
  const must = (condition, message) => { if (!condition) throw new Error(message); };
  const instances = [];
  let temp, server, feed, p1, p2, s;
  const disk = () => JSON.parse(readFileSync(server.dataFile, 'utf8'));
  const room = () => disk()[p1.code];
  const req = (method, url, body, headers) => server.request(method,
    method === 'GET' ? `${url}${url.includes('?') ? '&' : '?'}passcode=${encodeURIComponent(PASS)}` : url,
    method === 'POST' ? { ...body, passcode: PASS } : body, headers);
  const act = (player, type, fields = {}) => req('POST', '/api/action', { code: p1.code, token: player.token, type, ...fields });
  const contracts = (snapshot) => {
    must(snapshot.temporaryMode === false, '有口令快照必须声明非临时模式');
    must(Array.isArray(snapshot.repayments), 'snapshot.repayments 必须是数组');
    must(snapshot.ledgerPending === null || (snapshot.ledgerPending && typeof snapshot.ledgerPending === 'object'), 'snapshot.ledgerPending 必须是对象或 null');
    must(Number.isSafeInteger(snapshot.balance?.p1) && Number.isSafeInteger(snapshot.balance?.p2), 'balance 必须使用整数分');
    must(snapshot.balance.p1 === -snapshot.balance.p2 && equal(snapshot.balance, expectedBalance(snapshot)), 'balance 必须由消费和还款计算，正数应收');
  };
  const change = async (player, type, fields = {}) => {
    const beforeVersion = room().version;
    const index = feed.frames.length;
    const response = await act(player, type, fields);
    must(response.status === 200, `${type} 失败: ${response.status} ${JSON.stringify(response.data)}`);
    // 响应后立即同步读取，不等待 SSE 或延时，捕获防抖写入/先返回后持久化。
    const saved = room();
    must(saved.version > beforeVersion, `${type} 响应成功但没有立即持久化新版本`);
    s = await feed.wait((snap) => equal(persistentView(snap, true), persistentView(saved)), `${type} 广播必须与已落盘状态一致`, index);
    contracts(s);
    return { ...response, saved };
  };
  const rejected = async (player, type, fields, name) => {
    const before = readFileSync(server.dataFile, 'utf8');
    const index = feed.frames.length;
    const result = await act(player, type, fields);
    ok(result.status >= 400 && result.status < 500, name);
    await feed.noEventsSince(index);
    ok(readFileSync(server.dataFile, 'utf8') === before, `${name}：文件不变且不广播`);
    return result;
  };
  const records = () => ({ history: structuredClone(s.history), repayments: structuredClone(s.repayments), balance: { ...s.balance } });
  const unchanged = (before, name) => ok(equal(records(), before), name);
  const privateRolls = (snap) => snap.phase === 'rolling' && snap.bill.rolls === undefined && snap.bill.roll === undefined && snap.bill.ratio === undefined && snap.bill.shares === undefined;
  const addBill = async (amountCents, payer, note) => {
    await change(p1, 'start', { amountCents, payer, note });
    await change(p1, 'roll');
    await change(p2, 'roll');
    await change(p1, 'confirm');
    await change(p2, 'confirm');
    return s.history[0];
  };
  const approve = (player, proposalId) => change(player, 'ledgerRespond', { proposalId, approve: true });
  const failedWrite = async (player, type, fields, name) => {
    const before = readFileSync(server.dataFile, 'utf8');
    const beforeState = persistentView(s, true);
    const index = feed.frames.length;
    const blocker = `${server.dataFile}.tmp-${server.child.pid}`;
    mkdirSync(blocker); // 仅在自己的临时目录制造写入失败；不改权限。
    try {
      const result = await act(player, type, fields);
      ok(result.status >= 400, `${name}：写入失败返回非 2xx`);
      ok(readFileSync(server.dataFile, 'utf8') === before, `${name}：原始文件逐字节不变`);
      await feed.noEventsSince(index);
      ok(true, `${name}：没有成功广播`);
      const refreshed = await server.observe(p1.code, p2.token, PASS);
      try {
        ok(equal(persistentView(refreshed.latest, true), beforeState), `${name}：重新连接无幽灵内存状态`);
      } finally { await refreshed.close(); }
      ok(readFileSync(server.dataFile, 'utf8') === before, `${name}：刷新也不覆盖旧文件`);
    } finally {
      rmSync(blocker, { recursive: true, force: true });
    }
  };

  try {
    temp = await mkdtemp(path.join(tmpdir(), 'dice-split-flow-'));
    const dataFile = path.join(temp, 'ledger.json');
    server = new TestServer(dataFile, { PASSCODE: PASS });
    instances.push(server);
    await server.start();

    console.log('—— 建房 / 加入 ——');
    const created = await req('POST', '/api/create', { name: '大熊' });
    p1 = created.data;
    must(created.status === 200 && p1?.code?.length === 5 && p1.role === 'p1' && p1.token, '建房失败');
    ok(room().players.p1.token === p1.token, '建房响应前身份已写盘');
    ok((await req('GET', `/api/ping?code=${p1.code}&token=${p1.token}`)).data.role === 'p1', 'ping 校验 P1 身份');
    ok((await req('GET', `/api/ping?code=${p1.code}&token=badtoken`)).status === 401, '错误 token 被拒');
    ok((await req('GET', `/api/ping?code=INVALID&token=${p1.token}`)).status === 401, '不存在的房间被拒');
    const joined = await req('POST', '/api/join', { name: '小猫', code: p1.code });
    p2 = joined.data;
    must(joined.status === 200 && p2?.role === 'p2' && p2.token, 'P2 加入失败');
    ok(room().players.p2.token === p2.token && room().phase === 'idle', '加入响应前已写盘');
    const full = await req('POST', '/api/join', { name: '路人', code: p1.code });
    ok(full.status === 409 && full.data.error === 'full', '第三人加入被拒 (409 full)');
    const offlineBytes = readFileSync(dataFile, 'utf8');
    const offlineMtime = statSync(dataFile).mtimeMs;
    feed = await server.observe(p1.code, p2.token, PASS);
    s = await feed.wait((snap) => snap.players.p2.online, 'P2 上线广播');
    contracts(s);
    ok(s.players.p1.name === '大熊' && s.ledgerPending === null && s.repayments.length === 0, 'SSE 初始快照包含新契约字段');
    ok(readFileSync(dataFile, 'utf8') === offlineBytes && statSync(dataFile).mtimeMs === offlineMtime, '仅上线不写盘');

    console.log('—— 掷骰保密 / 刷新恢复 ——');
    await change(p1, 'start', { amountCents: 10000, note: '晚饭火锅', payer: 'p1' });
    ok(privateRolls(s) && s.bill.faces === 6 && equal(s.bill.rolled, { p1: false, p2: false }), '开账锁定面数且不泄露点数');
    const r2 = (await change(p2, 'roll')).data;
    ok(Number.isInteger(r2.roll) && r2.roll >= 1 && r2.roll <= 6, 'P2 掷骰返回自己的合法点数');
    ok(privateRolls(s) && equal(s.bill.rolled, { p1: false, p2: true }), 'P2 已掷但全局快照无任何 roll');
    // 同一身份断线后重连；刷新只恢复 rolled，连自己的点数也不通过快照补发。
    await feed.close();
    feed = await server.observe(p1.code, p2.token, PASS);
    s = await feed.wait((snap) => snap.players.p2.online && snap.bill.rolled.p2, '刷新恢复已掷状态');
    ok(privateRolls(s) && s.bill.faces === 6 && !s.bill.rolled.p1, '刷新恢复锁定面数和 rolled，但不泄露自己的点数');
    const opponent = await server.observe(p1.code, p1.token, PASS);
    try {
      await feed.wait((snap) => snap.players.p1.online, '对方测试连接上线');
      ok(privateRolls(opponent.latest) && opponent.latest.bill.rolled.p2, '对方刷新同样不能看到 P2 点数');
    } finally { await opponent.close(); }
    await feed.wait((snap) => !snap.players.p1.online, '对方测试连接关闭');
    await rejected(p2, 'roll', {}, '刷新后重复掷骰被拒');
    const r1 = (await change(p1, 'roll')).data;
    ok(s.phase === 'result' && s.bill.rolls.p1 === r1.roll && s.bill.rolls.p2 === r2.roll, '双方掷完才揭晓');
    await rejected(p1, 'roll', {}, '揭晓后不能再次掷骰');
    const gcd = (a, b) => b ? gcd(b, a % b) : a;
    const divisor = gcd(r1.roll, r2.roll);
    ok(equal(s.bill.ratio, [r1.roll / divisor, r2.roll / divisor]), '比例正确化简');
    ok(s.bill.shares.p1 === Math.round(10000 * r1.roll / (r1.roll + r2.roll)) && s.bill.shares.p1 + s.bill.shares.p2 === 10000, '金额拆分与尾差合计正确');

    console.log('—— 分账提议身份 / 迟到请求 / 同名动作防绕过 ——');
    for (const type of ['reroll', 'void']) {
      await change(p1, type); // 新提议不带 proposalId。
      const id = s.bill.pending.id;
      ok(typeof id === 'string' && id.length > 0 && s.bill.pending.type === type && s.bill.pending.by === 'p1' && equal(s.bill.pending.required, ['p1', 'p2']) && s.bill.pending.approvals.p1 === true && !s.bill.pending.approvals.p2 && Object.keys(s.bill.pending.approvals).every((role) => ['p1', 'p2'].includes(role)), `${type} 生成带 ID、全员 required 和发起人自动同意的提议`);
      for (const proposalId of [undefined, 'wrong', null, [id]]) {
        const label = `${type} ID=${JSON.stringify(proposalId)}`;
        await rejected(p2, 'respond', { proposalId, approve: true }, `${label} 不能回应`);
        await rejected(p1, 'withdraw', { proposalId }, `${label} 不能撤回`);
        await rejected(p2, type, { proposalId }, `${label} 不能用同名动作确认`);
      }
      for (const approveValue of [undefined, null, 0, 1, 'true', 'false', {}, []]) {
        await rejected(p2, 'respond', { proposalId: id, approve: approveValue }, `${type} approve=${JSON.stringify(approveValue)} 必须为 boolean`);
      }
      for (const approveValue of [true, false]) {
        await rejected(p1, 'respond', { proposalId: id, approve: approveValue }, `${type} 发起人不能自行回应 ${approveValue}`);
      }
      await rejected(p2, 'withdraw', { proposalId: id }, `${type} 他人不能撤回`);
      await rejected(p1, type, {}, `${type} 重复提议不能覆盖当前 ID`);
      await rejected(p1, type, { proposalId: id }, `${type} 发起人不能用同名动作自批准`);
      await rejected(p2, type === 'reroll' ? 'void' : 'reroll', { proposalId: id }, `${type} 当前 ID 也不能确认不同类型`);
      await change(p1, 'withdraw', { proposalId: id });
      for (const [player, action, fields] of [
        [p2, 'respond', { proposalId: id, approve: true }],
        [p2, 'respond', { proposalId: id, approve: false }],
        [p1, 'withdraw', { proposalId: id }],
      ]) {
        await rejected(player, action, fields, `${type} 撤回后重复 ${action}/${fields.approve} 被拒`);
      }
      // result 阶段仍允许新提议，确保拒绝不是仅靠阶段检查。
      for (const action of ['reroll', 'void']) {
        for (const proposalId of [id, null, '']) {
          await rejected(p2, action, { proposalId }, `无 pending 的 ${action} 不能把携带 ID 的请求变成新提议`);
        }
      }
      for (const replacement of type === 'reroll' ? ['reroll', 'void'] : ['void']) {
        await change(p1, replacement);
        const nextId = s.bill.pending.id;
        ok(typeof nextId === 'string' && nextId.length > 0 && nextId !== id, `${type} 撤回改提 ${replacement} 使用新 ID`);
        for (const approveValue of [true, false]) {
          await rejected(p2, 'respond', { proposalId: id, approve: approveValue }, `旧 ${type} 回应 ${approveValue} 不影响新 ${replacement}`);
        }
        await rejected(p1, 'withdraw', { proposalId: id }, `旧 ${type} 撤回不影响新 ${replacement}`);
        for (const action of new Set([type, replacement])) {
          await rejected(p2, action, { proposalId: id }, `旧 ${action} 同名确认不影响新 ${replacement}`);
        }
        ok(s.bill.pending.id === nextId && room().bill.pending.id === nextId, '所有迟到请求后仍保留新提议');
        await change(p1, 'withdraw', { proposalId: nextId });
      }
    }

    console.log('—— 双人重掷 / 作废 / 确认入账 ——');
    await change(p2, 'reroll');
    ok(s.phase === 'result' && s.bill.pending?.type === 'reroll' && s.bill.pending.by === 'p2', '重掷先提议');
    await rejected(p1, 'confirm', {}, '有提议时确认被拒');
    const approvedRerollId = s.bill.pending.id;
    await change(p1, 'respond', { proposalId: approvedRerollId, approve: true });
    ok(s.phase === 'rolling' && s.bill.rerolls === 1 && s.bill.pending === null && s.bill.faces === 6 && equal(s.bill.rolled, { p1: false, p2: false }), '同意重掷重置 rolled，保留锁定面数');
    await rejected(p1, 'respond', { proposalId: approvedRerollId, approve: true }, '重复同意重掷不执行第二次');
    await change(p1, 'roll');
    await change(p2, 'roll');
    ok(s.bill.shares.p1 + s.bill.shares.p2 === 10000, '重掷后金额合计不变');
    await change(p1, 'void');
    const declinedVoidId = s.bill.pending.id;
    await change(p2, 'respond', { proposalId: declinedVoidId, approve: false });
    ok(s.phase === 'result' && s.bill.pending === null, '拒绝作废保持揭晓');
    await rejected(p2, 'respond', { proposalId: declinedVoidId, approve: false }, '重复拒绝作废被拒');
    await change(p2, 'reroll');
    await change(p2, 'withdraw', { proposalId: s.bill.pending.id });
    ok(s.phase === 'result' && s.bill.pending === null, '发起方撤回重掷');
    await change(p1, 'reroll');
    const sameRerollId = s.bill.pending.id;
    await change(p2, 'reroll', { proposalId: sameRerollId });
    ok(s.phase === 'rolling' && s.bill.rerolls === 2 && s.bill.pending === null, '双方都点重掷视为同意');
    await rejected(p2, 'reroll', { proposalId: sameRerollId }, '重复同名重掷确认被拒');
    await change(p1, 'roll');
    await change(p2, 'roll');
    await change(p1, 'confirm');
    ok(s.phase === 'result' && s.bill.confirms.p1 && s.history.length === 0, '单方确认不入账');
    await change(p2, 'confirm');
    ok(s.phase === 'idle' && s.bill === null && s.history.length === 1, '双方确认后才入账');
    ok(s.history[0].payer === 'p1' && s.history[0].note === '晚饭火锅', '保留垫付人和备注');
    const firstId = s.history[0].id;
    await change(p1, 'start', { amountCents: 5000, note: '奶茶', payer: 'p2' });
    for (const [type, fields] of [['deleteHistory', { id: firstId }], ['clearHistory', {}], ['repay', { from: 'p2', to: 'p1', amountCents: 1 }], ['deleteRepayment', { id: 'missing' }]]) {
      await rejected(p1, type, fields, `${type} 在 rolling 阶段被拒`);
    }
    await change(p1, 'roll');
    await change(p2, 'void');
    ok(s.phase === 'rolling' && s.bill.pending?.type === 'void' && s.bill.pending.by === 'p2', '掷骰中可以提议作废');
    await rejected(p2, 'roll', {}, '有作废提议时掷骰被拒');
    const approvedVoidId = s.bill.pending.id;
    await change(p1, 'respond', { proposalId: approvedVoidId, approve: true });
    ok(s.phase === 'idle' && s.bill === null && s.history.length === 1, '双方作废不入账');
    await rejected(p1, 'respond', { proposalId: approvedVoidId, approve: true }, '重复同意作废被拒');
    await change(p1, 'start', { amountCents: 100, payer: 'p1' });
    await change(p1, 'void');
    const sameVoidId = s.bill.pending.id;
    await change(p2, 'void', { proposalId: sameVoidId });
    ok(s.phase === 'idle' && s.bill === null && s.history.length === 1, '双方带当前 ID 点作废视为同意且不入账');
    await rejected(p2, 'void', { proposalId: sameVoidId }, '重复同名作废确认被拒');

    console.log('—— 删除账目提议 / 身份 / 防重放 ——');
    await rejected(p1, 'deleteHistory', { id: 'nope' }, '不存在的消费不能删除');
    const beforeDelete = records();
    await change(p1, 'deleteHistory', { id: firstId });
    const oldId = s.ledgerPending.id;
    ok(typeof oldId === 'string' && oldId.length > 0 && s.ledgerPending.type === 'deleteHistory' && s.ledgerPending.by === 'p1' && s.ledgerPending.historyId === firstId, '删除产生带唯一 ID 的账本提议');
    unchanged(beforeDelete, '删除提议本身不改账');
    await rejected(p1, 'ledgerRespond', { proposalId: oldId, approve: true }, '发起人不能自行批准');
    await rejected(p2, 'ledgerWithdraw', { proposalId: oldId }, '另一人不能撤回');
    await rejected(p2, 'ledgerRespond', { proposalId: 'wrong', approve: true }, '回应必须匹配 ID');
    await rejected(p2, 'ledgerRespond', { approve: true }, '回应不能省略 ID');
    for (const approveValue of [undefined, null, 0, 1, 'true', {}, []]) {
      await rejected(p2, 'ledgerRespond', { proposalId: oldId, approve: approveValue }, `approve=${JSON.stringify(approveValue)} 不是 boolean`);
    }
    await rejected(p1, 'start', { amountCents: 100 }, '待处理账本提议冻结开账');
    for (const [type, fields] of [['clearHistory', {}], ['deleteHistory', { id: firstId }], ['repay', { from: 'p2', to: 'p1', amountCents: 1 }], ['deleteRepayment', { id: 'missing' }]]) {
      await rejected(p2, type, fields, `已有账本提议时拒绝 ${type}`);
    }
    await change(p2, 'ledgerRespond', { proposalId: oldId, approve: false });
    ok(s.ledgerPending === null, '另一人可拒绝');
    unchanged(beforeDelete, '拒绝删除不改记录与余额');
    await change(p1, 'deleteHistory', { id: firstId });
    const withdrawnId = s.ledgerPending.id;
    ok(withdrawnId !== oldId, '新提议不复用旧 ID');
    await rejected(p2, 'ledgerRespond', { proposalId: oldId, approve: true }, '迟到的旧同意不能批准新提议');
    await rejected(p1, 'ledgerWithdraw', { proposalId: oldId }, '旧撤回不能撤掉新提议');
    await rejected(p1, 'ledgerWithdraw', {}, '撤回不能省略 ID');
    await change(p1, 'ledgerWithdraw', { proposalId: withdrawnId });
    unchanged(beforeDelete, '发起人撤回不改记录与余额');
    await change(p1, 'deleteHistory', { id: firstId });
    const deleteId = s.ledgerPending.id;
    ok(deleteId !== withdrawnId && deleteId !== oldId, '撤回后重新提议仍使用新 ID');
    await rejected(p1, 'ledgerWithdraw', { proposalId: withdrawnId }, '重复撤回不会删除新提议');
    await approve(p2, deleteId);
    ok(s.history.length === 0 && s.ledgerPending === null && s.balance.p1 === 0, '双人同意后删除消费并重新计算余额');
    await rejected(p2, 'ledgerRespond', { proposalId: deleteId, approve: true }, '重复批准被拒');
    await rejected(p1, 'repay', { from: 'p2', to: 'p1', amountCents: 1 }, '没有欠款不能还款');

    console.log('—— 金额边界 / 换面数 ——');
    for (const amountCents of [0, -1, 1000000001, 'not-money']) {
      await rejected(p1, 'start', { amountCents }, `非法消费金额 ${amountCents} 被拒`);
    }
    await addBill(333, 'p1', '零头');
    ok(s.history.length === 1 && s.history[0].amountCents === 333, '3.33 元小账入账');
    for (const faces of [1, 121]) await rejected(p1, 'setFaces', { faces }, `面数 ${faces} 被拒`);
    for (const faces of [2, 120, 8]) {
      await change(p1, 'setFaces', { faces });
      ok(s.faces === faces, `可设置 ${faces} 面骰`);
    }
    await change(p1, 'start', { amountCents: 800, note: '8面骰', payer: 'p1' });
    ok(s.faces === 8 && s.bill.faces === 8, '房间与账目均带锁定面数');
    await rejected(p2, 'setFaces', { faces: 20 }, '掷骰中不能换面数');
    const r8 = (await change(p1, 'roll')).data;
    ok(Number.isInteger(r8.roll) && r8.roll >= 1 && r8.roll <= 8, '8 面骰点数合法');
    const refresh8 = await server.observe(p1.code, p2.token, PASS);
    try {
      ok(refresh8.latest.bill.faces === 8 && refresh8.latest.bill.rolled.p1 && privateRolls(refresh8.latest), '8 面账刷新保留面数与保密状态');
    } finally { await refresh8.close(); }
    await change(p2, 'roll');
    await rejected(p2, 'setFaces', { faces: 6 }, '揭晓中也不能换面数');
    await rejected(p1, 'clearHistory', {}, 'result 阶段不能提议清空');
    await change(p1, 'confirm');
    await change(p2, 'confirm');
    ok(s.history.length === 2 && s.history[0].faces === 8, '消费记录保存当时面数');

    console.log('—— 换手机安全恢复身份 ——');
    const previousToken = p2.token;
    const beforeRecovery = persistentView(s, true);
    const recoveryCode = p2.recoveryCode;
    must(typeof recoveryCode === 'string' && recoveryCode.length > 0, '加入必须返回私密恢复码');
    const claim = await req('POST', '/api/join', { code: p1.code, name: '不能冒领', claimRole: 'p2' });
    ok(claim.status >= 400 && claim.status < 500, '知道房间码和角色不能冒领身份');
    feed.expectServerClose();
    const recovered = await req('POST', '/api/recover', { code: p1.code, recoveryCode, name: '不能偷偷改名' });
    must(recovered.status === 200, '恢复失败');
    p2 = recovered.data;
    ok(p2.role === 'p2' && p2.code === p1.code && p2.recoveryCode === recoveryCode && p2.token !== previousToken && room().players.p2.token === p2.token, '恢复保留角色及恢复码，新 token 响应前立即落盘');
    await feed.waitForServerClose();
    ok((await req('GET', `/api/ping?code=${p1.code}&token=${previousToken}`)).status === 401, '旧 token 失效');
    feed = await server.observe(p1.code, p2.token, PASS);
    s = await feed.wait((snap) => snap.players.p2.online, '新身份上线');
    ok(equal(persistentView(s, true), beforeRecovery), '恢复不改名字、账本或分账状态');
    await change(p2, 'setMe', { name: '小猫改名' });
    ok(room().players.p2.name === '小猫改名' && !hasRuntimeFields(disk()), '改名立即持久化，online/_conns 等运行时字段不落盘');

    console.log('—— 双人还款 / 净余额 / 并发提议 ——');
    const debt = s.balance.p1;
    must(debt > 2, '测试消费应产生 P2 对 P1 的欠款');
    for (const amountCents of [undefined, null, 0, -1, 1.5, '1', true, Number.MAX_SAFE_INTEGER + 1, debt + 1]) {
      await rejected(p1, 'repay', { from: 'p2', to: 'p1', amountCents }, `还款金额 ${JSON.stringify(amountCents)} 不合法`);
    }
    const partial = Math.max(1, Math.floor(debt / 3));
    const beforeRepay = records();
    await change(p1, 'repay', { from: 'p2', to: 'p1', amountCents: partial });
    let proposal = s.ledgerPending;
    ok(proposal.type === 'repay' && proposal.by === 'p1' && proposal.from === 'p2' && proposal.to === 'p1' && proposal.amountCents === partial, '应收方也能提议，明确的付款方向符合净额');
    unchanged(beforeRepay, '还款提议未确认不改变记录或余额');
    await rejected(p1, 'ledgerRespond', { proposalId: proposal.id, approve: true }, '还款不能自行批准');
    await change(p2, 'ledgerRespond', { proposalId: proposal.id, approve: false });
    unchanged(beforeRepay, '拒绝还款不改账');
    await change(p2, 'repay', { from: 'p2', to: 'p1', amountCents: partial });
    proposal = s.ledgerPending;
    ok(proposal.by === 'p2' && proposal.from === 'p2' && proposal.to === 'p1', '欠款方也可发起且方向一致');
    await change(p2, 'ledgerWithdraw', { proposalId: proposal.id });
    unchanged(beforeRepay, '撤回还款不改账');

    const concurrent = await Promise.all([act(p1, 'repay', { from: 'p2', to: 'p1', amountCents: 1 }), act(p2, 'clearHistory')]);
    ok(concurrent.filter((result) => result.status === 200).length === 1 && concurrent.filter((result) => result.status >= 400 && result.status < 500).length === 1, '并发提议只接受一个');
    const concurrentSaved = room();
    s = await feed.wait((snap) => equal(persistentView(snap, true), persistentView(concurrentSaved)), '并发提议最终状态');
    unchanged(beforeRepay, '并发提议不会直接改账');
    const winner = concurrent[0].status === 200 ? p1 : p2;
    ok(s.ledgerPending.by === winner.role, '仅保留成功发起人的提议');
    await change(winner, 'ledgerWithdraw', { proposalId: s.ledgerPending.id });

    console.log('—— 原子写入失败 / 无幽灵状态 / 可重试 ——');
    await failedWrite(p1, 'repay', { from: 'p2', to: 'p1', amountCents: partial }, '还款提议');
    await change(p1, 'repay', { from: 'p2', to: 'p1', amountCents: partial });
    proposal = s.ledgerPending;
    await failedWrite(p2, 'ledgerRespond', { proposalId: proposal.id, approve: true }, '还款确认');
    await approve(p2, proposal.id);
    const repayment = s.repayments[0];
    ok(s.ledgerPending === null && s.repayments.length === 1 && repayment.from === 'p2' && repayment.to === 'p1' && repayment.amountCents === partial && typeof repayment.id === 'string' && repayment.id.length > 0 && Number.isSafeInteger(repayment.ts) && repayment.ts > 0, '恢复写入后可重试，生成完整还款记录');
    ok(equal(s.history, beforeRepay.history), '还款不改变消费及手气统计原始数据');
    ok(s.balance.p1 === debt - partial && s.balance.p2 === -(debt - partial), '还款只减少净欠款');
    await rejected(p2, 'ledgerRespond', { proposalId: proposal.id, approve: true }, '重复还款批准不产生第二条还款');
    await change(p2, 'repay', { from: 'p2', to: 'p1', amountCents: s.balance.p1 });
    const fullRepayId = s.ledgerPending.id;
    await rejected(p2, 'ledgerRespond', { proposalId: proposal.id, approve: true }, '上一笔迟到批准不影响新的还款');
    await approve(p1, fullRepayId);
    ok(s.balance.p1 === 0 && s.balance.p2 === 0 && s.repayments.length === 2, '全额还款归零');
    await rejected(p2, 'repay', { from: 'p2', to: 'p1', amountCents: 1 }, '结清后不能继续还款');

    console.log('—— 双人删除还款 / 反向净欠款 ——');
    await rejected(p1, 'deleteRepayment', { id: 'missing' }, '不存在的还款不能删除');
    const settled = records();
    await change(p2, 'deleteRepayment', { id: repayment.id });
    proposal = s.ledgerPending;
    ok(proposal.type === 'deleteRepayment' && proposal.repaymentId === repayment.id && proposal.by === 'p2', '删除还款也先产生提议');
    unchanged(settled, '删除还款提议不改账');
    await rejected(p2, 'ledgerRespond', { proposalId: proposal.id, approve: true }, '不能自己批准删除还款');
    await change(p1, 'ledgerRespond', { proposalId: proposal.id, approve: false });
    unchanged(settled, '拒绝删除还款保持记录');
    await change(p2, 'deleteRepayment', { id: repayment.id });
    await approve(p1, s.ledgerPending.id);
    ok(s.repayments.length === 1 && !s.repayments.some((item) => item.id === repayment.id) && s.balance.p1 === partial && equal(s.history, settled.history), '双人同意后删除还款并恢复欠款，不改消费');
    await change(p1, 'start', { amountCents: 100000, payer: 'p2', note: '反向垫付' });
    for (const phase of ['rolling', 'result']) {
      // 使用真实消费/还款 ID 和合法还款金额，避免因参数无效而假阳性。
      for (const [type, fields] of [
        ['deleteHistory', { id: s.history[0].id }], ['clearHistory', {}],
        ['deleteRepayment', { id: s.repayments[0].id }], ['repay', { from: 'p2', to: 'p1', amountCents: 1 }],
      ]) {
        await rejected(p2, type, fields, `${phase} 阶段拒绝合法参数的 ${type} 提议`);
      }
      if (phase === 'rolling') {
        await change(p1, 'roll');
        await change(p2, 'roll');
      }
    }
    await change(p1, 'confirm');
    await change(p2, 'confirm');
    must(s.balance.p1 < -100, '反向垫付应使 P1 成为欠款方');
    const reverseBefore = records();
    await change(p2, 'repay', { from: 'p1', to: 'p2', amountCents: 100 });
    ok(s.ledgerPending.from === 'p1' && s.ledgerPending.to === 'p2', '反向净额允许明确指定 P1 付款给 P2');
    await approve(p1, s.ledgerPending.id);
    ok(s.balance.p1 === reverseBefore.balance.p1 + 100 && equal(s.history, reverseBefore.history), '反向还款正确抵扣且不改变消费/手气');

    console.log('—— SIGKILL 后重启恢复 ——');
    await change(p1, 'deleteHistory', { id: s.history[0].id });
    const durable = structuredClone(s);
    const durableBytes = readFileSync(dataFile, 'utf8');
    ok(!hasRuntimeFields(JSON.parse(durableBytes)), '在线连接存在时持久化也不包含运行时字段');
    const stopped = await server.stop('SIGKILL');
    ok(stopped.signal === 'SIGKILL', '仅 SIGKILL 自己 spawn 的实例并等待退出');
    ok(readFileSync(dataFile, 'utf8') === durableBytes, '关闭 SSE 和杀进程不改账本');
    server = new TestServer(dataFile, { PASSCODE: PASS });
    instances.push(server);
    await server.start();
    ok((await req('GET', `/api/ping?code=${p1.code}&token=${p2.token}`)).status === 200, '重启恢复换机后的身份');
    feed = await server.observe(p1.code, p2.token, PASS);
    s = await feed.wait((snap) => snap.players.p2.online, '重启 SSE');
    contracts(s);
    ok(equal(s.history, durable.history) && equal(s.repayments, durable.repayments) && equal(s.ledgerPending, durable.ledgerPending) && equal(s.balance, durable.balance), '重启恢复消费、还款、未批准提议及余额');
    await rejected(p2, 'start', { amountCents: 100 }, '重启后未批准提议继续冻结开账');
    await change(p2, 'ledgerRespond', { proposalId: s.ledgerPending.id, approve: false });
    ok(equal(s.history, durable.history) && equal(s.repayments, durable.repayments), '重启不会自动执行未批准提议');

    console.log('—— 双人清空消费与还款 ——');
    const beforeClear = records();
    await change(p1, 'clearHistory');
    proposal = s.ledgerPending;
    ok(proposal.type === 'clearHistory' && proposal.by === 'p1', '清空也必须先提议');
    unchanged(beforeClear, '清空提议不立即删除任何记录');
    await change(p2, 'ledgerRespond', { proposalId: proposal.id, approve: false });
    unchanged(beforeClear, '拒绝清空不改账');
    await change(p2, 'clearHistory');
    await change(p2, 'ledgerWithdraw', { proposalId: s.ledgerPending.id });
    unchanged(beforeClear, '撤回清空不改账');
    await change(p1, 'clearHistory');
    const clearId = s.ledgerPending.id;
    await rejected(p1, 'ledgerRespond', { proposalId: clearId, approve: true }, '不能单方批准清空');
    await rejected(p2, 'ledgerRespond', { proposalId: proposal.id, approve: true }, '旧清空批准不能批准新提议');
    await failedWrite(p2, 'ledgerRespond', { proposalId: clearId, approve: true }, '清空确认');
    await approve(p2, clearId);
    ok(s.history.length === 0 && s.repayments.length === 0 && s.ledgerPending === null && s.balance.p1 === 0 && s.balance.p2 === 0, '确认清空同时删除消费和还款，余额归零');
    ok(room().history.length === 0 && room().repayments.length === 0 && room().ledgerPending === null, '清空响应后文件立即更新');
  } catch (error) {
    ok(false, error.stack || String(error));
  } finally {
    let allClosed = true;
    for (const instance of instances) {
      try { await instance.stop(); }
      catch (error) { ok(false, `清理子进程: ${error.stack || error}`); }
      if (!instance.exit) allClosed = false;
    }
    if (temp && allClosed) {
      try { await rm(temp, { recursive: true, force: true }); }
      catch (error) { ok(false, `清理自己的临时目录: ${error.message}`); }
    }
  }
  console.log(`\n结果: ${pass} 通过, ${fail} 失败`);
  if (fail) process.exitCode = 1;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await runFlow();
