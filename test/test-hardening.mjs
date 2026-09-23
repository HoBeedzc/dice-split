// 加固测试：口令 / 真实 429 / 容量 / 原子失败回滚 / 旧数据升级与损坏保护。
// 用法：node test/test-hardening.mjs。所有实例使用随机端口及自己的临时目录。
import { mkdtemp, rm } from 'node:fs/promises';
import { existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { isDeepStrictEqual as equal } from 'node:util';
import { TestServer, deadline, persistentView, expectedBalance, hasRuntimeFields } from './test-flow.mjs';

const PASS = 'test-暗号123';
let pass = 0, fail = 0;
const ok = (condition, name) => {
  if (condition) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.error(`  FAIL ${name}`); }
};
const must = (condition, message) => { if (!condition) throw new Error(message); };
const instances = [];
let temp, server, feed, p1, p2, s;
const launch = async (file, env = {}) => {
  const instance = new TestServer(file, env);
  instances.push(instance); // 启动失败也必须在 finally 中等待退出。
  await instance.start();
  return instance;
};
const req = (method, url, body, usePass = true) => server.request(method, url,
  usePass && body !== undefined ? { passcode: PASS, ...body } : body);
const saved = () => JSON.parse(readFileSync(server.dataFile, 'utf8'));
const actionBody = (player, type, fields = {}) => ({ code: p1.code, token: player.token, type, ...fields });
const act = (player, type, fields = {}) => req('POST', '/api/action', actionBody(player, type, fields));
const change = async (player, type, fields = {}) => {
  const beforeVersion = saved()[p1.code].version;
  const index = feed.frames.length;
  const result = await act(player, type, fields);
  must(result.status === 200, `${type}: ${result.status} ${JSON.stringify(result.data)}`);
  const room = saved()[p1.code]; // 响应后立即读文件，不等广播或防抖。
  must(room.version > beforeVersion, `${type} 响应成功但未立即写盘`);
  s = await feed.wait((snap) => equal(persistentView(snap, true), persistentView(room)), `${type} 的已持久化快照`, index);
  must(equal(s.balance, expectedBalance(s)), `${type} 后余额错误`);
  return result;
};
const approve = (player, proposalId) => change(player, 'ledgerRespond', { proposalId, approve: true });
const rejected = async (player, type, fields, label) => {
  const before = readFileSync(server.dataFile, 'utf8');
  const index = feed.frames.length;
  const result = await act(player, type, fields);
  ok(result.status === 400, label);
  await feed.noEventsSince(index);
  ok(readFileSync(server.dataFile, 'utf8') === before, `${label}：不修改已持久化状态且不广播`);
  return result;
};
const addBill = async () => {
  await change(p1, 'start', { amountCents: 10000, payer: 'p1' });
  await change(p1, 'roll');
  await change(p2, 'roll');
  await change(p1, 'confirm');
  await change(p2, 'confirm');
};
const failMutation = async (url, body, label, watched = feed, identity = p1) => {
  const originalBytes = readFileSync(server.dataFile, 'utf8');
  const originalState = persistentView(watched.latest, true);
  const index = watched.frames.length;
  const blocker = `${server.dataFile}.tmp-${server.child.pid}`;
  mkdirSync(blocker);
  try {
    const result = await req('POST', url, body);
    ok(result.status >= 400, `${label}：临时写入失败返回非 2xx`);
    ok(readFileSync(server.dataFile, 'utf8') === originalBytes, `${label}：旧文件逐字节不变`);
    await watched.noEventsSince(index);
    ok(true, `${label}：失败不广播成功快照`);
    const refresh = await server.observe(identity.code, identity.token, PASS);
    try {
      ok(equal(persistentView(refresh.latest, true), originalState), `${label}：新 SSE 无幽灵内存记录`);
    } finally { await refresh.close(); }
  } finally {
    rmSync(blocker, { recursive: true, force: true });
  }
};

try {
  temp = await mkdtemp(path.join(tmpdir(), 'dice-split-hardening-'));
  server = await launch(path.join(temp, 'hardening.json'), {
    PASSCODE: PASS, MAX_ROOMS: '3', MAX_HISTORY: '2', RATE_LIMIT: '100000',
  });

  console.log('—— 口令闸门 ——');
  let r = await req('POST', '/api/create', { name: 'A' }, false);
  ok(r.status === 401 && r.data.error === 'need-passcode', '无口令建房被拒');
  r = await req('POST', '/api/create', { name: 'A', passcode: 'wrong' });
  ok(r.status === 401, '错误口令被拒');
  r = await req('POST', '/api/create', { name: '大熊' });
  must(r.status === 200 && r.data?.token, '正确口令建房失败');
  p1 = r.data;
  ok(saved()[p1.code].players.p1.token === p1.token, '正确口令建房且响应前已持久化');
  const pingUrl = `/api/ping?code=${p1.code}&token=${p1.token}`;
  r = await req('GET', pingUrl);
  ok(r.status === 401 && r.data.error === 'need-passcode', 'ping 无口令被拒');
  ok((await req('GET', `${pingUrl}&passcode=${encodeURIComponent(PASS)}`)).status === 200, 'ping 带口令通过');
  r = await req('POST', '/api/join', { code: p1.code, name: '小猫' }, false);
  ok(r.status === 401, '无口令加入被拒');
  r = await req('POST', '/api/join', { code: p1.code, name: '小猫' });
  must(r.status === 200 && r.data?.role === 'p2', '带口令加入失败');
  p2 = r.data;
  ok(saved()[p1.code].players.p2.token === p2.token, '加入响应前 token 已落盘');
  r = await req('POST', '/api/action', { code: p1.code, token: p1.token }, false);
  ok(r.status === 401, '无口令动作被拒');
  r = await req('POST', '/api/join', { code: p1.code, claimRole: 'p1', passcode: 'wrong' });
  ok(r.status === 401, '错误口令认领被拒');
  for (const [url, fields] of [
    ['/api/recover', { recoveryCode: p1.recoveryCode }],
    ['/api/recovery', { token: p1.token }],
  ]) {
    for (const passcode of [undefined, 'wrong']) {
      const before = readFileSync(server.dataFile, 'utf8');
      r = await req('POST', url, { code: p1.code, ...fields, passcode }, false);
      ok(r.status === 401 && r.data.error === 'need-passcode', `${url} 缺少或错误口令被拒`);
      ok(readFileSync(server.dataFile, 'utf8') === before, `${url} 门禁拒绝不修改身份`);
    }
  }

  console.log('—— SSE 口令 / 网页门禁 ——');
  r = await req('GET', `/events?code=${p1.code}&token=${p1.token}`);
  ok(r.status === 401, 'SSE 无口令被拒');
  feed = await server.observe(p1.code, p1.token, PASS);
  s = await feed.wait((snap) => snap.players.p1.online, '带口令 SSE 上线');
  ok(s.code === p1.code, 'SSE 带口令连上');
  ok(s.temporaryMode === false, '有口令快照明确为持久化模式');
  const home = await req('GET', '/');
  ok(home.status === 200 && home.text.includes('/api/unlock') && !home.text.includes('id="app"'), '未解锁只能看到口令页');
  ok((await req('POST', '/api/unlock', { passcode: 'wrong' })).status === 401, '错误口令不能解锁');
  const unlock = await req('POST', '/api/unlock', { passcode: PASS });
  const cookie = (unlock.headers.get('set-cookie') || '').split(';')[0];
  ok(unlock.status === 200 && cookie.startsWith('ds_unlock='), '正确口令下发解锁 Cookie');
  const unlockedHome = await server.request('GET', '/', undefined, { Cookie: cookie });
  ok(unlockedHome.status === 200 && unlockedHome.text.includes('id="app"'), '解锁后能获取应用页面');
  ok(/<meta\b(?=[^>]*\bname=["']dice-split-temporary["'])(?=[^>]*\bcontent=["']false["'])[^>]*>/i.test(unlockedHome.text), '有口令首页模式标记为 false');
  ok((await server.request('GET', pingUrl, undefined, { Cookie: cookie })).status === 200, 'Cookie 可替代接口口令');

  console.log('—— 建房 / 加入写入失败与容量回滚 ——');
  await failMutation('/api/create', { name: '不应存在的房间' }, '建房失败');
  ok(Object.keys(saved()).length === 1, '失败建房没有幽灵磁盘房间');
  r = await req('POST', '/api/create', { name: 'B' });
  must(r.status === 200, '恢复写入后应可创建 B');
  const b = r.data;
  ok(saved()[b.code].players.p1.token === b.token, '恢复写入后创建 B 立即持久化');
  const bFeed = await server.observe(b.code, b.token, PASS);
  await bFeed.wait((snap) => snap.players.p1.online, 'B 上线');
  await failMutation('/api/join', { code: b.code, name: 'B 的伙伴' }, '加入失败', bFeed, b);
  ok(!saved()[b.code].players.p2 && Object.values(saved()[b.code].players).filter(Boolean).length === 1 && saved()[b.code].phase === 'lobby', '失败加入不占身份或修改房间阶段');
  r = await req('POST', '/api/join', { code: b.code, name: 'B 的伙伴' });
  ok(r.status === 200 && saved()[b.code].players.p2.token === r.data.token, '失败加入恢复后可以重试');
  await bFeed.close();
  r = await req('POST', '/api/create', { name: 'C' });
  ok(r.status === 200 && Object.keys(saved()).length === 3, '失败建房未占容量，第 3 个房间正常创建');
  r = await req('POST', '/api/create', { name: 'D' });
  ok(r.status === 403 && Object.keys(saved()).length === 3, '第 4 个房间被拒（上限 3）');

  console.log('—— 改名 / 开账 / 确认失败不改变内存 ——');
  await failMutation('/api/recover', { code: p1.code, recoveryCode: p1.recoveryCode }, '恢复身份失败');
  await failMutation('/api/recovery', { code: p1.code, token: p1.token }, '重置恢复码失败');
  ok((await req('GET', `${pingUrl}&passcode=${encodeURIComponent(PASS)}`)).status === 200, '身份写入失败保留旧 token 及活跃连接');
  await failMutation('/api/action', actionBody(p1, 'setMe', { name: '失败改名' }), '改名失败');
  await change(p1, 'setMe', { name: '重试改名' });
  ok(s.players.p1.name === '重试改名', '恢复写入后改名可重试');
  await failMutation('/api/action', actionBody(p1, 'start', { amountCents: 10000, payer: 'p1' }), '开账失败');
  await change(p1, 'start', { amountCents: 10000, payer: 'p1' });
  await change(p1, 'roll');
  await change(p2, 'roll');
  await change(p1, 'confirm');
  await failMutation('/api/action', actionBody(p2, 'confirm'), '最终确认失败');
  ok(saved()[p1.code].history.length === 0 && !saved()[p1.code].bill.confirms.p2, '失败确认不产生消费或残留确认标记');
  await change(p2, 'confirm');
  ok(s.history.length === 1, '恢复写入后同一确认可重试且只入账一次');

  console.log('—— 消费容量上限 / 双人删除后恢复 ——');
  await addBill();
  ok(s.history.length === 2 && s.phase === 'idle', '第 2 笔完整入账（上限 2）');
  r = await rejected(p1, 'start', { amountCents: 100, payer: 'p1' }, '第 3 笔开账被拦');
  ok(String(r.data.error).includes('账本已满'), '开账超限明确报告账本已满');
  await rejected(p1, 'deleteHistory', { id: 'none' }, '超限后仍可响应动作');
  const historyId = s.history[0].id;
  await change(p1, 'deleteHistory', { id: historyId });
  ok(s.history.length === 2 && s.ledgerPending.historyId === historyId, '容量满时仍能提议删除，但不直接删除');
  await approve(p2, s.ledgerPending.id);
  ok(s.history.length === 1, '双人确认删除释放消费容量');
  await addBill();
  ok(s.history.length === 2 && s.phase === 'idle', '删除后可重新开账入账，状态机不残留');

  console.log('—— 还款容量上限 / 删除还款 / 清空 ——');
  const consumption = structuredClone(s.history);
  for (let i = 0; i < 2; i++) {
    await change(i === 0 ? p1 : p2, 'repay', { from: 'p2', to: 'p1', amountCents: 1 });
    const pending = s.ledgerPending;
    ok(s.repayments.length === i && pending.from === 'p2' && pending.to === 'p1', `第 ${i + 1} 笔还款提议不占已确认记录`);
    await approve(i === 0 ? p2 : p1, pending.id);
    ok(s.repayments.length === i + 1, `第 ${i + 1} 笔还款确认后入账`);
  }
  ok(equal(s.history, consumption), '消费满额不阻止还款，且还款不占用或改变消费/手气记录');
  await rejected(p1, 'repay', { from: 'p2', to: 'p1', amountCents: 1 }, '第 3 笔还款提议被拒（还款上限 2）');
  ok(saved()[p1.code].ledgerPending === null && saved()[p1.code].repayments.length === 2, '还款超限不残留提议或额外记录');
  const repaymentId = s.repayments[0].id;
  await change(p2, 'deleteRepayment', { id: repaymentId });
  ok(s.repayments.length === 2 && s.ledgerPending.repaymentId === repaymentId, '还款满额时仍可提议删除');
  await approve(p1, s.ledgerPending.id);
  ok(s.repayments.length === 1 && !s.repayments.some((item) => item.id === repaymentId), '双人确认释放还款容量');
  await change(p1, 'repay', { from: 'p2', to: 'p1', amountCents: 1 });
  await approve(p2, s.ledgerPending.id);
  ok(s.repayments.length === 2 && equal(s.history, consumption), '删除后可以再次还款且不改消费');
  await change(p1, 'clearHistory');
  ok(s.history.length === 2 && s.repayments.length === 2, '清空待确认时两类记录都保持');
  await approve(p2, s.ledgerPending.id);
  ok(s.history.length === 0 && s.repayments.length === 0 && s.balance.p1 === 0, '清空确认同时释放消费和还款容量');
  await addBill();
  ok(s.history.length === 1 && s.phase === 'idle', '清空后可继续记账');
  ok(!hasRuntimeFields(saved()), '多个 SSE 连接存在时运行时字段仍不落盘');

  console.log('—— 分账提议原子失败 / 重启身份 / 无 ID 旧提议升级 ——');
  {
    const file = server.dataFile;
    const reload = async () => {
      server = await launch(file, { PASSCODE: PASS, MAX_ROOMS: '3', MAX_HISTORY: '2' });
      feed = await server.observe(p1.code, p1.token, PASS);
      s = await feed.wait((snap) => snap.players.p1.online, '分账提议重启上线');
    };
    for (const type of ['reroll', 'void']) {
      await change(p1, 'start', { amountCents: 10000, payer: 'p1' });
      await change(p1, 'roll');
      await change(p2, 'roll');
      await failMutation('/api/action', actionBody(p1, type), `${type} 新提议写入失败`);
      await change(p1, type);
      const proposalId = s.bill.pending.id;
      ok(typeof proposalId === 'string' && proposalId.length > 0, `${type} 新 ID 响应前已落盘并广播`);
      for (const [player, action, fields] of [
        [p2, 'respond', { proposalId, approve: true }],
        [p1, 'withdraw', { proposalId }],
        [p2, type, { proposalId }],
      ]) {
        await failMutation('/api/action', actionBody(player, action, fields), `${type} 的 ${action} 写入失败`);
      }
      const durable = structuredClone(s);
      const bytes = readFileSync(file, 'utf8');
      await server.stop('SIGKILL');
      await reload();
      ok(readFileSync(file, 'utf8') === bytes && equal(persistentView(s, true), persistentView(durable, true)) && s.bill.pending.id === proposalId, `${type} 重启保持原 ID、提议和账本，不自动执行`);
      await change(p1, 'withdraw', { proposalId });
      ok(s.bill.pending === null && s.phase === 'result', `${type} 重启后原 ID 可撤回`);

      // 仅改自己已停机的临时账本，模拟旧版本尚未生成 ID 的提议。
      await change(p1, type);
      const legacyState = structuredClone(s);
      await server.stop('SIGKILL');
      const legacyData = saved();
      delete legacyData[p1.code].bill.pending.id;
      delete legacyData[p1.code].bill.pending.required;
      delete legacyData[p1.code].bill.pending.approvals;
      delete legacyData[p1.code].bill.participants;
      delete legacyData[p1.code].bill.required;
      writeFileSync(file, JSON.stringify(legacyData));
      await reload();
      const migratedId = s.bill.pending.id;
      ok(typeof migratedId === 'string' && migratedId.length > 0 && migratedId !== legacyState.bill.pending.id, `${type} 旧提议加载即补充新 ID`);
      legacyState.bill.pending.id = migratedId;
      ok(equal(persistentView(s, true), persistentView(legacyState, true)), `${type} 旧提议升级不改变其他分账和账本状态`);
      await rejected(p2, 'respond', { approve: true }, `${type} 升级后回应仍不能省略 ID`);
      await rejected(p1, 'withdraw', {}, `${type} 升级后撤回仍不能省略 ID`);
      await change(p1, 'setMe', { name: s.players.p1.name });
      ok(saved()[p1.code].bill.pending.id === migratedId, `${type} 升级 ID 随首次成功修改落盘且不重新生成`);
      await change(p2, 'respond', { proposalId: migratedId, approve: true });
      ok(s.phase === (type === 'reroll' ? 'rolling' : 'idle') && (type === 'reroll' ? s.bill.rerolls === 1 && s.bill.pending === null : s.bill === null), `${type} 升级提议可用新 ID 同意`);
      if (type === 'reroll') {
        await change(p1, 'void');
        await change(p2, 'respond', { proposalId: s.bill.pending.id, approve: true });
      }
      ok(equal(s.history, durable.history) && equal(s.repayments, durable.repayments) && equal(s.balance, durable.balance), `${type} 重启、升级和作废不改已确认账本`);
    }
  }

  console.log('—— 隔离低限额实例：必须真实返回 429 ——');
  const limited = await launch(path.join(temp, 'rate-limit.json'), { RATE_LIMIT: '5', PASSCODE: '' });
  // 此实例没有 readiness HTTP 探测，也没有 SSE 请求；明确耗尽同一 IP 的五次额度。
  const allowedStatuses = [];
  for (let i = 0; i < 5; i++) {
    allowedStatuses.push((await limited.request('GET', '/api/ping?code=missing&token=missing')).status);
  }
  ok(allowedStatuses.every((status) => status === 401), '限额内五次请求均到达身份校验');
  const sixth = await limited.request('GET', '/api/ping?code=missing&token=missing');
  ok(sixth.status === 429, '同一 IP 第六次请求必须返回 429');
  ok((await limited.request('GET', '/api/ping?code=missing&token=missing')).status === 429, '耗尽额度后继续请求仍为 429，不能以非 500 代替');
  await limited.stop();

  console.log('—— 旧数据默认字段升级 ——');
  const legacyRoom = structuredClone(saved()[p1.code]);
  delete legacyRoom.repayments;
  delete legacyRoom.ledgerPending;
  delete legacyRoom.faces;
  delete legacyRoom.capacity;
  delete legacyRoom.repaymentCarry;
  for (const player of Object.values(legacyRoom.players)) delete player.recoveryHash;
  for (const bill of legacyRoom.history) {
    delete bill.participants;
    delete bill.required;
  }
  legacyRoom.players.p1.online = true;
  legacyRoom.players.p1._conns = 99;
  legacyRoom.players.p2.online = true;
  legacyRoom.players.p2._conns = 99;
  const legacyFile = path.join(temp, 'legacy.json');
  writeFileSync(legacyFile, JSON.stringify({ [p1.code]: legacyRoom }));
  const legacy = await launch(legacyFile, { PASSCODE: PASS });
  const legacyFeed = await legacy.observe(p1.code, p1.token, PASS);
  const initialLegacy = legacyFeed.frames[0];
  ok(initialLegacy.faces === 6 && Array.isArray(initialLegacy.repayments) && initialLegacy.repayments.length === 0 && initialLegacy.ledgerPending === null, '加载老数据补六面骰、repayments=[]、ledgerPending=null');
  const normalizedHistory = legacyRoom.history.map((bill) => ({ ...bill, participants: ['p1', 'p2'] }));
  ok(initialLegacy.capacity === 2 && equal(initialLegacy.history, normalizedHistory) && equal(initialLegacy.balance, expectedBalance(initialLegacy)), '升级补充双人成员列表，保留老消费与净余额');
  ok(!initialLegacy.players.p1.online && !initialLegacy.players.p2.online, '加载时不恢复旧在线连接');
  const legacyRecovery = await legacy.request('POST', '/api/recovery', { passcode: PASS, code: p1.code, token: p1.token });
  must(legacyRecovery.status === 200 && typeof legacyRecovery.data.recoveryCode === 'string' && legacyRecovery.data.recoveryCode.length > 0, '无 recoveryHash 的旧身份可认证生成恢复码');
  const recoveredLegacyDisk = JSON.parse(readFileSync(legacyFile, 'utf8'))[p1.code];
  ok(recoveredLegacyDisk.players.p1.token === p1.token && recoveredLegacyDisk.players.p2.token === p2.token && typeof recoveredLegacyDisk.players.p1.recoveryHash === 'string' && !readFileSync(legacyFile, 'utf8').includes(legacyRecovery.data.recoveryCode), '旧 token 保留且新增恢复码只存 hash');
  r = await legacy.request('POST', '/api/action', { passcode: PASS, code: p1.code, token: p1.token, type: 'setMe', name: '升级后改名' });
  const upgraded = JSON.parse(readFileSync(legacyFile, 'utf8'))[p1.code];
  ok(r.status === 200 && upgraded.players.p1.name === '升级后改名' && upgraded.faces === 6 && equal(upgraded.repayments, []) && upgraded.ledgerPending === null, '升级后的首次修改响应前即写入新结构');
  ok(!hasRuntimeFields(upgraded), '旧运行时字段升级后不再落盘');
  await legacy.stop();

  console.log('—— 损坏 JSON 不得忽略或覆盖 ——');
  const corruptFile = path.join(temp, 'corrupt.json');
  const corruptBytes = '{"broken": [\n';
  writeFileSync(corruptFile, corruptBytes);
  const corrupt = new TestServer(corruptFile, { PASSCODE: PASS });
  corrupt.expectedExit = true; // 唯一允许自发退出的实例；下方要求明确的非零退出码。
  instances.push(corrupt);
  try {
    const exit = await deadline(corrupt.closed, '损坏文件必须导致启动失败', 10000);
    ok(Number.isInteger(exit.code) && exit.code !== 0 && exit.signal === null, '损坏 JSON 导致非零退出，不是继续运行');
    ok(corrupt.base === null, '损坏数据不能继续开始监听');
  } finally {
    await corrupt.stop();
    ok(readFileSync(corruptFile, 'utf8') === corruptBytes, '损坏文件逐字节保留，没有被空账本覆盖');
  }

  console.log('—— 临时模式：真实双人分账 / 有符号还款汇总 ——');
  {
    // 独立实例和快照，不复用上面的持久化 action/change/saved helpers。
    const temporaryFile = path.join(temp, 'missing-parent', 'temporary.json');
    must(!existsSync(path.dirname(temporaryFile)), '测试数据文件父路径必须不存在');
    const temporary = await launch(temporaryFile, { MAX_HISTORY: '1', MAX_ROOMS: '2' });
    const temporaryHome = await temporary.request('GET', '/');
    ok(temporaryHome.status === 200 && temporaryHome.text.includes('id="app"'), '无口令直接打开应用');
    ok(/<meta\b(?=[^>]*\bname=["']dice-split-temporary["'])(?=[^>]*\bcontent=["']true["'])[^>]*>/i.test(temporaryHome.text), '无口令首页模式标记为 true');
    ok(temporary.logs.includes('未设置密码') && temporary.logs.includes('临时模式'), '启动日志中文说明未设置密码及临时模式');
    const created = await temporary.request('POST', '/api/create', { name: '临时甲' });
    must(created.status === 200 && created.data?.token, '父路径不存在时临时建房失败');
    const guest1 = created.data;
    const temporaryFeed = await temporary.observe(guest1.code, guest1.token);
    const joined = await temporary.request('POST', '/api/join', { code: guest1.code, name: '临时乙' });
    must(joined.status === 200 && joined.data?.role === 'p2', '临时双人加入失败');
    const guest2 = joined.data;
    const peerFeed = await temporary.observe(guest1.code, guest2.token);
    const duplicateFeed = await temporary.observe(guest1.code, guest1.token);
    let current = await temporaryFeed.wait((snap) => snap.players.p1.online && snap.players.p2?.online, '临时双人在线');
    const carry = () => Object.fromEntries(['p1', 'p2'].map((id) => [id, current.repaymentCarry[id] ?? 0]));
    ok(current.temporaryMode === true && equal(carry(), { p1: 0, p2: 0 }), '临时快照声明模式及初始零汇总');

    const reserved = await temporary.request('POST', '/api/create', { name: '从未连接 SSE' });
    must(reserved.status === 200 && reserved.data?.token, '创建预留房间失败');
    const reservation = reserved.data;
    ok((await temporary.request('GET', `/api/ping?code=${reservation.code}&token=${reservation.token}`)).status === 200, '建房后尚未连接 SSE 的预留身份立即可用');
    ok((await temporary.request('POST', '/api/create', { name: '超容量' })).status === 403, '在线房间和预留房间均占容量');

    const ledger = () => structuredClone({
      history: current.history, repayments: current.repayments,
      repaymentCarry: carry(), balance: current.balance,
    });
    const update = async (player, type, fields = {}) => {
      const index = temporaryFeed.frames.length;
      const version = current.version;
      const result = await temporary.request('POST', '/api/action', { code: guest1.code, token: player.token, type, ...fields });
      must(result.status === 200, `临时 ${type}: ${result.status} ${JSON.stringify(result.data)}`);
      current = await temporaryFeed.wait((snap) => snap.version > version, `临时 ${type} 广播`, index);
      must(current.temporaryMode === true && current.repaymentCarry && !Array.isArray(current.repaymentCarry) && Object.keys(current.repaymentCarry).every((id) => ['p1', 'p2'].includes(id)) && Object.values(current.repaymentCarry).every(Number.isSafeInteger) && carry().p1 + carry().p2 === 0 && !('repaymentCarryCents' in current), '临时模式及每人零和整数汇总必须包含在每次快照中');
      must(current.history.length <= 1 && current.repayments.length <= 1, '临时明细不能超过一条');
      must(equal(current.balance, expectedBalance(current)), `临时 ${type} 后消费、还款、汇总与余额不符`);
      return result.data;
    };
    const accept = () => update(guest2, 'ledgerRespond', { proposalId: current.ledgerPending.id, approve: true });
    const completeBill = async (amountCents, payer, note) => {
      const before = ledger();
      await update(guest1, 'start', { amountCents, payer, note });
      ok(equal(ledger(), before), `${note}：开账尚未确认不裁剪旧账或还款`);
      const roll1 = await update(guest1, 'roll');
      const roll2 = await update(guest2, 'roll');
      must(current.phase === 'result' && current.bill.rolls.p1 === roll1.roll && current.bill.rolls.p2 === roll2.roll, '必须完成真实双人掷骰');
      const shares = {
        p1: Math.round(amountCents * roll1.roll / (roll1.roll + roll2.roll)),
      };
      shares.p2 = amountCents - shares.p1;
      await update(guest1, 'confirm');
      ok(current.phase === 'result' && equal(ledger(), before), `${note}：单方确认不替换历史`);
      await update(guest2, 'confirm');
      const latest = current.history[0];
      ok(current.phase === 'idle' && current.bill === null && current.history.length === 1 && latest?.id !== before.history[0]?.id && latest?.amountCents === amountCents && latest?.payer === payer && latest?.note === note && equal(latest?.shares, shares), `${note}：MAX_HISTORY=1 仍可双人入账且仅保留最新消费`);
      ok(current.repayments.length === 0 && carry().p1 === 0 && current.balance.p1 === (payer === 'p1' ? shares.p2 : -shares.p1), `${note}：新确认清理所有旧还款及汇总，余额只来自新账`);
      return latest;
    };
    const repay = async (amountCents) => {
      const before = ledger();
      await update(guest1, 'repay', { from: before.balance.p1 < 0 ? 'p1' : 'p2', to: before.balance.p1 < 0 ? 'p2' : 'p1', amountCents });
      const pending = current.ledgerPending;
      ok(equal(ledger(), before), '还款提议不提前裁剪已有明细或累计汇总');
      await accept();
      const sign = before.balance.p1 > 0 ? -1 : 1;
      const previousSigned = before.repayments.reduce((sum, item) => sum + (item.from === 'p1' ? item.amountCents : -item.amountCents), 0);
      ok(current.repayments.length === 1 && current.repayments[0].id === pending.id && current.repayments[0].amountCents === amountCents && current.repayments[0].from === (sign === 1 ? 'p1' : 'p2') && current.repayments[0].to === (sign === 1 ? 'p2' : 'p1'), '确认还款只留最新一条，付款方向正确');
      ok(carry().p1 === before.repaymentCarry.p1 + previousSigned && current.balance.p1 === before.balance.p1 + sign * amountCents && equal(current.history, before.history), '旧还款仅转有符号汇总，余额累计且不改变消费');
    };
    const isEmpty = (label) => ok(current.history.length === 0 && current.repayments.length === 0 && carry().p1 === 0 && equal(current.balance, { p1: 0, p2: 0 }), label);

    const firstBill = await completeBill(12000, 'p1', '临时第一笔');
    for (const amount of [11, 17, 23]) await repay(amount);
    ok(carry().p1 === -28 && current.balance.p1 === firstBill.shares.p2 - 51, '三次正向还款余额包含全部 51 分，汇总为 -28 分');
    const latestRepayment = current.repayments[0];
    await update(guest1, 'deleteRepayment', { id: latestRepayment.id });
    await accept();
    ok(current.repayments.length === 0 && carry().p1 === -28 && current.balance.p1 === firstBill.shares.p2 - 28, '删除最新还款仅撤销最近 23 分，汇总保持');
    await repay(7);
    ok(carry().p1 === -28 && current.balance.p1 === firstBill.shares.p2 - 35, '删掉最新还款后继续还款不丢失旧汇总');
    const confirmed = ledger();
    for (const [type, fields] of [
      ['repay', { from: 'p2', to: 'p1', amountCents: 9 }], ['clearHistory', {}],
      ['deleteHistory', { id: firstBill.id }], ['deleteRepayment', { id: current.repayments[0].id }],
    ]) {
      await update(guest1, type, fields);
      await update(guest2, 'ledgerRespond', { proposalId: current.ledgerPending.id, approve: false });
      ok(equal(ledger(), confirmed), `拒绝 ${type} 不裁剪已确认消费、还款及汇总`);
    }
    await update(guest1, 'start', { amountCents: 14000, payer: 'p2' });
    await update(guest1, 'roll');
    await update(guest1, 'void');
    await update(guest2, 'respond', { proposalId: current.bill.pending.id, approve: false });
    ok(current.phase === 'rolling' && equal(ledger(), confirmed), '拒绝作废不裁剪已确认账本');
    await update(guest1, 'void');
    await update(guest2, 'respond', { proposalId: current.bill.pending.id, approve: true });
    ok(current.phase === 'idle' && current.bill === null && equal(ledger(), confirmed), '同意作废未确认分账也不裁剪旧账及其还款');

    const secondBill = await completeBill(18000, 'p2', '临时第二笔');
    await repay(13);
    await repay(19);
    ok(carry().p1 === 13 && current.balance.p1 === -secondBill.shares.p1 + 32, '反向还款汇总为正数且累计抵扣欠款');
    await update(guest1, 'clearHistory');
    await accept();
    isEmpty('双人清空消费、还款和非零汇总后余额归零');
    await completeBill(6000, 'p1', '临时删除消费');
    await repay(5);
    await repay(7);
    await update(guest1, 'deleteHistory', { id: current.history[0].id });
    await accept();
    isEmpty('临时删除消费也清理最新还款及非零汇总，余额归零');
    ok(!existsSync(temporaryFile) && !existsSync(path.dirname(temporaryFile)), '完整分账和还款全程不生成数据文件或父目录');

    const inaccessible = async (identity, label) => {
      const query = new URLSearchParams({ code: identity.code, token: identity.token });
      const ping = await temporary.request('GET', `/api/ping?${query}`);
      ok(ping.status === 401 && ping.data?.error === 'stale', `${label}：旧身份 ping 失败`);
      const action = await temporary.request('POST', '/api/action', { ...identity, type: 'setMe', name: '不能复活' });
      ok(action.status === 401 && action.data?.error === 'stale', `${label}：旧身份 action 失败`);
      ok((await temporary.request('POST', '/api/join', { code: identity.code })).status === 404, `${label}：join 不能复活已销毁房间`);
      const claim = await temporary.request('POST', '/api/join', { code: identity.code, claimRole: identity.role });
      ok(claim.status >= 400 && claim.status < 500, `${label}：旧认领入口始终拒绝`);
      const events = await temporary.request('GET', `/events?${query}`);
      ok(events.status === 404 && events.data?.error === 'stale', `${label}：events 无法访问`);
    };
    console.log('—— 临时模式：真实 15 秒预留回收 / SSE 连接计数 ——');
    // 唯一的真实时钟等待：不轮询，也不缩短生产的 15 秒超时。
    await new Promise((resolve) => setTimeout(resolve, 16000));
    await inaccessible(reservation, '从未连接 SSE 的房间满 15 秒回收');
    ok((await temporary.request('GET', `/api/ping?code=${guest1.code}&token=${guest1.token}`)).status === 200, '已连接 SSE 的房间超过 15 秒仍存在');
    const capacityProbe = await temporary.request('POST', '/api/create', { name: '预留已释放容量' });
    must(capacityProbe.status === 200, '预留回收必须释放容量');
    const probeFeed = await temporary.observe(capacityProbe.data.code, capacityProbe.data.token);
    await probeFeed.close();
    await inaccessible(capacityProbe.data, '单连接房间最后断开立即回收');

    await duplicateFeed.close();
    await update(guest2, 'setMe', { name: '关闭同角色一个连接' });
    ok(current.players.p1.online && current.players.p2.online, '同角色两个 SSE 关闭一个后仍在线，不能按角色误减到零');
    await peerFeed.close();
    current = await temporaryFeed.wait((snap) => !snap.players.p2.online, '另一角色断开广播');
    ok(current.players.p1.online, '另一角色断开时只剩一个 SSE，房间仍在');
    await update(guest1, 'setMe', { name: '最后一个连接仍可操作' });
    await temporaryFeed.close();
    await inaccessible(guest1, '最后连接断开立即销毁 P1');
    await inaccessible(guest2, '最后连接断开立即销毁 P2');
    const reclaimed = await temporary.request('POST', '/api/create', { name: '断线后容量复用一' });
    const reclaimedAgain = await temporary.request('POST', '/api/create', { name: '断线后容量复用二' });
    ok(reclaimed.status === 200 && reclaimedAgain.status === 200, '最后断开释放房间容量，可以重新填满两个名额');
    await temporary.stop();
    ok(!existsSync(temporaryFile) && !existsSync(path.dirname(temporaryFile)), '临时实例关闭也不创建数据文件或父目录');
  }

  console.log('—— 临时模式：忽略受保护旧数据 / 坏 JSON / 重启不恢复 ——');
  {
    const protectedIdentity = { code: upgraded.code, token: upgraded.players.p1.token };
    for (const [label, file, originalIdentity] of [
      ['已有受保护账本', legacyFile, protectedIdentity], ['坏 JSON', corruptFile, null],
    ]) {
      const originalBytes = readFileSync(file, 'utf8');
      const originalStat = statSync(file);
      const ignored = await launch(file); // 保持 TestServer 的空口令默认值。
      if (originalIdentity) {
        const query = new URLSearchParams(originalIdentity);
        ok((await ignored.request('GET', `/api/ping?${query}`)).status === 401, '无口令实例不加载已有受保护身份');
        ok((await ignored.request('POST', '/api/join', { code: originalIdentity.code })).status === 404, '无口令实例不能加入磁盘上的受保护房间');
        ok((await ignored.request('GET', `/events?${query}`)).status === 404, '无口令 SSE 不泄露受保护账本');
      }
      const fresh = await ignored.request('POST', '/api/create', { name: '只在内存' });
      must(fresh.status === 200 && fresh.data?.token, `${label} 不应阻止无口令服务运行`);
      const volatileIdentity = fresh.data;
      const volatileFeed = await ignored.observe(volatileIdentity.code, volatileIdentity.token);
      const index = volatileFeed.frames.length;
      const mutation = await ignored.request('POST', '/api/action', { ...volatileIdentity, type: 'setMe', name: '重启不应恢复此名' });
      must(mutation.status === 200, `${label} 下内存修改失败`);
      const volatileState = await volatileFeed.wait((snap) => snap.players.p1.name === '重启不应恢复此名', '临时内存修改广播', index);
      ok(volatileState.temporaryMode === true && volatileState.history.length === 0 && volatileState.repayments.length === 0, `${label}：只加载本次内存房间，不读取旧消费/还款`);
      ok(readFileSync(file, 'utf8') === originalBytes && statSync(file).mtimeMs === originalStat.mtimeMs && statSync(file).ino === originalStat.ino, `${label}：临时修改不覆盖、重写或替换已有文件`);
      // 保持 SSE 活跃直接杀掉自己的实例，避免先断线销毁导致重启断言空洞。
      ignored.expectedExit = true;
      volatileFeed.closing = true;
      ignored.child.kill('SIGKILL');
      const exit = await deadline(ignored.closed, '临时实例带活跃 SSE 退出');
      must(exit.signal === 'SIGKILL', '临时重启测试必须实际终止自己的子进程');
      await ignored.stop();
      ok(readFileSync(file, 'utf8') === originalBytes, `${label}：临时退出也保留原文件`);
      const restarted = await launch(file);
      const staleQuery = new URLSearchParams({ code: volatileIdentity.code, token: volatileIdentity.token });
      ok((await restarted.request('GET', `/api/ping?${staleQuery}`)).status === 401, `${label}：临时服务重启不恢复活跃房间身份`);
      ok((await restarted.request('POST', '/api/join', { code: volatileIdentity.code })).status === 404, `${label}：重启后旧临时房间不可加入`);
      ok((await restarted.request('GET', `/events?${staleQuery}`)).status === 404, `${label}：重启后 SSE 不恢复旧临时快照`);
      await restarted.stop();
      ok(readFileSync(file, 'utf8') === originalBytes && statSync(file).mtimeMs === originalStat.mtimeMs, `${label}：无口令重启仍不改写数据`);
    }
    const restored = await launch(legacyFile, { PASSCODE: PASS });
    const restoredFeed = await restored.observe(protectedIdentity.code, protectedIdentity.token, PASS);
    ok(restoredFeed.latest.temporaryMode === false && equal(persistentView(restoredFeed.latest, true), persistentView(upgraded)) && equal(restoredFeed.latest.balance, expectedBalance(restoredFeed.latest)), '重新设置口令后仍完整恢复原受保护身份、消费、还款及余额');
    await restored.stop();
  }
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
