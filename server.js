// 骰子分账 · 零依赖单文件服务端
// 运行: node server.js [端口] [口令]   设置口令后持久化到 dice-split-data.json
// 公网部署务必设置口令: PASSCODE=你们的暗号 node server.js 80
// 未设置口令时仅使用内存，房间无人连接即销毁。
//
// 状态: lobby -> idle -> rolling -> result -> idle；结果经所需成员确认后入账。

'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.argv[2] || process.env.PORT || 8787);
const PASSCODE = String(process.env.PASSCODE || process.argv[3] || '');
const TEMPORARY_MODE = !PASSCODE;
const ROOM_CONNECT_TIMEOUT_MS = 15000;
const MAX_ROOMS = Number(process.env.MAX_ROOMS || 100);      // 全服房间数上限，防资源滥用
const MAX_HISTORY = Number(process.env.MAX_HISTORY || 1000); // 每房间账目条数上限
const RATE_LIMIT = Number(process.env.RATE_LIMIT || 240);    // 同一网络的八位成员会共享 IP 限额。
const DATA_FILE = process.env.DICE_DATA || path.join(__dirname, 'dice-split-data.json');
const INDEX_HTML = path.join(__dirname, 'public', 'index.html');

// 未解锁时下发的口令页：不含任何应用代码
const LOCK_HTML = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#FF6B6B">
<title>🔒 骰子分账</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🎲</text></svg>">
<style>
  body { margin:0; min-height:100vh; min-height:100dvh; display:flex; align-items:center; justify-content:center;
    background:#FBF6EF; color:#3D3229;
    font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Noto Sans SC","Microsoft YaHei",sans-serif; }
  .card { background:#fff; width:min(330px,88vw); border-radius:24px; padding:36px 26px 28px; text-align:center;
    box-shadow:0 12px 40px rgba(61,50,41,.10); }
  .logo { font-size:44px; }
  h1 { font-size:19px; margin:10px 0 4px; }
  p { color:#9C8E80; font-size:13px; margin:0 0 18px; }
  input { width:100%; border:2px solid #F0E7DB; border-radius:14px; padding:13px; font-size:16px; text-align:center;
    outline:none; background:#FBF6EF; color:#3D3229; }
  input:focus { border-color:#FF6B6B; }
  button { width:100%; margin-top:12px; border:0; border-radius:16px; padding:14px; font-size:16px; font-weight:800;
    color:#fff; background:linear-gradient(135deg,#FF6B6B,#FF8E53); cursor:pointer; }
  button:disabled { opacity:.6; }
  .err { min-height:18px; margin-top:12px; color:#E25555; font-size:13px; }
  .shake { animation:shake .3s; }
  @keyframes shake { 0%,100%{transform:translateX(0)} 25%{transform:translateX(-6px)} 75%{transform:translateX(6px)} }
</style>
</head>
<body>
<div class="card">
  <div class="logo">🔒</div>
  <h1>骰子分账</h1>
  <p>这台服务器设了口令，验证后才能进入</p>
  <input id="pc" type="password" placeholder="输入口令" autocomplete="current-password">
  <button id="go">🔓 进入</button>
  <div class="err" id="err"></div>
</div>
<script>
  var input = document.getElementById('pc');
  function shake() {
    var c = document.querySelector('.card');
    c.classList.remove('shake'); void c.offsetWidth; c.classList.add('shake');
  }
  function go() {
    var btn = document.getElementById('go');
    btn.disabled = true;
    fetch('/api/unlock', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ passcode: input.value }) })
      .then(function (r) {
        if (r.ok) { location.reload(); return; }
        return r.json().then(function () {
          document.getElementById('err').textContent =
            r.status === 429 ? '尝试太频繁了，喝口水歇会儿' : '口令不对，再试试';
          shake();
        });
      })
      .catch(function () { document.getElementById('err').textContent = '网络开小差了，再试一次'; })
      .then(function () { btn.disabled = false; });
  }
  document.getElementById('go').addEventListener('click', go);
  input.addEventListener('keydown', function (e) { if (e.key === 'Enter') go(); });
  input.focus();
</script>
</body>
</html>`;

// ---------- 持久化 ----------
const rooms = new Map(); // code -> room

function persist(room) {
  if (TEMPORARY_MODE) return;
  const data = Object.fromEntries(rooms);
  data[room.code] = room;
  const json = JSON.stringify(data, (k, v) => (k.startsWith('_') || k === 'online' ? undefined : v));
  const temp = `${DATA_FILE}.tmp-${process.pid}`;
  try {
    fs.writeFileSync(temp, json, { flush: true });
    fs.renameSync(temp, DATA_FILE);
  } catch (e) {
    try { fs.unlinkSync(temp); } catch {}
    console.error('保存失败:', e.message);
    throw new Error('账本保存失败，本次操作未生效，请稍后重试');
  }
}

if (!TEMPORARY_MODE && fs.existsSync(DATA_FILE)) {
  const data = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  for (const [code, r] of Object.entries(data)) {
    r.capacity ??= 2;
    const ids = memberIds(r);
    for (const id of ids) {
      r.players[id].online = false;
      r.players[id]._conns = 0;
    }
    r.faces ||= 6;
    r.repayments ??= [];
    r.ledgerPending ??= null;
    r.repaymentCarry ??= {};
    if (r.repaymentCarryCents) {
      r.repaymentCarry.p1 = r.repaymentCarryCents;
      r.repaymentCarry.p2 = -r.repaymentCarryCents;
    }
    delete r.repaymentCarryCents;
    for (const h of r.history) h.participants ??= Object.keys(h.shares);
    if (r.bill) {
      r.bill.participants ??= Object.keys(r.bill.rolls);
      r.bill.required ??= [...r.bill.participants];
      if (r.bill.pending) upgradeProposal(r.bill.pending, r.bill.required);
    }
    if (r.ledgerPending) {
      const p = r.ledgerPending;
      const repayment = r.repayments.find((item) => item.id === p.repaymentId);
      const required = p.type === 'repay' ? [p.from, p.to]
        : p.type === 'deleteRepayment' && repayment ? [repayment.from, repayment.to] : ids;
      upgradeProposal(p, required);
    }
    rooms.set(code, r);
  }
  console.log(`已从数据文件恢复 ${rooms.size} 个房间`);
}

function memberIds(room) {
  return Object.keys(room.players).filter((id) => room.players[id]);
}

function upgradeProposal(proposal, required) {
  proposal.id ??= newToken();
  proposal.required ??= [...required];
  proposal.approvals ??= { [proposal.by]: true };
}

// ---------- 工具 ----------
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // 去掉 I/L/O/0/1 等易混淆字符
function newCode() {
  for (;;) {
    let c = '';
    for (let i = 0; i < 5; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
}
function newToken() { return crypto.randomBytes(16).toString('hex'); }
const rollDie = (faces) => crypto.randomInt(1, (faces || 6) + 1); // x 面骰 = 1–x 点

// ---------- 口令与限速 ----------
// 网页门禁：设了口令时，未解锁的浏览器访问首页只拿到一个解锁页，应用内容完全不下发。
// 解锁成功种一个 HMAC 签名的 Cookie（30 天有效），之后访问首页和各接口都凭它通过。
const UNLOCK_COOKIE = 'ds_unlock';
const UNLOCK_MAX_AGE = 30 * 24 * 3600; // 秒
const unlockToken = (ts) => ts + '.' + crypto.createHmac('sha256', PASSCODE).update(String(ts)).digest('hex');

function cookieOk(req) {
  const m = new RegExp('(?:^|;\\s*)' + UNLOCK_COOKIE + '=([^;]+)').exec(String(req.headers.cookie || ''));
  if (!m) return false;
  const dot = m[1].indexOf('.');
  if (dot < 0) return false;
  const ts = m[1].slice(0, dot);
  if (!/^\d+$/.test(ts) || Date.now() - Number(ts) > UNLOCK_MAX_AGE * 1000) return false;
  const a = Buffer.from(m[1].slice(dot + 1));
  const b = Buffer.from(crypto.createHmac('sha256', PASSCODE).update(ts).digest('hex'));
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// 先哈希再比较，消除长度侧信道；已解锁的 Cookie 同样放行
function passOk(given, req) {
  if (!PASSCODE) return true;
  if (req && cookieOk(req)) return true;
  const a = crypto.createHash('sha256').update(String(given || '')).digest();
  const b = crypto.createHash('sha256').update(PASSCODE).digest();
  return crypto.timingSafeEqual(a, b);
}
const needPass = (res) => json(res, 401, { error: 'need-passcode' });

const rateMap = new Map(); // ip -> { start, count }
const recoveryRateMap = new Map();
function rateLimit(key, map = rateMap, limit = RATE_LIMIT) {
  const now = Date.now();
  let e = map.get(key);
  if (!e || now - e.start > 60000) { e = { start: now, count: 0 }; map.set(key, e); }
  e.count++;
  if (map.size > 5000) for (const [k, v] of map) if (now - v.start > 60000) map.delete(k);
  return e.count <= limit;
}
function clientIp(req) {
  const xff = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return xff || req.socket.remoteAddress || '?';
}

function sanitizeName(s, fallback) {
  s = String(s || '').trim().slice(0, 12);
  return s || fallback;
}
function sanitizeAvatar(s, fallback) {
  s = String(s || '').trim().slice(0, 8);
  return /^[\p{Emoji_Presentation}\p{Extended_Pictographic}]$/u.test(s) ? s : fallback;
}

function splitByRatio(totalCents, participants, rolls) {
  const sum = participants.reduce((n, id) => n + rolls[id], 0);
  const shares = Object.fromEntries(participants.map((id) => [id, Math.floor(totalCents * rolls[id] / sum)]));
  const remainder = totalCents - Object.values(shares).reduce((n, cents) => n + cents, 0);
  // 余分按最大余数分配，同余时按固定成员顺序，避免客户端调整顺序获利。
  const ranked = [...participants].sort((a, b) => (totalCents * rolls[b]) % sum - (totalCents * rolls[a]) % sum);
  for (let i = 0; i < remainder; i++) shares[ranked[i]]++;
  return shares;
}

function balance(room) {
  const net = Object.fromEntries(memberIds(room).map((id) => [id, TEMPORARY_MODE ? room.repaymentCarry[id] || 0 : 0]));
  for (const h of room.history) {
    net[h.payer] += h.amountCents;
    for (const [id, cents] of Object.entries(h.shares)) net[id] -= cents;
  }
  for (const p of room.repayments) {
    net[p.from] += p.amountCents;
    net[p.to] -= p.amountCents;
  }
  return net;
}

function snapshot(room) {
  let bill = null;
  if (room.bill) {
    const b = room.bill;
    bill = {
      amountCents: b.amountCents, note: b.note, payer: b.payer,
      participants: b.participants, required: b.required,
      faces: b.faces, rerolls: b.rerolls, pending: b.pending || null,
      confirms: b.confirms,
      rolled: Object.fromEntries(b.participants.map((id) => [id, b.rolls[id] != null])),
    };
    if (room.phase === 'result') {
      bill.rolls = b.rolls;
      bill.ratio = b.ratio;
      bill.shares = b.shares;
    }
  }
  return {
    code: room.code, capacity: room.capacity,
    temporaryMode: TEMPORARY_MODE,
    version: room.version, phase: room.phase, faces: room.faces || 6,
    players: Object.fromEntries(Object.entries(room.players).map(([id, p]) => [id,
      p ? { name: p.name, avatar: p.avatar, online: !!p.online } : null])),
    bill, history: room.history, repayments: room.repayments,
    ...(TEMPORARY_MODE ? { repaymentCarry: room.repaymentCarry } : {}),
    ledgerPending: room.ledgerPending, balance: balance(room),
  };
}

function broadcast(room) {
  const payload = 'data: ' + JSON.stringify(snapshot(room)) + '\n\n';
  const set = clients.get(room.code);
  if (set) for (const res of set) {
    if (res.writableEnded || res.destroyed) continue;
    if (roleOf(room, res._token) !== res._role) {
      res.end('data: {"error":"stale"}\n\n');
      continue;
    }
    res.write(payload);
  }
}

function touch(room) {
  room.version = (room.version || 0) + 1;
  persist(room);
  rooms.set(room.code, room);
  broadcast(room);
}

const clients = new Map(); // code -> Set<res>
const initialConnectionTimers = new Map();

function setOnline(code, role, delta) {
  const room = rooms.get(code);
  if (!room) return;
  const p = room.players[role];
  if (!p) return;
  p._conns = Math.max(0, (p._conns || 0) + delta);
  const online = p._conns > 0;
  if (TEMPORARY_MODE && delta > 0) {
    clearTimeout(initialConnectionTimers.get(code));
    initialConnectionTimers.delete(code);
    delete room._recoveryUntil;
  }
  if (!!p.online !== online) {
    p.online = online;
    room.version += 1;
    if (TEMPORARY_MODE && !memberIds(room).some((id) => room.players[id].online) && !(room._recoveryUntil > Date.now())) {
      rooms.delete(code);
      return;
    }
    broadcast(room);
  }
}

function roleOf(room, token) {
  return memberIds(room).find((id) => room.players[id].token === token) || null;
}

function reserveConnection(room) {
  clearTimeout(initialConnectionTimers.get(room.code));
  const timer = setTimeout(() => {
    initialConnectionTimers.delete(room.code);
    const current = rooms.get(room.code);
    if (current && !memberIds(current).some((id) => current.players[id].online)) rooms.delete(room.code);
  }, ROOM_CONNECT_TIMEOUT_MS);
  timer.unref();
  initialConnectionTimers.set(room.code, timer);
}

function finishBill(room) {
  const b = room.bill;
  b.shares = splitByRatio(b.amountCents, b.participants, b.rolls);
  const gcd = (a, c) => c ? gcd(c, a % c) : a;
  const divisor = b.participants.reduce((n, id) => gcd(n, b.rolls[id]), 0);
  b.ratio = b.participants.map((id) => b.rolls[id] / divisor);
  room.phase = 'result';
}

function doReroll(room) {
  const b = room.bill;
  b.rolls = Object.fromEntries(b.participants.map((id) => [id, null]));
  b.confirms = Object.fromEntries(b.required.map((id) => [id, false]));
  delete b.shares;
  delete b.ratio;
  b.rerolls += 1;
  room.phase = 'rolling';
}

function proposal(type, by, required) {
  return { id: newToken(), type, by, required: [...required], approvals: { [by]: true } };
}

function approveProposal(pending, role) {
  if (!pending.required.includes(role)) return '你不是这项操作的确认成员';
  if (pending.approvals[role]) return '你已经确认过这项操作';
  pending.approvals[role] = true;
  return null;
}

function allApproved(pending) {
  return pending.required.every((id) => pending.approvals[id]);
}

function applyBillProposal(room) {
  const type = room.bill.pending.type;
  room.bill.pending = null;
  if (type === 'reroll') doReroll(room);
  else { room.bill = null; room.phase = 'idle'; }
}

function applyLedgerProposal(room, pending) {
  if (pending.type === 'deleteHistory') {
    room.history = room.history.filter((h) => h.id !== pending.historyId);
    if (TEMPORARY_MODE) { room.repayments = []; room.repaymentCarry = {}; }
  } else if (pending.type === 'deleteRepayment') {
    room.repayments = room.repayments.filter((p) => p.id !== pending.repaymentId);
  } else if (pending.type === 'clearHistory') {
    room.history = [];
    room.repayments = [];
    room.repaymentCarry = {};
  } else if (pending.type === 'repay') {
    if (TEMPORARY_MODE) {
      // 旧明细被裁剪后仍需抵扣余额，否则还过的钱会重新成为欠款。
      for (const p of room.repayments) {
        room.repaymentCarry[p.from] = (room.repaymentCarry[p.from] || 0) + p.amountCents;
        room.repaymentCarry[p.to] = (room.repaymentCarry[p.to] || 0) - p.amountCents;
      }
      room.repayments = [];
    }
    room.repayments.unshift({
      id: pending.id, ts: Date.now(), amountCents: pending.amountCents,
      from: pending.from, to: pending.to,
    });
  }
}

function handleAction(room, role, body) {
  room = structuredClone(room);
  const t = body.type;
  const bill = room.bill;

  switch (t) {
    case 'setMe': {
      const p = room.players[role];
      if (body.name !== undefined) p.name = sanitizeName(body.name, p.name);
      if (body.avatar !== undefined) p.avatar = sanitizeAvatar(body.avatar, p.avatar);
      break;
    }
    case 'start': {
      if (room.phase !== 'idle') return '当前状态不能开新的一笔';
      if (room.ledgerPending) return '先处理待确认的账本操作';
      if (!TEMPORARY_MODE && room.history.length >= MAX_HISTORY) return `账本已满（${MAX_HISTORY} 条），先删几条旧账再记`;
      const cents = body.amountCents;
      if (!Number.isSafeInteger(cents) || cents < 1 || cents > 1000000000) return '金额不合法';
      const ids = memberIds(room);
      const selected = body.participants === undefined ? ids : body.participants;
      if (!Array.isArray(selected) || selected.length < 2 || selected.length > ids.length ||
          new Set(selected).size !== selected.length || !selected.every((id) => ids.includes(id))) return '请选择至少两位有效且不重复的参与成员';
      const participants = ids.filter((id) => selected.includes(id));
      if (!participants.includes(role)) return '只能为自己参与的分账开账';
      const payer = body.payer === undefined ? role : body.payer;
      if (!participants.includes(payer)) return '垫付人必须参与本笔分账';
      const required = TEMPORARY_MODE ? ids : participants;
      room.bill = {
        amountCents: cents,
        note: String(body.note || '').trim().slice(0, 30),
        payer, participants, required,
        faces: room.faces || 6,
        rolls: Object.fromEntries(participants.map((id) => [id, null])),
        rerolls: 0,
        confirms: Object.fromEntries(required.map((id) => [id, false])),
      };
      room.phase = 'rolling';
      break;
    }
    case 'roll': {
      if (room.phase !== 'rolling') return '当前不能掷骰';
      if (!bill.participants.includes(role)) return '你没有参与本笔分账';
      if (bill.pending) return '有作废提议待处理，请先回应';
      if (bill.rolls[role] != null) return '你已经掷过了';
      bill.rolls[role] = rollDie(bill.faces);
      if (bill.participants.every((id) => bill.rolls[id] != null)) finishBill(room);
      break;
    }
    case 'setFaces': {
      if (room.phase !== 'idle' && room.phase !== 'lobby') return '这一把掷完再换骰子';
      const f = Math.round(Number(body.faces));
      if (!Number.isFinite(f) || f < 2 || f > 120) return '面数不合法（2–120）';
      if (room.faces !== f) room.faces = f;
      break;
    }
    case 'reroll':
    case 'void': {
      if (t === 'reroll' && room.phase !== 'result') return '当前不能重掷';
      if (!bill || (room.phase !== 'rolling' && room.phase !== 'result')) return '没有进行中的账';
      if (!bill.required.includes(role)) return '你不是本笔分账的确认成员';
      if (bill.pending || body.proposalId !== undefined) {
        if (!bill.pending || bill.pending.id !== body.proposalId) return '提议已变化，请查看最新分账';
        if (bill.pending.type !== t) return '请先回应当前提议';
        const err = approveProposal(bill.pending, role);
        if (err) return err;
        if (allApproved(bill.pending)) applyBillProposal(room);
      } else {
        bill.pending = proposal(t, role, bill.required);
      }
      break;
    }
    case 'confirm': {
      if (room.phase !== 'result') return '当前不能确认';
      if (!bill.required.includes(role)) return '你不是本笔分账的确认成员';
      if (bill.pending) return '有重掷或作废提议待回应，请先处理';
      if (bill.confirms[role]) return '你已经确认过本笔分账';
      bill.confirms[role] = true;
      if (bill.required.every((id) => bill.confirms[id])) {
        if (!TEMPORARY_MODE && room.history.length >= MAX_HISTORY) return `账本已满（${MAX_HISTORY} 条），请先清理旧账`;
        room.history.unshift({
          id: newToken(), ts: Date.now(), amountCents: bill.amountCents,
          note: bill.note, payer: bill.payer, participants: bill.participants,
          faces: bill.faces, rolls: bill.rolls, ratio: bill.ratio,
          shares: bill.shares, rerolls: bill.rerolls,
        });
        if (TEMPORARY_MODE) {
          room.history = room.history.slice(0, 1);
          room.repayments = [];
          room.repaymentCarry = {};
        }
        room.bill = null;
        room.phase = 'idle';
      }
      break;
    }
    case 'respond': {
      const pending = bill?.pending;
      if (!pending || pending.id !== body.proposalId) return '提议已变化，请查看最新分账';
      if (typeof body.approve !== 'boolean') return '请选择同意或不同意';
      const err = approveProposal(pending, role);
      if (err) return err;
      if (!body.approve) bill.pending = null;
      else if (allApproved(pending)) applyBillProposal(room);
      break;
    }
    case 'withdraw': {
      if (!bill?.pending || bill.pending.id !== body.proposalId) return '提议已变化，请查看最新分账';
      if (bill.pending.by !== role) return '只能撤回自己发起的提议';
      bill.pending = null;
      break;
    }
    case 'deleteHistory':
    case 'deleteRepayment':
    case 'clearHistory':
    case 'repay': {
      if (room.phase !== 'idle') return '先完成当前这一笔，再管理账本';
      if (room.ledgerPending) return '已有账本操作等待确认';
      const ids = memberIds(room);
      const pending = proposal(t, role, ids);
      if (t === 'deleteHistory') {
        if (!room.history.some((h) => h.id === body.id)) return '没有找到这条记录';
        pending.historyId = body.id;
      } else if (t === 'deleteRepayment') {
        const repayment = room.repayments.find((p) => p.id === body.id);
        if (!repayment) return '没有找到这条还款记录';
        pending.repaymentId = body.id;
        pending.required = [repayment.from, repayment.to];
      } else if (t === 'clearHistory') {
        if (!room.history.length && !room.repayments.length && !Object.values(room.repaymentCarry).some(Boolean)) return '账本已经是空的';
      } else {
        const { from, to, amountCents: cents } = body;
        if (from === to || !ids.includes(from) || !ids.includes(to)) return '请选择有效的付款和收款成员';
        const net = balance(room);
        if (net[from] >= 0 || net[to] <= 0) return '付款人须有待还余额，收款人须有应收余额';
        if (!Number.isSafeInteger(cents) || cents < 1 || cents > 1000000000 || cents > Math.min(-net[from], net[to])) return '还款金额须大于零且不超过双方可结算金额及一千万元';
        if (!TEMPORARY_MODE && room.repayments.length >= MAX_HISTORY) return '还款记录已满，请先确认清理旧记录';
        pending.amountCents = cents;
        pending.from = from;
        pending.to = to;
        pending.required = [from, to];
      }
      if (!pending.required.includes(role)) return '只能管理与自己有关的还款';
      room.ledgerPending = pending;
      break;
    }
    case 'ledgerRespond': {
      const pending = room.ledgerPending;
      if (!pending || pending.id !== body.proposalId) return '提议已变化，请查看最新账本';
      if (typeof body.approve !== 'boolean') return '请选择同意或不同意';
      const err = approveProposal(pending, role);
      if (err) return err;
      if (!body.approve) room.ledgerPending = null;
      else if (allApproved(pending)) {
        applyLedgerProposal(room, pending);
        room.ledgerPending = null;
      }
      break;
    }
    case 'ledgerWithdraw': {
      const pending = room.ledgerPending;
      if (!pending || pending.id !== body.proposalId) return '提议已变化，请查看最新账本';
      if (pending.by !== role) return '只能撤回自己发起的提议';
      room.ledgerPending = null;
      break;
    }
    default:
      return '未知动作';
  }
  touch(room);
  return null;
}

// ---------- HTTP ----------
function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > 100 * 1024) { reject(new Error('too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {}); }
      catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

function recoveryHash(code) {
  return crypto.createHash('sha256').update(String(code).replace(/[-\s]/g, '').toUpperCase()).digest('hex');
}

function newRecoveryCode(players = {}) {
  const used = new Set(Object.values(players).filter(Boolean).map((player) => player.recoveryHash));
  for (;;) {
    const code = String(crypto.randomInt(100000000)).padStart(8, '0');
    if (!used.has(recoveryHash(code))) return code;
  }
}

function makePlayer(name, avatar, players = {}) {
  const recoveryCode = newRecoveryCode(players);
  return {
    player: { name, avatar, token: newToken(), recoveryHash: recoveryHash(recoveryCode), online: false, _conns: 0 },
    recoveryCode,
  };
}

function makeRoom(name, avatar, capacity) {
  const { player, recoveryCode } = makePlayer(name, avatar);
  const room = {
    code: newCode(), capacity,
    version: 0, createdAt: Date.now(), phase: 'lobby', faces: 6,
    players: { p1: player, p2: null },
    bill: null, history: [], repayments: [], repaymentCarry: {}, ledgerPending: null,
  };
  return { room, token: player.token, recoveryCode };
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const pathName = url.pathname;

  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Cache-Control', 'no-store');

  // 统一限速：掐死房间码枚举与接口滥用
  if (!rateLimit(clientIp(req))) {
    console.log(`[限速] ${clientIp(req)} 请求过频`);
    json(res, 429, { error: '请求太频繁了，喝口水歇会儿' });
    return;
  }

  try {
    // 首页（设了口令时，未解锁只下发口令页，应用内容不出门）
    if (req.method === 'GET' && (pathName === '/' || pathName === '/index.html')) {
      res.setHeader('Cache-Control', 'no-store');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      if (PASSCODE && !cookieOk(req)) { res.end(LOCK_HTML); return; }
      res.end(fs.readFileSync(INDEX_HTML, 'utf8').replace('__TEMPORARY_MODE__', String(TEMPORARY_MODE)));
      return;
    }

    // 解锁网页门禁（口令正确 → 种 30 天签名 Cookie）
    if (req.method === 'POST' && pathName === '/api/unlock' && PASSCODE) {
      const body = await readBody(req);
      if (!passOk(body.passcode)) { needPass(res); return; }
      res.setHeader('Set-Cookie',
        `${UNLOCK_COOKIE}=${unlockToken(Date.now())}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${UNLOCK_MAX_AGE}`);
      json(res, 200, { ok: true });
      return;
    }

    // SSE 实时状态推送
    if (req.method === 'GET' && pathName === '/events') {
      if (!passOk(url.searchParams.get('passcode'), req)) { needPass(res); return; }
      const room = rooms.get((url.searchParams.get('code') || '').toUpperCase());
      const role = room && roleOf(room, url.searchParams.get('token') || '');
      if (!room || !role) { json(res, 404, { error: 'stale' }); return; }

      res.writeHead(200, {
        'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
      });
      res.write('retry: 2000\n');
      res.write('data: ' + JSON.stringify(snapshot(room)) + '\n\n');

      let set = clients.get(room.code);
      if (!set) { set = new Set(); clients.set(room.code, set); }
      res._role = role;
      res._token = url.searchParams.get('token');
      set.add(res);
      setOnline(room.code, role, +1);

      const heartbeat = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) { /* 忽略 */ } }, 25000);
      req.on('close', () => {
        clearInterval(heartbeat);
        set.delete(res);
        if (!set.size) clients.delete(room.code);
        setOnline(room.code, role, -1);
      });
      return;
    }

    // 身份有效性轻量校验（前端连接 SSE 前调用）
    if (req.method === 'GET' && pathName === '/api/ping') {
      if (!passOk(url.searchParams.get('passcode'), req)) { needPass(res); return; }
      const room = rooms.get((url.searchParams.get('code') || '').toUpperCase());
      const role = room && roleOf(room, url.searchParams.get('token') || '');
      if (!room || !role) { json(res, 401, { error: 'stale' }); return; }
      json(res, 200, { ok: true, code: room.code, role });
      return;
    }

    // 建房
    if (req.method === 'POST' && pathName === '/api/create') {
      const body = await readBody(req);
      if (!passOk(body.passcode, req)) { needPass(res); return; }
      if (rooms.size >= MAX_ROOMS) { json(res, 403, { error: '房间数已达上限，联系服务器主人清理' }); return; }
      const capacity = body.capacity === undefined ? 2 : body.capacity;
      if (!Number.isInteger(capacity) || capacity < 2 || capacity > 8) {
        json(res, 400, { error: '房间人数须为 2～8 人' }); return;
      }
      const { room, token, recoveryCode } = makeRoom(
        sanitizeName(body.name, '玩家1'),
        sanitizeAvatar(body.avatar, '🐻'), capacity
      );
      touch(room);
      if (TEMPORARY_MODE) reserveConnection(room);
      json(res, 200, { code: room.code, token, role: 'p1', recoveryCode });
      return;
    }

    if (req.method === 'POST' && pathName === '/api/join') {
      const body = await readBody(req);
      if (!passOk(body.passcode, req)) { needPass(res); return; }
      if (body.claimRole !== undefined) { json(res, 403, { error: '不能凭房间号认领身份，请使用个人恢复码' }); return; }
      const current = rooms.get(String(body.code || '').trim().toUpperCase());
      if (!current) { json(res, 404, { error: '房间不存在，检查一下房间码' }); return; }
      const room = structuredClone(current);
      const count = memberIds(room).length;
      if (count >= room.capacity) { json(res, 409, { error: 'full' }); return; }
      const role = `p${count + 1}`;
      const { player, recoveryCode } = makePlayer(
        sanitizeName(body.name, `玩家${count + 1}`), sanitizeAvatar(body.avatar, '🐱'), room.players
      );
      room.players[role] = player;
      if (room.phase === 'lobby') room.phase = 'idle';
      touch(room);
      json(res, 200, { code: room.code, token: player.token, role, recoveryCode });
      return;
    }

    if (req.method === 'POST' && pathName === '/api/recover') {
      const body = await readBody(req);
      if (!passOk(body.passcode, req)) { needPass(res); return; }
      const current = rooms.get(String(body.code || '').trim().toUpperCase());
      // 短数字码按房间限速，不能通过更换 IP 或代理头绕过。
      if (current && !rateLimit(current.code, recoveryRateMap, 20)) {
        json(res, 429, { error: '此房间恢复码操作太频繁，请一分钟后重试' }); return;
      }
      const hash = typeof body.recoveryCode === 'string' ? recoveryHash(body.recoveryCode) : null;
      const role = current && memberIds(current).find((id) => current.players[id].recoveryHash === hash);
      if (!role) { json(res, 401, { error: '房间号或个人恢复码不正确' }); return; }
      const room = structuredClone(current);
      const token = newToken();
      room.players[role].token = token;
      // 换机先关闭旧连接，新设备拿到响应再重连，需要短暂保留临时房间。
      if (TEMPORARY_MODE) room._recoveryUntil = Date.now() + ROOM_CONNECT_TIMEOUT_MS;
      touch(room);
      if (TEMPORARY_MODE) reserveConnection(room);
      json(res, 200, { code: room.code, token, role, recoveryCode: String(body.recoveryCode).replace(/[-\s]/g, '').toUpperCase().match(/.{8}/g).join('-') });
      return;
    }

    if (req.method === 'POST' && pathName === '/api/recovery') {
      const body = await readBody(req);
      if (!passOk(body.passcode, req)) { needPass(res); return; }
      const current = rooms.get(String(body.code || '').trim().toUpperCase());
      const role = current && roleOf(current, body.token || '');
      if (!role) { json(res, 401, { error: 'stale' }); return; }
      const custom = body.recoveryCode !== undefined;
      if (custom && (typeof body.recoveryCode !== 'string' || !/^[0-9]{8}$/.test(body.recoveryCode))) {
        json(res, 400, { error: '恢复码必须是 8 位数字' }); return;
      }
      if (!rateLimit(current.code, recoveryRateMap, 20)) {
        json(res, 429, { error: '此房间恢复码操作太频繁，请一分钟后重试' }); return;
      }
      const recoveryCode = custom ? body.recoveryCode : newRecoveryCode(current.players);
      const hash = recoveryHash(recoveryCode);
      if (memberIds(current).some((id) => current.players[id].recoveryHash === hash)) {
        json(res, 409, { error: '此恢复码不可用，请换一组 8 位数字' }); return;
      }
      const room = structuredClone(current);
      room.players[role].recoveryHash = hash;
      touch(room);
      json(res, 200, { recoveryCode });
      return;
    }

    // 房间动作
    if (req.method === 'POST' && pathName === '/api/action') {
      const body = await readBody(req);
      if (!passOk(body.passcode, req)) { needPass(res); return; }
      const room = rooms.get(String(body.code || '').toUpperCase());
      const role = room && roleOf(room, body.token || '');
      if (!room || !role) { json(res, 401, { error: 'stale' }); return; }
      const err = handleAction(room, role, body);
      if (err) { json(res, 400, { error: err }); return; }
      const updated = rooms.get(room.code);
      json(res, 200, { ok: true, roll: body.type === 'roll' ? updated.bill.rolls[role] : undefined });
      return;
    }

    json(res, 404, { error: 'not found' });
  } catch (e) {
    json(res, 500, { error: '服务器开小差了: ' + e.message });
  }
});

server.listen(PORT, () => {
  console.log(`🎲 骰子分账已启动: http://localhost:${server.address().port}`);
  console.log(`   口令保护: ${PASSCODE ? '已开启 ✓（网页未解锁只见口令页）' : '未开启（仅建议同网/私网这样跑）'}`);
  if (TEMPORARY_MODE) {
    console.warn('   警告：目前未设置密码，已启用临时模式；不读取或写入账本文件，重启后不恢复。');
    console.warn('   最后一人断开即销毁房间和全部记录；新建房间 15 秒内无人连接也会回收。');
    console.warn('   分账仅保留最近一笔，新分账替换旧分账及其还款；还款明细仅留最近一条，当前分账已还金额仍累计。');
    console.warn('   临时清理不能代替访问保护，公网部署仍建议设置 PASSCODE。');
  }
  console.log(`   限额: 房间≤${MAX_ROOMS} · 每房账目≤${TEMPORARY_MODE ? 1 : MAX_HISTORY} · 每 IP ${RATE_LIMIT} 次/分钟`);
  console.log('   两台手机连同一个网络，用浏览器访问上面的地址即可');
});
