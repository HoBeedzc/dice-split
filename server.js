// 骰子分账 · 零依赖单文件服务端
// 运行: node server.js [端口] [口令]   设置口令后持久化到 dice-split-data.json
// 公网部署务必设置口令: PASSCODE=你们的暗号 node server.js 80
// 未设置口令时仅使用内存，房间无人连接即销毁。
//
// 房间状态机: lobby(等对方加入) -> idle(可记账) -> rolling(双方掷骰中)
//             -> result(揭晓, 双方确认) -> 入账回 idle
// 掷骰随机数由服务器生成并广播，双方都无法本地作弊。

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
const RATE_LIMIT = Number(process.env.RATE_LIMIT || 60);     // 每 IP 每分钟请求数上限（正常两人使用远低于此）
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
    r.players.p1.online = false;
    r.players.p1._conns = 0;
    if (r.players.p2) {
      r.players.p2.online = false;
      r.players.p2._conns = 0;
    }
    if (!r.faces) r.faces = 6;
    r.repayments ??= [];
    r.ledgerPending ??= null;
    if (r.bill?.pending) r.bill.pending.id ??= newToken();
    rooms.set(code, r);
  }
  console.log(`已从数据文件恢复 ${rooms.size} 个房间`);
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
function rateLimit(ip) {
  const now = Date.now();
  let e = rateMap.get(ip);
  if (!e || now - e.start > 60000) { e = { start: now, count: 0 }; rateMap.set(ip, e); }
  e.count++;
  if (rateMap.size > 5000) for (const [k, v] of rateMap) if (now - v.start > 60000) rateMap.delete(k);
  return e.count <= RATE_LIMIT;
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

// 按点数比例拆分金额（单位: 分）。前者按比例四舍五入，余数归后者，保证合计相等。
function splitByRatio(totalCents, r1, r2) {
  const s1 = Math.round((totalCents * r1) / (r1 + r2));
  return { s1, s2: totalCents - s1 };
}

function balance(room) {
  let p1 = TEMPORARY_MODE ? (room.repaymentCarryCents || 0) : 0;
  for (const h of room.history) p1 += (h.payer === 'p1' ? h.amountCents : 0) - h.shares.p1;
  for (const p of room.repayments) p1 += p.from === 'p1' ? p.amountCents : -p.amountCents;
  return { p1, p2: -p1 };
}

// ---------- 快照与广播 ----------
// rolling 阶段不泄露对方点数（各自点数只通过 roll 动作的响应发给本人），
// 双方都掷完后进入 result 才一起揭晓，保留开盲盒的悬念。
function snapshot(room) {
  const p1 = room.players.p1, p2 = room.players.p2;
  let bill = null;
  if (room.bill) {
    const b = room.bill;
    bill = {
      amountCents: b.amountCents, note: b.note, payer: b.payer,
      faces: b.faces,
      rerolls: b.rerolls,
      pending: b.pending || null,
      confirms: { p1: !!b.confirms.p1, p2: !!b.confirms.p2 },
      rolled: { p1: b.rolls.p1 != null, p2: b.rolls.p2 != null },
    };
    if (room.phase === 'result') {
      bill.rolls = { p1: b.rolls.p1, p2: b.rolls.p2 };
      bill.ratio = b.ratio;
      bill.shares = b.shares;
    }
  }
  return {
    code: room.code,
    temporaryMode: TEMPORARY_MODE,
    version: room.version,
    phase: room.phase,
    faces: room.faces || 6,
    players: {
      p1: p1 ? { name: p1.name, avatar: p1.avatar, online: !!p1.online } : null,
      p2: p2 ? { name: p2.name, avatar: p2.avatar, online: !!p2.online } : null,
    },
    bill,
    history: room.history,
    repayments: room.repayments,
    ...(TEMPORARY_MODE ? { repaymentCarryCents: room.repaymentCarryCents || 0 } : {}),
    ledgerPending: room.ledgerPending,
    balance: balance(room),
  };
}

function broadcast(room) {
  const payload = 'data: ' + JSON.stringify(snapshot(room)) + '\n\n';
  const set = clients.get(room.code);
  if (set) for (const res of set) { try { res.write(payload); } catch (e) { /* 连接已断 */ } }
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
  }
  if (!!p.online !== online) {
    p.online = online;
    room.version += 1;
    if (TEMPORARY_MODE && !room.players.p1.online && !room.players.p2?.online) {
      rooms.delete(code);
      return;
    }
    broadcast(room);
  }
}

function roleOf(room, token) {
  if (room.players.p1 && room.players.p1.token === token) return 'p1';
  if (room.players.p2 && room.players.p2.token === token) return 'p2';
  return null;
}

// ---------- 动作处理 ----------
function finishBill(room) {
  const b = room.bill;
  const total = b.amountCents;
  const { s1, s2 } = splitByRatio(total, b.rolls.p1, b.rolls.p2);
  const g = (n, m) => (m ? g(m, n % m) : n);
  const d = g(b.rolls.p1, b.rolls.p2);
  b.ratio = [b.rolls.p1 / d, b.rolls.p2 / d];
  b.shares = { p1: s1, p2: s2 };
  room.phase = 'result';
}

// 重掷/作废都需要双方同意：第一次点击成为提议（bill.pending），
// 对方用 respond 同意/拒绝，发起方可用 withdraw 撤回；对方点同样的动作视为同意。
function doReroll(room) {
  const b = room.bill;
  b.rolls = { p1: null, p2: null };
  b.confirms = { p1: false, p2: false };
  b.rerolls += 1;
  room.phase = 'rolling';
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
      if (!room.players.p2) return '对方还没加入房间';
      if (room.ledgerPending) return '先处理待确认的账本操作';
      if (!TEMPORARY_MODE && room.history.length >= MAX_HISTORY) return `账本已满（${MAX_HISTORY} 条），先删几条旧账再记`;
      const cents = Math.round(Number(body.amountCents));
      if (!Number.isFinite(cents) || cents < 1 || cents > 1000000000) return '金额不合法';
      room.bill = {
        amountCents: cents,
        note: String(body.note || '').trim().slice(0, 30),
        payer: body.payer === 'p2' ? 'p2' : 'p1',
        faces: room.faces || 6, // 开账时锁定面数，中途不可换
        rolls: { p1: null, p2: null },
        rerolls: 0,
        confirms: { p1: false, p2: false },
      };
      room.phase = 'rolling';
      break;
    }
    case 'roll': {
      if (room.phase !== 'rolling') return '当前不能掷骰';
      if (bill.pending) return '对方提议作废这笔，先回应一下';
      if (bill.rolls[role] != null) return '你已经掷过了';
      bill.rolls[role] = rollDie(bill.faces);
      if (bill.rolls.p1 != null && bill.rolls.p2 != null) finishBill(room);
      break;
    }
    case 'setFaces': {
      if (room.phase !== 'idle' && room.phase !== 'lobby') return '这一把掷完再换骰子';
      const f = Math.round(Number(body.faces));
      if (!Number.isFinite(f) || f < 2 || f > 120) return '面数不合法（2–120）';
      if (room.faces !== f) room.faces = f;
      break;
    }
    case 'reroll': {
      if (room.phase !== 'result') return '当前不能重掷';
      if (bill.pending || body.proposalId !== undefined) {
        if (!bill.pending || bill.pending.id !== body.proposalId) return '提议已变化，请查看最新分账';
        if (bill.pending.by === role) return '你已提议重掷，等对方回应';
        if (bill.pending.type !== 'reroll') return '对方提议的是作废，先回应那个';
        bill.pending = null;
        doReroll(room);
        break;
      }
      bill.pending = { id: newToken(), type: 'reroll', by: role };
      break;
    }
    case 'confirm': {
      if (room.phase !== 'result') return '当前不能确认';
      if (bill.pending) return '对方有重掷/作废提议待回应，先处理一下';
      bill.confirms[role] = true;
      if (bill.confirms.p1 && bill.confirms.p2) {
        // 容量兜底（正常已被 start 前置拦截）：回滚确认并作废这笔，避免卡死在 result
        if (!TEMPORARY_MODE && room.history.length >= MAX_HISTORY) {
          bill.confirms = { p1: false, p2: false };
          room.bill = null;
          room.phase = 'idle';
          touch(room); // 此分支改了状态，需广播给双方
          return `账本已满（${MAX_HISTORY} 条），这笔没法入账，先删几条旧账吧`;
        }
        room.history.unshift({
          id: newToken().slice(0, 8),
          ts: Date.now(),
          amountCents: bill.amountCents,
          note: bill.note,
          payer: bill.payer,
          faces: bill.faces,
          rolls: bill.rolls,
          ratio: bill.ratio,
          shares: bill.shares,
          rerolls: bill.rerolls,
        });
        if (TEMPORARY_MODE) {
          room.history = room.history.slice(0, 1);
          room.repayments = [];
          room.repaymentCarryCents = 0;
        }
        room.bill = null;
        room.phase = 'idle';
      }
      break;
    }
    case 'void': {
      if (room.phase !== 'rolling' && room.phase !== 'result') return '没有进行中的账';
      if (bill.pending || body.proposalId !== undefined) {
        if (!bill.pending || bill.pending.id !== body.proposalId) return '提议已变化，请查看最新分账';
        if (bill.pending.by === role) return '你已提议过，等对方回应';
        if (bill.pending.type !== 'void') return '对方提议的是重掷，先回应那个';
        bill.pending = null;
        room.bill = null;
        room.phase = 'idle';
        break;
      }
      bill.pending = { id: newToken(), type: 'void', by: role };
      break;
    }
    case 'respond': {
      if (!bill?.pending || bill.pending.id !== body.proposalId) return '提议已变化，请查看最新分账';
      if (bill.pending.by === role) return '这是你自己的提议，等对方回应';
      if (typeof body.approve !== 'boolean') return '请选择同意或不同意';
      const type = bill.pending.type;
      bill.pending = null;
      if (body.approve) {
        if (type === 'reroll') doReroll(room);
        else { room.bill = null; room.phase = 'idle'; }
      }
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
      if (room.ledgerPending) return '已有账本操作等待双方确认';
      const pending = { id: newToken(), type: t, by: role };
      if (t === 'deleteHistory') {
        if (!room.history.some((h) => h.id === body.id)) return '没有找到这条记录';
        pending.historyId = body.id;
      } else if (t === 'deleteRepayment') {
        if (!room.repayments.some((p) => p.id === body.id)) return '没有找到这条还款记录';
        pending.repaymentId = body.id;
      } else if (t === 'clearHistory') {
        if (!room.history.length && !room.repayments.length && !room.repaymentCarryCents) return '账本已经是空的';
      } else {
        const net = balance(room).p1;
        if (net === 0) return '目前已两清，无需还款';
        const cents = body.amountCents;
        if (!Number.isSafeInteger(cents) || cents < 1 || cents > Math.abs(net)) return '还款金额须大于零且不超过当前欠款';
        if (!TEMPORARY_MODE && room.repayments.length >= MAX_HISTORY) return '还款记录已满，请先双方确认清理旧记录';
        pending.amountCents = cents;
        pending.from = net < 0 ? 'p1' : 'p2';
        pending.to = net < 0 ? 'p2' : 'p1';
      }
      room.ledgerPending = pending;
      break;
    }
    case 'ledgerRespond': {
      const pending = room.ledgerPending;
      if (!pending || pending.id !== body.proposalId) return '提议已变化，请查看最新账本';
      if (pending.by === role) return '需要另一人确认这项操作';
      if (typeof body.approve !== 'boolean') return '请选择同意或不同意';
      if (body.approve) {
        if (pending.type === 'deleteHistory') {
          room.history = room.history.filter((h) => h.id !== pending.historyId);
          if (TEMPORARY_MODE) {
            room.repayments = [];
            room.repaymentCarryCents = 0;
          }
        } else if (pending.type === 'deleteRepayment') {
          room.repayments = room.repayments.filter((p) => p.id !== pending.repaymentId);
        } else if (pending.type === 'clearHistory') {
          room.history = [];
          room.repayments = [];
          if (TEMPORARY_MODE) room.repaymentCarryCents = 0;
        } else if (pending.type === 'repay') {
          if (TEMPORARY_MODE) {
            // 丢弃旧明细，但已确认的还款不能因此重新变成欠款。
            for (const p of room.repayments) {
              room.repaymentCarryCents = (room.repaymentCarryCents || 0) + (p.from === 'p1' ? p.amountCents : -p.amountCents);
            }
            room.repayments = [];
          }
          room.repayments.unshift({
            id: pending.id, ts: Date.now(), amountCents: pending.amountCents,
            from: pending.from, to: pending.to,
          });
        }
      }
      room.ledgerPending = null;
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

function makeRoom(name, avatar) {
  const code = newCode();
  const token = newToken();
  const room = {
    code,
    version: 0,
    createdAt: Date.now(),
    phase: 'lobby',
    faces: 6,
    players: {
      p1: { name, avatar, token, online: false, _conns: 0 },
      p2: null,
    },
    bill: null,
    history: [],
    repayments: [],
    ledgerPending: null,
  };
  return { room, token };
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const pathName = url.pathname;

  res.setHeader('X-Content-Type-Options', 'nosniff');

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
      const { room, token } = makeRoom(
        sanitizeName(body.name, '玩家1'),
        sanitizeAvatar(body.avatar, '🐻')
      );
      touch(room);
      if (TEMPORARY_MODE) {
        // 建房响应之后浏览器才能连接 SSE，未连接的预留房间不能永久占用容量。
        const timer = setTimeout(() => {
          initialConnectionTimers.delete(room.code);
          if (!clients.has(room.code)) rooms.delete(room.code);
        }, ROOM_CONNECT_TIMEOUT_MS);
        timer.unref();
        initialConnectionTimers.set(room.code, timer);
      }
      json(res, 200, { code: room.code, token, role: 'p1' });
      return;
    }

    // 加入（含换机认领）
    if (req.method === 'POST' && pathName === '/api/join') {
      const body = await readBody(req);
      if (!passOk(body.passcode, req)) { needPass(res); return; }
      const current = rooms.get(String(body.code || '').trim().toUpperCase());
      if (!current) { json(res, 404, { error: '房间不存在，检查一下房间码' }); return; }
      const room = structuredClone(current);
      if (room.players.p2) {
        // 房间已满：支持凭房间码认领身份（换手机场景，前端有二次确认）
        if (body.claimRole === 'p1' || body.claimRole === 'p2') {
          const role = body.claimRole;
          const token = newToken();
          room.players[role].token = token;
          if (body.name) room.players[role].name = sanitizeName(body.name, room.players[role].name);
          touch(room);
          json(res, 200, { code: room.code, token, role });
        } else {
          json(res, 409, { error: 'full' });
        }
        return;
      }
      const token = newToken();
      room.players.p2 = {
        name: sanitizeName(body.name, '玩家2'),
        avatar: sanitizeAvatar(body.avatar, '🐱'),
        token, online: false, _conns: 0,
      };
      room.phase = 'idle';
      touch(room);
      json(res, 200, { code: room.code, token, role: 'p2' });
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
