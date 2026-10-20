// 家庭聊天服务端：Node.js + WebSocket
// 功能：多人文字聊天 + WebRTC 语音通话信令 + 在线成员列表
// 运行：node server.js  （默认端口 3000，可用环境变量 PORT 修改）
// 多连接：同一个昵称允许并存多条连接（App 原生服务常驻 + 网页终端），
//         只要还有任意一条活着就算在线，全部断开才广播"走了"。

const http = require('http');
const fs = require('fs');
const path = require('path');
const { attachWebSocketServer } = require('./ws'); // 零依赖 WebSocket（无需 npm install）

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');

// ---------- HTTP 静态服务 ----------
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.wav': 'audio/wav',
};

// ---------- 媒体存储（图片/语音走 HTTP，避开隧道丢 WS 大帧） ----------
// 大文件经 HTTP 上传拿到 id，WS 只广播一条短消息（含 id），对端再 HTTP 取回。
// 内存存储 + 15 分钟 TTL，容量上限自动清理，足够家庭小群组使用。
const mediaStore = new Map(); // id -> { buf, type, expire }
let mediaSeq = 1;
const MAX_UPLOAD = 6 * 1024 * 1024; // 单文件上限 6MB
function gcMedia() {
  if (mediaStore.size <= 800) return;
  const now = Date.now();
  for (const [k, v] of mediaStore) if (v.expire < now) mediaStore.delete(k);
}

const server = http.createServer((req, res) => {
  const rawPath = (req.url || '/').split('?')[0];
  let urlPath;
  try { urlPath = decodeURIComponent(rawPath); } catch (e) { urlPath = rawPath; } // 畸形 %xx 不再 crash 进程
  // ---- 保活探针：Render 免费档 15 分钟无请求会休眠，外部监控定时打这个端点即可保活 ----
  if (req.method === 'GET' && urlPath === '/ping') {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Cache-Control': 'no-store' });
    res.end('pong');
    return;
  }
  // ---- 媒体上传：图片/语音的二进制经 HTTP POST（隧道对连续 WS 大帧会丢，HTTP 更稳） ----
  if (req.method === 'POST' && urlPath === '/upload') {
    const chunks = []; let size = 0; let aborted = false;
    req.on('data', (c) => {
      if (aborted) return;
      size += c.length;
      if (size > MAX_UPLOAD) { aborted = true; chunks.length = 0; req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      if (aborted) return;
      const buf = Buffer.concat(chunks);
      if (!buf.length) { res.writeHead(400); res.end('empty'); return; }
      gcMedia();
      const id = 'm' + Date.now().toString(36) + (mediaSeq++).toString(36);
      const type = (req.headers['content-type'] || 'application/octet-stream').split(';')[0];
      mediaStore.set(id, { buf, type, expire: Date.now() + 15 * 60 * 1000 });
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
      res.end(JSON.stringify({ id }));
    });
    req.on('error', () => { if (!res.headersSent) { res.writeHead(400); res.end('err'); } });
    return;
  }
  // ---- 媒体下载：按 id 取回二进制 ----
  if (req.method === 'GET' && urlPath.startsWith('/media/')) {
    const id = urlPath.slice('/media/'.length);
    const m = mediaStore.get(id);
    if (!m) { res.writeHead(404); res.end('gone'); return; }
    m.expire = Date.now() + 15 * 60 * 1000; // 访问续期
    res.writeHead(200, { 'Content-Type': m.type, 'Cache-Control': 'no-store' });
    res.end(m.buf);
    return;
  }
  // ---- 静态文件 ----
  const filePath = path.join(PUBLIC_DIR, urlPath === '/' ? 'index.html' : urlPath);
  // 防目录穿越
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403); res.end('forbidden'); return;
  }
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('not found'); return; }
    const ext = path.extname(filePath);
    const headers = { 'Content-Type': MIME[ext] || 'application/octet-stream' };
    if (ext === '.html') headers['Cache-Control'] = 'no-store'; // 保证 App 总能拿到最新页面
    res.writeHead(200, headers);
    res.end(data);
  });
});

// ---------- WebSocket（零依赖实现，见 ws.js） ----------
const wss = { clients: new Set() }; // clients 集合供心跳遍历用
attachWebSocketServer(server, { path: '/ws' }, (ws) => {
  wss.clients.add(ws);
  ws.on('close', () => wss.clients.delete(ws));
  handleConnection(ws);
});
const users = new Map(); // 昵称 -> { conns: Set<ws>, offlineAt }  offlineAt 非空=全部连接断开、宽限期中
const history = [];      // 最近聊天记录缓存（解决刷新后记录丢失）
const MAX_HISTORY = 50;
const OFFLINE_GRACE = 60000; // 全部连接断开 60 秒内回来，不算"走了"，不打扰大家
let nextMsgId = 1;           // 聊天消息自增 ID（撤回用）

// 心跳：清理半开死连接（手机杀后台后 TCP 可能不断）
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.__alive === false) { ws.terminate(); return; }
    ws.__alive = false;
    ws.ping();
  });
}, 30000);

// 宽限期到点：连接全断真正下线的才广播"走了"
setInterval(() => {
  const now = Date.now();
  users.forEach((u, name) => {
    if (u.offlineAt && now - u.offlineAt > OFFLINE_GRACE) {
      users.delete(name);
      broadcast({ type: 'system', text: `${name} 走了`, time: nowTime() });
      broadcastUsers();
    }
  });
}, 10000);

function handleConnection(ws) {
  ws.__alive = true;
  ws.on('pong', () => { ws.__alive = true; });
  let myName = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    switch (msg.type) {
      case 'join': {
        const name = cleanName(msg.name);
        if (!name) { ws.close(); return; }
        // 换昵称：先从旧昵称的连接集合里摘掉自己
        if (myName && myName !== name) {
          const old = users.get(myName);
          if (old) {
            old.conns.delete(ws);
            if (old.conns.size === 0 && !old.offlineAt) {
              old.offlineAt = Date.now();
              broadcastUsers();
            }
          }
        }
        const prev = users.get(name);
        const wasOnline = !!(prev && prev.conns.size > 0 && !prev.offlineAt);
        const returning = wasOnline ||
            (!!prev && prev.offlineAt && Date.now() - prev.offlineAt < OFFLINE_GRACE);
        if (!prev) users.set(name, { conns: new Set(), offlineAt: null });
        const u = users.get(name);
        u.conns.add(ws);
        u.offlineAt = null;
        myName = name;
        ws.send(JSON.stringify({ type: 'joined', name }));
        // 下发最近的聊天记录，刷新页面不丢上下文
        if (history.length > 0) {
          ws.send(JSON.stringify({ type: 'history', list: history.slice(-MAX_HISTORY) }));
        }
        if (!returning) {
          broadcast({ type: 'system', text: `${name} 来了`, time: nowTime() });
        }
        broadcastUsers();
        break;
      }
      case 'chat': {
        const text = String(msg.text || '').slice(0, 500).trim();
        if (!text || !myName) break;
        const m = { type: 'chat', id: nextMsgId++, from: myName, text, time: nowTime() };
        history.push(m);
        if (history.length > MAX_HISTORY) history.shift();
        broadcast(m);
        break;
      }
      // 撤回：只允许撤自己发的（按 ID 匹配），服务端从历史里抹掉并广播
      case 'chat-recall': {
        const id = Number(msg.id);
        if (!id || !myName) break;
        const i = history.findIndex((h) => h.id === id && h.from === myName);
        if (i < 0) break; // 不是自己的或已不存在，静默忽略
        history.splice(i, 1);
        broadcast({ type: 'chat-recall', id });
        break;
      }
      // 图片消息：两种形态——
      //   a) 整条（旧客户端）：dataURL 直接转发（不进 history，体积大，刷新后消失）
      //   b) 短消息引用（新客户端）：先 HTTP 上传拿 id，这里只广播 {id,mime}，
      //      对端再 HTTP 取回。绕开隧道丢 WS 大帧的老问题
      case 'chat-img': {
        if (msg.id && msg.mime) {
          broadcast({ type: 'chat-img', id: String(msg.id).slice(0, 40), mime: String(msg.mime).slice(0, 40), from: myName, time: nowTime() });
          break;
        }
        const img = String(msg.img || '');
        if (!img.startsWith('data:image/') || img.length > 900 * 1024 || !myName) break;
        broadcast({ type: 'chat-img', from: myName, img, time: nowTime() });
        break;
      }
      // 语音消息：8kHz 16bit 单声道。同样支持整条(base64) 与短消息引用(id) 两种
      case 'chat-audio': {
        if (msg.id && msg.dur) {
          broadcast({ type: 'chat-audio', id: String(msg.id).slice(0, 40), dur: Math.max(1, Math.round(Number(msg.dur) || 1)), from: myName, time: nowTime() });
          break;
        }
        const a = String(msg.audio || '');
        if (!a || a.length > 600 * 1024 || !myName) break;
        const dur = Math.max(1, Math.round(Number(msg.dur) || 1));
        broadcast({ type: 'chat-audio', from: myName, audio: a, dur, time: nowTime() });
        break;
      }
      // 大载荷分片中继：免费隧道（cloudflared）会静默丢弃 >50KB 的 WS 帧
      // （实测 10KB 正常、100KB 必丢），图片/语音 base64 由发送端切成 ≤24KB 小帧，
      // 服务端不重组、原样广播，接收端按 seq 拼回——上下行都走小帧，绕开隧道限制
      case 'chunk': {
        const mid = String(msg.mid || '').slice(0, 40);
        const seq = Number(msg.seq), total = Number(msg.total);
        if (!mid || !Number.isInteger(seq) || !Number.isInteger(total) ||
            total < 1 || total > 2000 || seq < 0 || seq >= total || !myName) break;
        const data = String(msg.data || '');
        // 单帧净荷上限：免费隧道对 >5KB 的 WS 帧会静默丢弃（实测 5KB 过、10KB 丢），
        // 客户端已切成 ≤3KB，这里留余量卡在 5000 防滥用
        if (!data || data.length > 5000) break;
        broadcast({
          type: 'chunk', mid, seq, total,
          mtype: String(msg.mtype || '').slice(0, 20),
          from: myName, time: nowTime(),
          dur: (seq === 0 && msg.dur != null) ? Math.max(1, Math.round(Number(msg.dur) || 1)) : undefined,
          data,
        });
        break;
      }
      // ---- 通话信令，转发给目标的所有连接（原生服务 + 网页终端） ----
      case 'call-offer':
      case 'call-answer':
      case 'call-reject':
      case 'call-end':
      case 'video-toggle': // 视频通话开关通知（复用信令通道）
      case 'ice': {
        if (!myName) break;
        const target = users.get(cleanName(msg.to));
        const live = target && target.conns.size > 0 && !target.offlineAt;
        if (live) {
          const payload = JSON.stringify({ ...msg, from: myName });
          target.conns.forEach((c) => { if (c.readyState === 1) c.send(payload); });
        } else if (msg.type === 'call-offer') {
          // 对方不在线，明确告诉呼叫方
          ws.send(JSON.stringify({ type: 'call-fail', to: cleanName(msg.to), reason: 'offline' }));
        }
        break;
      }
    }
  });

  ws.on('close', () => {
    if (!myName) return;
    const u = users.get(myName);
    if (!u) return;
    u.conns.delete(ws);
    // 还有别的连接活着（比如 App 原生服务）→ 人还在线，不广播
    if (u.conns.size > 0) return;
    if (!u.offlineAt) {
      u.offlineAt = Date.now(); // 进入宽限期，60 秒后定时器才真正清
      broadcastUsers();
    }
  });
}

function broadcast(m) {
  const s = JSON.stringify(m);
  users.forEach((u) => {
    if (u.offlineAt) return;
    u.conns.forEach((c) => { if (c.readyState === 1) c.send(s); });
  });
}

function broadcastUsers() {
  broadcast({ type: 'users', list: [...users.keys()] });
}

function cleanName(n) {
  return String(n || '').replace(/[\s<>"']/g, '').slice(0, 20);
}

function nowTime() {
  const d = new Date();
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

server.listen(PORT, () => {
  console.log(`家庭聊天服务已启动: http://0.0.0.0:${PORT}`);
});
