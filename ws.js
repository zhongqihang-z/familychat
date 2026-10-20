// ws.js — 零依赖 WebSocket 服务端（只用 Node 内置模块）
// 目的：部署到云托管/Docker 时不需要 npm install，构建零风险。
// 用法：
//   const { attachWebSocketServer } = require('./ws');
//   attachWebSocketServer(httpServer, { path: '/ws' }, (conn) => {
//     conn.on('message', (data) => { ... });   // data: Buffer
//     conn.on('close', () => { ... });
//     conn.on('pong', () => { ... });
//     conn.send('text 或 Buffer'); conn.ping(); conn.close(1000); conn.terminate();
//     conn.readyState === 1
//   });

const crypto = require('crypto');
const { EventEmitter } = require('events');

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_PAYLOAD = 8 * 1024 * 1024; // 单帧净荷上限 8MB，超过直接断

function attachWebSocketServer(server, opts, onConnection) {
  const path = (opts && opts.path) || '/ws';
  server.on('upgrade', (req, socket, head) => {
    const pathname = (req.url || '/').split('?')[0];
    if (pathname !== path) { socket.destroy(); return; }
    const key = req.headers['sec-websocket-key'];
    if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') {
      socket.destroy(); return;
    }
    const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
    socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
    );
    if (socket.writable) socket.setNoDelay(true);
    const conn = createConn(socket);
    if (head && head.length) conn._feed(head);
    onConnection(conn, req);
  });
  return server;
}

function createConn(socket) {
  const conn = new EventEmitter();
  conn.readyState = 1;
  let buf = Buffer.alloc(0);
  let frags = [];

  function closeConn(code, reason) {
    if (conn.readyState !== 1) return;
    conn.readyState = 3;
    try { socket.end(); } catch (e) { /* ignore */ }
    try { socket.destroy(); } catch (e) { /* ignore */ }
    conn.emit('close', code, reason);
  }
  conn._closeSocket = closeConn;

  function sendFrame(opcode, payload) {
    if (conn.readyState !== 1) return false;
    const len = payload.length;
    let header;
    if (len < 126) {
      header = Buffer.alloc(2);
      header[1] = len;
    } else if (len < 65536) {
      header = Buffer.alloc(4);
      header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[1] = 127;
      header.writeBigUInt64BE(BigInt(len), 2);
    }
    header[0] = 0x80 | opcode; // FIN=1
    try {
      socket.write(Buffer.concat([header, payload]));
      return true;
    } catch (e) { closeConn(1006, 'write error'); return false; }
  }
  conn._sendFrame = sendFrame;

  function handleFrame(fin, opcode, payload) {
    if (opcode === 0x8) { // close
      const code = payload.length >= 2 ? payload.readUInt16BE(0) : 1005;
      if (payload.length >= 2) sendFrame(0x8, payload.slice(0, 2));
      closeConn(1000, 'peer closed ' + code);
      return;
    }
    if (opcode === 0x9) { sendFrame(0xA, payload); return; } // ping → pong
    if (opcode === 0xA) { conn.emit('pong', payload); return; }
    if (opcode === 0x1 || opcode === 0x2) { // text / binary 起始帧
      if (fin) { conn.emit('message', payload); return; }
      frags = [payload];
      return;
    }
    if (opcode === 0x0) { // continuation
      frags.push(payload);
      if (fin) { const m = Buffer.concat(frags); frags = []; conn.emit('message', m); }
      return;
    }
    // 未知 opcode：忽略
  }

  conn._feed = (chunk) => {
    buf = buf.length ? Buffer.concat([buf, chunk]) : chunk;
    for (;;) {
      if (buf.length < 2) return;
      const fin = (buf[0] & 0x80) !== 0;
      const opcode = buf[0] & 0x0f;
      const masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f;
      let offset = 2;
      if (len === 126) {
        if (buf.length < 4) return;
        len = buf.readUInt16BE(2); offset = 4;
      } else if (len === 127) {
        if (buf.length < 10) return;
        const big = buf.readBigUInt64BE(2);
        if (big > BigInt(MAX_PAYLOAD)) { closeConn(1009, 'too big'); return; }
        len = Number(big); offset = 10;
      }
      let maskKey = null;
      if (masked) {
        if (buf.length < offset + 4) return;
        maskKey = buf.slice(offset, offset + 4); offset += 4;
      }
      if (len > MAX_PAYLOAD) { closeConn(1009, 'too big'); return; }
      if (buf.length < offset + len) return; // 等更多数据
      let payload = buf.slice(offset, offset + len);
      if (maskKey) {
        payload = Buffer.from(payload); // 拷贝，避免污染后续数据
        for (let i = 0; i < payload.length; i++) payload[i] ^= maskKey[i & 3];
      }
      buf = buf.slice(offset + len);
      handleFrame(fin, opcode, payload);
      if (conn.readyState !== 1) return;
    }
  };

  socket.on('data', (c) => { try { conn._feed(c); } catch (e) { closeConn(1007, 'parse error'); } });
  socket.on('error', () => closeConn(1006, 'socket error'));
  socket.on('close', () => closeConn(1006, 'tcp closed'));
  socket.on('end', () => closeConn(1006, 'tcp end'));

  conn.send = (data) => sendFrame(0x1, Buffer.isBuffer(data) ? data : Buffer.from(String(data)));
  conn.ping = () => sendFrame(0x9, Buffer.alloc(0));
  conn.pong = (data) => sendFrame(0xA, data ? Buffer.from(data) : Buffer.alloc(0));
  conn.close = (code) => {
    if (code && code >= 1000 && code <= 4999) sendFrame(0x8, Buffer.from([(code >> 8) & 0xff, code & 0xff]));
    else sendFrame(0x8, Buffer.alloc(0));
    setTimeout(() => closeConn(1000, 'local close'), 50);
  };
  conn.terminate = () => closeConn(1006, 'terminated');
  return conn;
}

module.exports = { attachWebSocketServer };
