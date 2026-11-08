const http = require('http');
const net = require('net');
const { WebSocketServer } = require('ws');
const UUID = (process.env.UUID || '').replace(/-/g, '');
const WSPATH = process.env.WSPATH || '/ws';
const PORT = parseInt(process.env.PORT || '8080', 10);
if (UUID.length !== 32) { console.error('UUID env required'); process.exit(1); }
const uuidBuf = Buffer.from(UUID, 'hex');
const server = http.createServer((req, res) => { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); });
const wss = new WebSocketServer({ server, path: WSPATH });
wss.on('connection', (ws) => {
  let socket = null, buf = Buffer.alloc(0), replied = false;
  ws.on('message', (data) => {
    const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (socket) { socket.write(chunk); return; }
    buf = Buffer.concat([buf, chunk]);
    try {
      if (buf.length < 18) return;
      if (!buf.subarray(1, 17).equals(uuidBuf)) { ws.close(); return; }
      let idx = 17;
      const optLen = buf[idx]; idx += 1 + optLen;
      if (buf.length < idx + 4) return;
      const cmd = buf[idx]; idx += 1;
      if (cmd !== 1) { ws.close(); return; }
      const port = buf.readUInt16BE(idx); idx += 2;
      const atype = buf[idx]; idx += 1;
      let host;
      if (atype === 1) { if (buf.length < idx + 4) return; host = Array.from(buf.subarray(idx, idx + 4)).join('.'); idx += 4; }
      else if (atype === 2) { if (buf.length < idx + 1) return; const len = buf[idx]; idx += 1; if (buf.length < idx + len) return; host = buf.subarray(idx, idx + len).toString(); idx += len; }
      else if (atype === 3) { if (buf.length < idx + 16) return; const b = buf.subarray(idx, idx + 16); const parts = []; for (let i = 0; i < 16; i += 2) parts.push(b.readUInt16BE(i).toString(16)); host = parts.join(':'); idx += 16; }
      else { ws.close(); return; }
      const payload = buf.subarray(idx);
      socket = net.connect(port, host, () => { if (payload.length) socket.write(payload); });
      socket.on('data', (d) => {
        if (!replied) { replied = true; d = Buffer.concat([Buffer.from([0, 0]), d]); }
        if (ws.readyState === 1) ws.send(d);
      });
      socket.on('error', () => { try { ws.close(); } catch (e) {} });
      socket.on('close', () => { try { ws.close(); } catch (e) {} });
    } catch (e) { try { ws.close(); } catch (_) {} }
  });
  ws.on('close', () => { if (socket) socket.destroy(); });
  ws.on('error', () => { if (socket) socket.destroy(); });
});
server.listen(PORT, '0.0.0.0', () => console.log('listening', PORT));
