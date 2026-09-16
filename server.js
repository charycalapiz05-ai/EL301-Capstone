/*
  EL301 Capstone – Centralized IoT Server
  ESP32 devices <-> this server <-> browser dashboard(s)

  Run locally:  node server.js
  Deploy free:  Render / Glitch / Railway (any Node host that gives you a
                public wss:// URL) so the Wokwi-simulated ESP32 can reach it.
*/

const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// Centralized in-memory store: latest reading per device+sensor
const latestState = {}; // { device_id: { sensor: {value, unit, timestamp, status} } }

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function validateMessage(obj) {
  // Basic schema/type checks – never trust incoming JSON blindly.
  if (typeof obj !== 'object' || obj === null) return 'not an object';
  if (!obj.type) return 'missing "type"';

  if (obj.type === 'sensor_data' || obj.type === 'alert') {
    if (!obj.device_id) return 'missing device_id';
    if (!obj.sensor) return 'missing sensor';
    if (obj.value === undefined || typeof obj.value !== 'number') return 'value must be a number';
    if (!obj.unit) return 'missing unit';
  } else if (obj.type === 'command') {
    if (!obj.action) return 'missing action';
  } else if (obj.type === 'ack') {
    // acks are informational, minimal validation
  } else {
    return `unknown type "${obj.type}"`;
  }
  return null; // valid
}

wss.on('connection', (ws, req) => {
  ws.role = 'unknown'; // becomes 'device' or 'dashboard' on first valid message
  log('Client connected from', req.socket.remoteAddress);

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      log('[REJECTED] Invalid JSON:', raw.toString());
      ws.send(JSON.stringify({ type: 'ack', result: 'rejected', reason: 'invalid_json' }));
      return;
    }

    const problem = validateMessage(msg);
    if (problem) {
      log('[REJECTED]', problem, msg);
      ws.send(JSON.stringify({ type: 'ack', result: 'rejected', reason: problem }));
      return;
    }

    // Identify role from message shape
    if (msg.type === 'sensor_data' || msg.type === 'alert') {
      ws.role = 'device';
      ws.deviceId = msg.device_id;

      latestState[msg.device_id] = latestState[msg.device_id] || {};
      latestState[msg.device_id][msg.sensor] = {
        value: msg.value,
        unit: msg.unit,
        status: msg.status,
        timestamp: msg.timestamp,
        type: msg.type,
      };

      log(`[${msg.type.toUpperCase()}] ${msg.device_id} ${msg.sensor}=${msg.value}${msg.unit}`);

      // Forward to every connected dashboard (centralized fan-out)
      broadcastToDashboards(msg);
    }

    if (msg.type === 'command') {
      ws.role = ws.role === 'unknown' ? 'dashboard' : ws.role;
      log('[COMMAND]', msg.action, 'requested for device', msg.device_id || '(broadcast)');
      forwardToDevices(msg);
    }

    if (msg.type === 'ack') {
      log('[ACK]', msg.device_id, msg.action, '->', msg.result);
      broadcastToDashboards(msg);
    }
  });

  ws.on('close', () => log('Client disconnected', ws.role, ws.deviceId || ''));
});

function broadcastToDashboards(msg) {
  const payload = JSON.stringify(msg);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN && client.role !== 'device') {
      client.send(payload);
    }
  });
}

function forwardToDevices(msg) {
  const payload = JSON.stringify(msg);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN && client.role === 'device') {
      client.send(payload);
    }
  });
}

// Simple REST-ish snapshot for debugging (optional, plain WS server has no HTTP routes
// beyond the upgrade handshake, so this just logs periodically instead)
setInterval(() => {
  log('Current centralized state:', JSON.stringify(latestState));
}, 30000);

log(`WebSocket server listening on port ${PORT}`);
