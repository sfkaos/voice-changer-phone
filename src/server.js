/**
 * Voice Changer Phone System - Main Server
 * 
 * Handles Twilio Media Streams and voice transformation via ElevenLabs.
 * 
 * Flow:
 * 1. Twilio call connects via WebSocket
 * 2. Audio chunks (μ-law 8kHz) received from Twilio
 * 3. Buffered, converted to PCM, sent to ElevenLabs STS
 * 4. Transformed audio converted back to μ-law, sent to Twilio
 */

import 'dotenv/config';
import Fastify from 'fastify';
import websocket from '@fastify/websocket';
import fastifyStatic from '@fastify/static';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocket } from 'ws';

import { handleMediaStream, handleClientAudioStream } from './services/media-stream.js';
import { CallManager } from './services/call-manager.js';
import { VoiceTransformer } from './services/voice-transformer.js';
import { AudioBridge } from './services/audio-bridge.js';
import { createLogger } from './utils/logger.js';
import { VOICE_PRESETS } from './utils/voice-presets.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Create logger
const logger = createLogger('server');

// Initialize Fastify
const fastify = Fastify({
  logger: false, // Using custom logger
});

// Register plugins
await fastify.register(websocket);
await fastify.register(fastifyStatic, {
  root: path.join(__dirname, '..', 'public'),
  prefix: '/',
});

// Register form body parser for Twilio webhooks
await fastify.register(import('@fastify/formbody'));

// Initialize services
const callManager = new CallManager();
const voiceTransformer = new VoiceTransformer();
const audioBridge = new AudioBridge(voiceTransformer);

// ============================================
// WebSocket Routes
// ============================================

// Log all incoming requests to detect Twilio WebSocket upgrade attempts
fastify.addHook('onRequest', (request, reply, done) => {
  if (request.url.startsWith('/media-stream')) {
    logger.info(`>>> HTTP request to /media-stream: method=${request.method} upgrade=${request.headers.upgrade || 'none'} url=${request.url} ip=${request.headers['x-forwarded-for'] || request.ip} ua=${request.headers['user-agent']}`);
  }
  done();
});

fastify.register(async function (fastify) {
  // Twilio Media Streams (receives audio from called person)
  fastify.get('/media-stream', { websocket: true }, (socket, req) => {
    // Read params from URL query string as fallback; primary source is Twilio's 'start' customParameters
    const params = new URL(req.url, `http://${req.headers.host}`).searchParams;
    const voicePreset = params.get('voicePreset') || 'deep_male';
    const callId = params.get('callId') || 'unknown';

    logger.info(`TWILIO Media Stream WebSocket CONNECTED`);
    logger.info(`   Call ID: ${callId}`);
    logger.info(`   Voice Preset: ${voicePreset}`);
    logger.info(`   Client IP: ${req.headers['x-forwarded-for'] || req.ip}`);
    logger.info(`   URL: ${req.url}`);

    handleMediaStream(socket, {
      callId,
      voicePreset,
      callManager,
      voiceTransformer,
      audioBridge,
      logger,
    });
  });
  
  // Client Audio Stream (receives microphone audio from browser)
  fastify.get('/client-audio-stream', { websocket: true }, (socket, req) => {
    const params = new URL(req.url, `http://${req.headers.host}`).searchParams;
    const callId = params.get('callId') || 'unknown';
    const voicePreset = params.get('voice') || 'deep_male';
    
    logger.info(`🎤 Client Audio Stream WebSocket CONNECTED`);
    logger.info(`   Call ID: ${callId}`);
    logger.info(`   Voice Preset: ${voicePreset}`);
    logger.info(`   Client IP: ${req.headers['x-forwarded-for'] || req.ip}`);
    
    handleClientAudioStream(socket, {
      callId,
      voicePreset,
      callManager,
      voiceTransformer,
      audioBridge,
      logger,
    });
  });
});

// ============================================
// HTTP Routes - Twilio Webhooks
// ============================================

/**
 * POST /voice - Initial TwiML for incoming/outgoing calls
 * Returns TwiML that sets up bidirectional Media Stream
 */
fastify.post('/voice', async (request, reply) => {
  const { voicePreset = 'deep_male', toNumber } = request.body || {};
  const callId = request.body?.CallSid || `call_${Date.now()}`;
  
  logger.info(`🔥 POST /voice webhook called!`);
  logger.info(`   CallSid: ${callId}`);
  logger.info(`   Voice: ${voicePreset}`);
  logger.info(`   Headers: ${JSON.stringify(request.headers)}`);
  logger.info(`   Body: ${JSON.stringify(request.body)}`);
  
  // Build TwiML response
  const host = process.env.SERVER_URL?.replace(/^https?:\/\//, '') || request.headers.host;
  // Use clean URL without query params — params are passed via <Parameter> elements
  const wsUrl = `wss://${host}/media-stream`;

  // TwiML for outbound call with voice transformation
  const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="voicePreset" value="${voicePreset}" />
      <Parameter name="callId" value="${callId}" />
    </Stream>
  </Connect>
</Response>`;

  logger.info(`Generated TwiML: ${twiml}`);
  reply.type('text/xml').send(twiml);
});

/**
 * GET /voice - Test route for browser testing
 */
fastify.get('/voice', async (request, reply) => {
  const voicePreset = request.query.voicePreset || 'deep_male';
  logger.info(`TwiML test request - Voice: ${voicePreset}`);
  
  const host = process.env.SERVER_URL?.replace(/^https?:\/\//, '') || request.headers.host;
  const wsUrl = `wss://${host}/media-stream`;

  const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="voicePreset" value="${voicePreset}" />
      <Parameter name="callId" value="test123" />
    </Stream>
  </Connect>
</Response>`;

  reply.type('text/xml').send(twiml);
});

/**
 * POST /voice/outbound - TwiML for the outbound leg (recipient side)
 */
fastify.post('/voice/outbound', async (request, reply) => {
  const voicePreset = request.query.voicePreset || 'deep_male';
  const callId = request.body?.CallSid || `call_${Date.now()}`;
  
  logger.info(`Outbound TwiML - CallSid: ${callId}`);
  
  const host = process.env.SERVER_URL?.replace(/^https?:\/\//, '') || request.headers.host;
  const wsUrl = `wss://${host}/media-stream?voicePreset=${voicePreset}&amp;callId=${callId}&amp;direction=outbound`;
  
  const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${wsUrl}" track="inbound_track">
      <Parameter name="voicePreset" value="${voicePreset}" />
      <Parameter name="direction" value="outbound" />
    </Stream>
  </Connect>
  <Pause length="3600" />
</Response>`;

  reply.type('text/xml').send(twiml);
});

/**
 * POST /call-status - Call status webhook
 */
fastify.post('/call-status', async (request, reply) => {
  const { CallSid, CallStatus, CallDuration, From, To } = request.body || {};
  
  logger.info(`📞 Call Status Webhook: ${CallSid} - ${CallStatus} (${CallDuration}s)`);
  logger.info(`   From: ${From}, To: ${To}`);
  logger.info(`   Status Body: ${JSON.stringify(request.body)}`);
  
  // Track call metrics
  if (CallStatus === 'completed') {
    callManager.recordCallEnd(CallSid, {
      duration: parseInt(CallDuration) || 0,
      from: From,
      to: To,
    });
  }
  
  reply.send({ received: true });
});

// ============================================
// API Routes - Call Control
// ============================================

/**
 * POST /api/call - Initiate an outbound call
 */
fastify.post('/api/call', async (request, reply) => {
  const { phone, voice = 'deep_male' } = request.body;
  
  if (!phone) {
    return reply.status(400).send({ success: false, error: 'Phone number required' });
  }
  
  try {
    const result = await callManager.initiateCall(phone, voice);
    logger.info(`Call initiated: ${result.callSid} to ${phone} with voice ${voice}`);
    
    reply.send({
      success: true,
      callSid: result.callSid,
      voice: voice,
      to: phone,
    });
  } catch (error) {
    logger.error(`Call initiation failed: ${error.message}`);
    reply.status(500).send({ success: false, error: error.message });
  }
});

/**
 * GET /api/call/:callSid - Get call status
 */
fastify.get('/api/call/:callSid', async (request, reply) => {
  const { callSid } = request.params;
  
  try {
    const status = await callManager.getCallStatus(callSid);
    reply.send({ success: true, status });
  } catch (error) {
    reply.status(500).send({ success: false, error: error.message });
  }
});

/**
 * DELETE /api/call/:callSid - End a call
 */
fastify.delete('/api/call/:callSid', async (request, reply) => {
  const { callSid } = request.params;
  
  try {
    await callManager.endCall(callSid);
    reply.send({ success: true, message: 'Call ended' });
  } catch (error) {
    reply.status(500).send({ success: false, error: error.message });
  }
});

/**
 * GET /api/voices - Get available voice presets
 */
fastify.get('/api/voices', async (request, reply) => {
  reply.send({
    success: true,
    voices: Object.entries(VOICE_PRESETS).map(([id, info]) => ({
      id,
      ...info,
    })),
  });
});

/**
 * GET /api/stats - Get usage statistics
 */
fastify.get('/api/stats', async (request, reply) => {
  const stats = callManager.getStats();
  reply.send({ success: true, stats });
});

/**
 * GET /api/health - Health check
 */
fastify.get('/api/health', async (request, reply) => {
  const health = {
    status: 'ok',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    services: {
      twilio: !!process.env.TWILIO_ACCOUNT_SID,
      elevenlabs: !!process.env.ELEVENLABS_API_KEY,
    },
    audioBridges: audioBridge.getActiveBridges(),
  };
  
  reply.send(health);
});

/**
 * GET /api/debug/bridges - Audio bridge debugging
 */
fastify.get('/api/debug/bridges', async (request, reply) => {
  reply.send({
    success: true,
    bridges: audioBridge.getActiveBridges(),
    timestamp: new Date().toISOString(),
  });
});

/**
 * GET /test-ws - Test WebSocket connectivity
 */
fastify.get('/test-ws', async (request, reply) => {
  const wsTestHtml = `
<!DOCTYPE html>
<html>
<head><title>WebSocket Test</title></head>
<body>
  <h1>WebSocket Connection Test</h1>
  <div id="status">Connecting...</div>
  <div id="log"></div>
  <script>
    const log = document.getElementById('log');
    const status = document.getElementById('status');
    
    try {
      const ws = new WebSocket('wss://web-production-9321.up.railway.app/media-stream?test=true');
      
      ws.onopen = function() {
        status.textContent = '✅ WebSocket Connected!';
        log.innerHTML += '<br>✅ Connection successful';
      };
      
      ws.onerror = function(error) {
        status.textContent = '❌ WebSocket Error';
        log.innerHTML += '<br>❌ Error: ' + error;
      };
      
      ws.onclose = function() {
        status.textContent = '⚠️ WebSocket Disconnected';
        log.innerHTML += '<br>⚠️ Connection closed';
      };
    } catch(e) {
      status.textContent = '❌ WebSocket Failed';
      log.innerHTML = '❌ Exception: ' + e.message;
    }
  </script>
</body>
</html>`;
  
  reply.type('text/html').send(wsTestHtml);
});

// ============================================
// Start Server
// ============================================
const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';

try {
  await fastify.listen({ port: PORT, host: HOST });
  logger.info(`🎭 Voice Changer Phone Server running on ${HOST}:${PORT}`);
  
  // Fix WebSocket URL - strip https:// from SERVER_URL  
  let serverUrl = process.env.SERVER_URL || `localhost:${PORT}`;
  if (serverUrl.startsWith('http://') || serverUrl.startsWith('https://')) {
    serverUrl = serverUrl.replace(/^https?:\/\//, '');
  }
  logger.info(`   WebSocket: wss://${serverUrl}/media-stream`);
  logger.info(`   Web UI: http://localhost:${PORT}`);
  
  // Log configuration status
  logger.info(`   Twilio: ${process.env.TWILIO_ACCOUNT_SID ? '✅ Configured' : '❌ Missing'}`);
  logger.info(`   ElevenLabs: ${process.env.ELEVENLABS_API_KEY ? '✅ Configured' : '❌ Missing'}`);
} catch (err) {
  logger.error(`Failed to start server: ${err.message}`);
  process.exit(1);
}

// Graceful shutdown
process.on('SIGINT', async () => {
  logger.info('Shutting down...');
  await fastify.close();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  logger.info('Shutting down...');
  await fastify.close();
  process.exit(0);
});
