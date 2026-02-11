/**
 * Media Stream Handler
 * 
 * Handles bidirectional audio streaming with Twilio Media Streams.
 * Receives μ-law audio, buffers it, transforms via ElevenLabs, and sends back.
 */

import { AudioBuffer } from '../utils/audio-buffer.js';
import { mulawDecode, mulawEncode, resample } from '../utils/audio-codec.js';
import { LatencyTracker } from '../utils/latency-tracker.js';

// Configuration
const BUFFER_MS = parseInt(process.env.AUDIO_BUFFER_MS) || 500;
const SAMPLE_RATE_TWILIO = 8000;  // Twilio's μ-law sample rate
const SAMPLE_RATE_ELEVENLABS = 16000;  // ElevenLabs optimal input rate

/**
 * Handle a Twilio Media Stream WebSocket connection
 */
export function handleMediaStream(socket, options) {
  const { callManager, voiceTransformer, audioBridge, logger } = options;
  // callId and voicePreset may come from URL query (fallback) or Twilio 'start' customParameters (preferred)
  let callId = options.callId || 'unknown';
  let voicePreset = options.voicePreset || 'deep_male';

  logger.info(`Twilio Media Stream WebSocket CONNECTED (initial callId=${callId})`);

  // State
  let streamSid = null;
  let isConnected = false;
  
  // Note: The Twilio path receives audio FROM the callee. We do NOT transform it
  // or send it back — that would create echo. Voice transformation happens only on
  // the CLIENT path (handleClientAudioStream) which transforms the caller's voice
  // and forwards it to Twilio for the callee to hear.
  
  // Handle incoming WebSocket messages
  socket.on('message', async (rawData) => {
    try {
      const message = JSON.parse(rawData.toString());
      
      switch (message.event) {
        case 'connected':
          logger.info(`Twilio connected for call ${callId}`);
          isConnected = true;
          break;
          
        case 'start':
          streamSid = message.start.streamSid;
          const customParams = message.start.customParameters || {};

          // Update callId and voicePreset from Twilio's custom parameters (set via <Parameter> in TwiML)
          if (customParams.callId) callId = customParams.callId;
          if (customParams.voicePreset) voicePreset = customParams.voicePreset;

          logger.info(`Twilio Stream started: streamSid=${streamSid} callId=${callId} voice=${voicePreset}`);
          logger.info(`Custom params: ${JSON.stringify(customParams)}`);

          // Track active call now that we have the real callId
          callManager.addActiveStream(callId, {
            voicePreset,
            startTime: Date.now(),
          });

          // Initialize voice transformer for this stream
          await voiceTransformer.initializeStream(callId, voicePreset);

          // Ensure bridge exists (Twilio may connect before the client WebSocket)
          if (!audioBridge.getBridgeStatus(callId)) {
            audioBridge.createBridge(callId, voicePreset);
          }

          // Connect Twilio stream to audio bridge
          audioBridge.connectTwilioStream(callId, socket, streamSid);
          break;
          
        case 'media': {
          const clientRate = audioBridge.getClientSampleRate(callId);
          if (!clientRate) break;

          const mulawBytes = Buffer.from(message.media.payload, 'base64');
          const pcm8k = mulawDecode(mulawBytes);
          const pcmClient = resample(pcm8k, SAMPLE_RATE_TWILIO, clientRate);
          audioBridge.forwardAudioToClient(callId, pcmClient);
          break;
        }
          
        case 'mark':
          // Audio playback marker (optional tracking)
          logger.debug(`Mark received: ${message.mark.name}`);
          break;
          
        case 'stop':
          logger.info(`Twilio stream stopped for call ${callId}`);
          isConnected = false;
          callManager.removeActiveStream(callId);
          break;
          
        default:
          logger.debug(`Unknown event: ${message.event}`);
      }
      
    } catch (error) {
      logger.error(`WebSocket message error: ${error.message}`);
    }
  });
  
  socket.on('close', () => {
    logger.info(`Twilio WebSocket closed for call ${callId}`);
    isConnected = false;
    callManager.removeActiveStream(callId);
  });
  
  socket.on('error', (error) => {
    logger.error(`WebSocket error for call ${callId}: ${error.message}`);
  });
}

/**
 * Create a mark message for Twilio
 * Useful for tracking audio playback
 */
export function createMarkMessage(streamSid, name) {
  return JSON.stringify({
    event: 'mark',
    streamSid: streamSid,
    mark: { name },
  });
}

/**
 * Create a clear message to stop Twilio audio playback
 */
export function createClearMessage(streamSid) {
  return JSON.stringify({
    event: 'clear',
    streamSid: streamSid,
  });
}

/**
 * Handle Client Audio Stream WebSocket connection
 * Receives raw PCM Int16 audio from browser AudioWorklet, transforms the voice
 * via ElevenLabs, encodes to mu-law, and forwards to the Twilio call.
 */
export function handleClientAudioStream(socket, options) {
  const { callId, voicePreset, callManager, voiceTransformer, audioBridge, logger } = options;

  // State
  let clientSampleRate = null;
  let audioChunkCount = 0;
  let pcmAudioBuffer = null; // AudioBuffer for PCM at client sample rate
  let activeRequests = 0;
  const MAX_CONCURRENT_REQUESTS = 2; // ElevenLabs Starter plan allows 3 concurrent — leave 1 headroom
  const latencyTracker = new LatencyTracker();

  logger.info(`Client audio stream started for call ${callId}, voice preset: ${voicePreset}`);

  // Create or get audio bridge for this call
  if (!audioBridge.getBridgeStatus(callId)) {
    audioBridge.createBridge(callId, voicePreset);
  }

  // Connect client stream to audio bridge
  audioBridge.connectClientStream(callId, socket);

  // Track active client stream
  callManager.addClientStream(callId, {
    voicePreset,
    startTime: Date.now(),
  });

  // Initialize voice transformer stream for this call
  voiceTransformer.initializeStream(callId, voicePreset).catch(err => {
    logger.error(`Failed to initialize voice transformer for call ${callId}: ${err.message}`);
  });

  // Size of each Twilio-bound chunk: 160ms of 16kHz 16-bit PCM = 5120 bytes
  // After resample to 8kHz mulaw this becomes ~1280 bytes (~160ms of audio)
  const STREAM_CHUNK_BYTES = 5120;

  // Process buffered PCM: resample -> stream transform -> resample -> mulaw -> Twilio
  async function processClientBuffer() {
    if (!pcmAudioBuffer) return;

    // Don't send to ElevenLabs if Twilio isn't connected yet (call still ringing)
    const bridgeStatus = audioBridge.getBridgeStatus(callId);
    if (!bridgeStatus || !bridgeStatus.twilioConnected) {
      pcmAudioBuffer.flush(); // Discard buffered audio — nowhere to send it
      return;
    }

    // Bounded concurrency: allow up to MAX_CONCURRENT_REQUESTS in-flight
    if (activeRequests >= MAX_CONCURRENT_REQUESTS) return;
    activeRequests++;

    const pcmChunk = pcmAudioBuffer.flush();
    if (!pcmChunk || pcmChunk.length === 0) {
      activeRequests--;
      return;
    }

    const startTime = Date.now();
    let firstChunkTime = null;

    try {
      // 1. Resample from client sample rate to 16kHz for ElevenLabs
      const pcm16k = resample(pcmChunk, clientSampleRate, SAMPLE_RATE_ELEVENLABS);

      if (audioChunkCount <= 5) {
        logger.info(`Pipeline[${callId}]: input=${pcmChunk.length}B@${clientSampleRate}Hz -> resampled=${pcm16k.length}B@${SAMPLE_RATE_ELEVENLABS}Hz`);
      }

      // 2. Stream-transform voice via ElevenLabs
      // Accumulate streamed PCM and forward to Twilio in ~160ms chunks
      let accumulator = Buffer.alloc(0);
      let chunksSent = 0;

      await voiceTransformer.transformStream(pcm16k, voicePreset, {
        sampleRate: SAMPLE_RATE_ELEVENLABS,
      }, (streamedPcm) => {
        if (!firstChunkTime) firstChunkTime = Date.now();

        accumulator = Buffer.concat([accumulator, streamedPcm]);

        // Forward complete ~160ms chunks as they accumulate
        while (accumulator.length >= STREAM_CHUNK_BYTES) {
          const slice = accumulator.subarray(0, STREAM_CHUNK_BYTES);
          accumulator = accumulator.subarray(STREAM_CHUNK_BYTES);

          // 3. Resample 16kHz -> 8kHz, encode to mu-law, forward to Twilio
          const pcm8k = resample(Buffer.from(slice), SAMPLE_RATE_ELEVENLABS, SAMPLE_RATE_TWILIO);
          const mulawOutput = mulawEncode(pcm8k);
          audioBridge.forwardAudioToTwilio(callId, mulawOutput);
          chunksSent++;
        }
      });

      // Flush any remaining accumulated PCM
      if (accumulator.length > 0) {
        const pcm8k = resample(accumulator, SAMPLE_RATE_ELEVENLABS, SAMPLE_RATE_TWILIO);
        const mulawOutput = mulawEncode(pcm8k);
        audioBridge.forwardAudioToTwilio(callId, mulawOutput);
        chunksSent++;
      }

      // Track latency
      const processingTime = Date.now() - startTime;
      const timeToFirst = firstChunkTime ? firstChunkTime - startTime : processingTime;
      latencyTracker.record(processingTime);

      if (audioChunkCount <= 5) {
        logger.info(`Pipeline[${callId}]: streamed ${chunksSent} chunks, first-chunk=${timeToFirst}ms, total=${processingTime}ms`);
      }

      if (timeToFirst > 1000) {
        logger.warn(`Slow first-chunk latency: ${timeToFirst}ms for call ${callId}`);
      }

    } catch (error) {
      logger.error(`Client audio transform error for call ${callId}: ${error.message}`);
    } finally {
      activeRequests--;
    }
  }

  // Handle incoming messages from browser
  socket.on('message', async (data) => {
    try {
      // First message should be a JSON config with the client's sample rate
      if (!clientSampleRate) {
        try {
          const config = JSON.parse(data.toString());
          if (config.type === 'config' && config.sampleRate) {
            clientSampleRate = config.sampleRate;
            // PCM Int16 = 2 bytes per sample, so bytesPerMs = sampleRate * 2 / 1000
            // AudioBuffer expects bytesPerMs as sampleRate/1000 (for 1-byte-per-sample formats)
            // For 16-bit PCM, we create the buffer with sampleRate*2 to account for 2 bytes/sample
            pcmAudioBuffer = new AudioBuffer(BUFFER_MS, clientSampleRate * 2);
            audioBridge.setClientSampleRate(callId, clientSampleRate);
            logger.info(`Client audio config: sampleRate=${clientSampleRate}, channels=${config.channels}, encoding=${config.encoding} for call ${callId}`);
            return;
          }
        } catch {
          // Not JSON - could be binary PCM without config, default to 48kHz
          clientSampleRate = 48000;
          pcmAudioBuffer = new AudioBuffer(BUFFER_MS, clientSampleRate * 2);
          audioBridge.setClientSampleRate(callId, clientSampleRate);
          logger.warn(`No config message received, defaulting to ${clientSampleRate}Hz for call ${callId}`);
        }
      }

      // Subsequent messages are raw PCM Int16 binary data
      const audioData = Buffer.isBuffer(data) ? data : Buffer.from(data);
      audioChunkCount++;

      if (audioChunkCount <= 3) {
        logger.info(`Client audio chunk #${audioChunkCount}: ${audioData.length} bytes for call ${callId}`);
      }

      // Buffer the PCM data
      pcmAudioBuffer.push(audioData);

      // Process when buffer has accumulated enough data (~500ms)
      if (pcmAudioBuffer.isReady()) {
        processClientBuffer().catch(err => {
          logger.error(`Async client audio processing error: ${err.message}`);
        });
      }

    } catch (error) {
      logger.error(`Client audio message error for call ${callId}: ${error.message}`);
    }
  });

  socket.on('open', () => {
    logger.info(`Client audio stream connected for call ${callId}`);
  });

  socket.on('close', () => {
    logger.info(`Client audio stream closed for call ${callId}`);

    // Log metrics
    const metrics = latencyTracker.getMetrics();
    if (metrics.count > 0) {
      logger.info(`Client audio metrics for ${callId}: avg=${metrics.average}ms, max=${metrics.max}ms, count=${metrics.count}`);
    }

    // Cleanup voice transformer stream
    voiceTransformer.closeStream(callId).catch(() => {});

    // Remove bridge
    audioBridge.removeBridge(callId);

    callManager.removeClientStream?.(callId);
  });

  socket.on('error', (error) => {
    logger.error(`Client audio stream error for call ${callId}: ${error.message}`);
  });
}
