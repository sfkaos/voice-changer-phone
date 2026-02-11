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
const BUFFER_MS = parseInt(process.env.AUDIO_BUFFER_MS) || 200;
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
  const audioBuffer = new AudioBuffer(BUFFER_MS, SAMPLE_RATE_TWILIO);
  const latencyTracker = new LatencyTracker();
  
  // Active stream tracking is deferred to 'start' event when we have the real callId
  
  // Process buffered audio
  async function processAudioBuffer() {
    if (!isConnected || !streamSid) return;
    
    const chunk = audioBuffer.flush();
    if (!chunk || chunk.length === 0) return;
    
    const startTime = Date.now();
    
    try {
      // 1. Decode μ-law to PCM (16-bit signed)
      const pcmBuffer = mulawDecode(chunk);
      
      // 2. Resample from 8kHz to 16kHz for ElevenLabs
      const resampledPcm = resample(pcmBuffer, SAMPLE_RATE_TWILIO, SAMPLE_RATE_ELEVENLABS);
      
      // 3. Transform voice via ElevenLabs
      const transformedPcm = await voiceTransformer.transform(resampledPcm, voicePreset, {
        sampleRate: SAMPLE_RATE_ELEVENLABS,
      });
      
      if (!transformedPcm || transformedPcm.length === 0) {
        logger.warn(`Empty transformation result for call ${callId}`);
        return;
      }
      
      // 4. Resample back to 8kHz for Twilio
      const outputPcm = resample(transformedPcm, SAMPLE_RATE_ELEVENLABS, SAMPLE_RATE_TWILIO);
      
      // 5. Encode to μ-law
      const mulawOutput = mulawEncode(outputPcm);
      
      // 6. Send back to Twilio
      const mediaMessage = {
        event: 'media',
        streamSid: streamSid,
        media: {
          payload: mulawOutput.toString('base64'),
        },
      };
      
      if (socket.readyState === socket.OPEN) {
        socket.send(JSON.stringify(mediaMessage));
      }
      
      // Track latency
      const processingTime = Date.now() - startTime;
      latencyTracker.record(processingTime);
      
      if (processingTime > 400) {
        logger.warn(`High latency detected: ${processingTime}ms for call ${callId}`);
      }
      
    } catch (error) {
      logger.error(`Audio processing error for call ${callId}: ${error.message}`);
      // Don't crash - just skip this chunk
    }
  }
  
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
          
        case 'media':
          // Receive audio chunk from Twilio
          const payload = Buffer.from(message.media.payload, 'base64');
          audioBuffer.push(payload);
          
          // Process when buffer is ready
          if (audioBuffer.isReady()) {
            // Don't await - process asynchronously to avoid blocking
            processAudioBuffer().catch(err => {
              logger.error(`Async processing error: ${err.message}`);
            });
          }
          break;
          
        case 'mark':
          // Audio playback marker (optional tracking)
          logger.debug(`Mark received: ${message.mark.name}`);
          break;
          
        case 'stop':
          logger.info(`Stream stopped for call ${callId}`);
          isConnected = false;
          
          // Cleanup
          await voiceTransformer.closeStream(callId);
          
          // Remove audio bridge
          audioBridge.removeBridge(callId);
          
          // Log metrics
          const metrics = latencyTracker.getMetrics();
          logger.info(`Call ${callId} metrics: avg=${metrics.average}ms, max=${metrics.max}ms, min=${metrics.min}ms`);
          
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
    logger.info(`WebSocket closed for call ${callId}`);
    isConnected = false;
    voiceTransformer.closeStream(callId).catch(() => {});
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
  let isProcessing = false;  // Serialize ElevenLabs requests (max 3 concurrent on Starter plan)
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

  // Process buffered PCM: resample -> transform -> resample -> mulaw -> Twilio
  async function processClientBuffer() {
    if (!pcmAudioBuffer) return;

    // Don't send to ElevenLabs if Twilio isn't connected yet (call still ringing)
    const bridgeStatus = audioBridge.getBridgeStatus(callId);
    if (!bridgeStatus || !bridgeStatus.twilioConnected) {
      pcmAudioBuffer.flush(); // Discard buffered audio — nowhere to send it
      return;
    }

    // Serialize: only one ElevenLabs request at a time to avoid 429 rate limits
    if (isProcessing) return;
    isProcessing = true;

    const pcmChunk = pcmAudioBuffer.flush();
    if (!pcmChunk || pcmChunk.length === 0) {
      isProcessing = false;
      return;
    }

    const startTime = Date.now();

    try {
      // 1. Resample from client sample rate to 16kHz for ElevenLabs
      const pcm16k = resample(pcmChunk, clientSampleRate, SAMPLE_RATE_ELEVENLABS);

      // 2. Transform voice via ElevenLabs (the core feature)
      const transformedPcm = await voiceTransformer.transform(pcm16k, voicePreset, {
        sampleRate: SAMPLE_RATE_ELEVENLABS,
      });

      if (!transformedPcm || transformedPcm.length === 0) {
        logger.warn(`Empty transformation result for call ${callId}`);
        return;
      }

      // 3. Resample transformed audio from 16kHz to 8kHz for Twilio
      const pcm8k = resample(transformedPcm, SAMPLE_RATE_ELEVENLABS, SAMPLE_RATE_TWILIO);

      // 4. Encode to mu-law
      const mulawOutput = mulawEncode(pcm8k);

      // 5. Forward to Twilio via the audio bridge
      audioBridge.forwardAudioToTwilio(callId, mulawOutput);

      // Track latency
      const processingTime = Date.now() - startTime;
      latencyTracker.record(processingTime);

      if (processingTime > 400) {
        logger.warn(`High client audio latency: ${processingTime}ms for call ${callId}`);
      }

    } catch (error) {
      logger.error(`Client audio transform error for call ${callId}: ${error.message}`);
    } finally {
      isProcessing = false;
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
            logger.info(`Client audio config: sampleRate=${clientSampleRate}, channels=${config.channels}, encoding=${config.encoding} for call ${callId}`);
            return;
          }
        } catch {
          // Not JSON - could be binary PCM without config, default to 48kHz
          clientSampleRate = 48000;
          pcmAudioBuffer = new AudioBuffer(BUFFER_MS, clientSampleRate * 2);
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

      // Process when buffer has accumulated enough data (~200ms)
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
