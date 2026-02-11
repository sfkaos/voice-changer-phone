/**
 * Audio Bridge Service
 * 
 * Connects client audio stream (microphone) to Twilio Media Stream (call)
 * Handles voice transformation pipeline between the two streams
 */

import { createLogger } from '../utils/logger.js';

const logger = createLogger('audio-bridge');

export class AudioBridge {
  constructor(voiceTransformer) {
    this.voiceTransformer = voiceTransformer;
    
    // Track active bridges: callId -> { clientStream, twilioStream }
    this.activeBridges = new Map();
    
    logger.info('AudioBridge service initialized');
  }
  
  /**
   * Create a bridge between client audio stream and Twilio call
   */
  createBridge(callId, voicePreset) {
    const bridge = {
      callId,
      voicePreset,
      clientStream: null,
      twilioStream: null,
      twilioStreamSid: null,
      audioQueue: [],
      isProcessing: false,
      startTime: Date.now()
    };
    
    this.activeBridges.set(callId, bridge);
    logger.info(`🌉 Audio bridge created for call ${callId} with voice ${voicePreset}`);
    
    return bridge;
  }
  
  /**
   * Connect client audio stream to bridge
   */
  connectClientStream(callId, clientSocket) {
    const bridge = this.activeBridges.get(callId);
    if (!bridge) {
      logger.error(`No bridge found for call ${callId}`);
      return;
    }
    
    bridge.clientStream = clientSocket;
    logger.info(`📱 Client stream connected to bridge for call ${callId}`);
    
    // If Twilio stream is already connected, start processing
    if (bridge.twilioStream && bridge.twilioStreamSid) {
      this.startAudioProcessing(callId);
    }
  }
  
  /**
   * Connect Twilio Media Stream to bridge
   */
  connectTwilioStream(callId, twilioSocket, streamSid) {
    const bridge = this.activeBridges.get(callId);
    if (!bridge) {
      logger.error(`No bridge found for call ${callId}`);
      return;
    }
    
    bridge.twilioStream = twilioSocket;
    bridge.twilioStreamSid = streamSid;
    logger.info(`📞 Twilio stream ${streamSid} connected to bridge for call ${callId}`);
    
    // If client stream is already connected, start processing
    if (bridge.clientStream) {
      this.startAudioProcessing(callId);
    }
  }
  
  /**
   * Forward already-transformed mu-law audio to the Twilio Media Stream.
   * Called by media-stream.js after the full PCM -> transform -> resample -> mulaw pipeline.
   *
   * @param {string} callId - The call identifier
   * @param {Buffer} mulawBuffer - mu-law encoded audio ready for Twilio
   */
  forwardAudioToTwilio(callId, mulawBuffer) {
    const bridge = this.activeBridges.get(callId);
    if (!bridge) {
      logger.warn(`Cannot forward audio - no bridge for call ${callId}`);
      return;
    }

    if (!bridge.twilioStream || !bridge.twilioStreamSid) {
      logger.debug(`Waiting for Twilio stream to connect for call ${callId}`);
      return;
    }

    const mediaMessage = JSON.stringify({
      event: 'media',
      streamSid: bridge.twilioStreamSid,
      media: {
        payload: mulawBuffer.toString('base64'),
      },
    });

    if (bridge.twilioStream.readyState === bridge.twilioStream.OPEN) {
      bridge.twilioStream.send(mediaMessage);
    } else {
      logger.warn(`Twilio WebSocket not open (state=${bridge.twilioStream.readyState}) for call ${callId}`);
    }
  }
  
  /**
   * Start audio processing between streams
   */
  startAudioProcessing(callId) {
    const bridge = this.activeBridges.get(callId);
    if (!bridge) return;
    
    logger.info(`🎬 Starting audio processing for call ${callId}`);
    logger.info(`   Client: ${bridge.clientStream ? 'Connected' : 'Missing'}`);
    logger.info(`   Twilio: ${bridge.twilioStream ? 'Connected' : 'Missing'}`);
    logger.info(`   StreamSid: ${bridge.twilioStreamSid || 'Missing'}`);
    
    // Processing happens in media-stream.js; transformed audio arrives via forwardAudioToTwilio()
  }
  
  /**
   * Remove bridge and cleanup
   */
  removeBridge(callId) {
    const bridge = this.activeBridges.get(callId);
    if (bridge) {
      const duration = Date.now() - bridge.startTime;
      logger.info(`🗑️ Removing audio bridge for call ${callId} (${duration}ms active)`);
      
      bridge.isProcessing = false;
      bridge.audioQueue = [];
    }
    
    this.activeBridges.delete(callId);
  }
  
  /**
   * Get bridge status for debugging
   */
  getBridgeStatus(callId) {
    const bridge = this.activeBridges.get(callId);
    if (!bridge) return null;
    
    return {
      callId: bridge.callId,
      voicePreset: bridge.voicePreset,
      clientConnected: !!bridge.clientStream,
      twilioConnected: !!bridge.twilioStream,
      streamSid: bridge.twilioStreamSid,
      queuedAudio: bridge.audioQueue.length,
      isProcessing: bridge.isProcessing,
      uptime: Date.now() - bridge.startTime
    };
  }
  
  /**
   * Get all active bridges
   */
  getActiveBridges() {
    return Array.from(this.activeBridges.keys()).map(callId => 
      this.getBridgeStatus(callId)
    );
  }
}