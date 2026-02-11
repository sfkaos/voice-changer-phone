# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Real-time voice transformation phone system. Users speak into their browser microphone, audio is transformed via ElevenLabs Speech-to-Speech API, and sent to a phone call recipient through Twilio. Built with Fastify, ES modules, Node 20+.

## Commands

```bash
npm start        # Production: node src/server.js
npm run dev      # Development with auto-reload: node --watch src/server.js
npm test         # Audio pipeline test: node scripts/test-audio.js
npm run call     # Initiate call from CLI: node scripts/initiate-call.js [phone] [voice]
```

No build step, no linter, no bundler. ES modules used natively (`"type": "module"` in package.json).

For local development with Twilio webhooks, expose the server via ngrok: `ngrok http 3000` and set `SERVER_URL` to the ngrok HTTPS URL.

## Architecture

### Audio Pipeline

```
Browser mic (getUserMedia) → WebSocket /client-audio-stream (WebM/Opus)
  → Server: Audio Bridge → Voice Transformer (ElevenLabs STS API)
    → μ-law 8kHz → Twilio Media Stream → Called person's phone
```

### Two WebSocket Endpoints

- **`/client-audio-stream`** — Receives browser microphone audio (binary WebM chunks). Handler: `handleClientAudioStream()` in `src/services/media-stream.js`.
- **`/media-stream`** — Twilio Media Stream integration (JSON + base64 μ-law audio). Handler: `handleMediaStream()` in `src/services/media-stream.js`.

### Key Services (src/services/)

- **call-manager.js** — Twilio SDK wrapper. Initiates outbound calls, tracks active streams, manages call history/stats, formats phone numbers to E.164.
- **media-stream.js** — WebSocket handlers for both Twilio and client audio streams. Exports `handleMediaStream()` and `handleClientAudioStream()`.
- **audio-bridge.js** — Connects client mic stream to Twilio call stream. Manages bridge lifecycle and queues audio for processing.
- **voice-transformer.js** — ElevenLabs Speech-to-Speech API integration. Converts PCM→WAV for the API. Supports default (`eleven_english_sts_v2`) and Flash (`eleven_flash_v2_5`) models.

### Audio Utilities (src/utils/)

- **audio-codec.js** — μ-law ↔ PCM conversion with lookup tables, sample rate resampling via linear interpolation.
- **audio-buffer.js** — Adaptive buffering (100-300ms) with sliding window and jitter buffer support.
- **voice-presets.js** — 9 voice presets (4 male, 3 female, 2 character) with ElevenLabs voice IDs and settings.
- **latency-tracker.js** — Processing latency metrics with percentile tracking (p50, p95, p99).
- **logger.js** — Pino-based structured logging with context-based child loggers.

### Frontend

Single-page app in `public/index.html`. Handles microphone capture, audio visualization, voice preset selection, and call control. No framework — vanilla JS.

### HTTP API

- `POST /voice` — TwiML webhook for Twilio
- `POST /api/call` — Initiate outbound call
- `GET /api/call/:callSid` — Call status
- `DELETE /api/call/:callSid` — End call
- `GET /api/voices` — Available voice presets
- `GET /api/stats` — Usage statistics
- `GET /api/health` — Health check

## Environment Variables

Required: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_PHONE_NUMBER`, `ELEVENLABS_API_KEY`, `SERVER_URL`

See `.env.example` for all options including `USE_FLASH_MODEL`, `AUDIO_BUFFER_MS`, `CUSTOM_VOICE_ID`, and rate limiting.

## Deployment

Primary target is Railway.app (see `railway.json` and `Procfile`). Uses NIXPACKS builder.

## Incomplete Areas

- WebM/Opus → PCM decoding in client stream handler is partially implemented
- Bidirectional audio (called person → user browser) not yet wired up
- No formal test framework — testing is manual via browser UI and server logs
