import React, { useState, useEffect, useRef, useCallback } from 'react';
import './App.css';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import {
  faMicrophone, faMicrophoneSlash, faStop, faPaperPlane, faPlay,
  faWifi, faPowerOff, faComments, faPause
} from '@fortawesome/free-solid-svg-icons';

// Constants
const INPUT_SAMPLE_RATE = 16000;
const OUTPUT_SAMPLE_RATE = 24000;
const MIC_BUFFER_SIZE = 4096;

const LANGUAGES = [
  { code: 'en-US', name: 'English' },
  { code: 'th-TH', name: 'Thai' },
  { code: 'id-ID', name: 'Indonesian' },
];

// Configure backend hosts for different environments
const PRODUCTION_HOST = 'gemini-backend-service-1018963165306.us-central1.run.app';
const LOCAL_HOST = 'localhost:8000';

// Set the backend host based on environment
// Use window.location.hostname to detect if we're in production or local development
const isProduction = window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1';
const BACKEND_HOST = isProduction ? PRODUCTION_HOST : LOCAL_HOST;

// WebSocket protocol - use wss for production, ws for local development
const WS_PROTOCOL = isProduction ? 'wss' : 'ws';
const HTTP_PROTOCOL = isProduction ? 'https' : 'http';
const generateUniqueId = () => `${Date.now().toString(36)}-${Math.random().toString(36).substring(2, 7)}`;

const App = () => {
  const [isRecording, setIsRecording] = useState(false); // Is microphone actively sending audio
  const [isSessionActive, setIsSessionActive] = useState(false); // Is the overall session (WS + mic) active
  const [isMuted, setIsMuted] = useState(false); // Is microphone muted
  const [messages, setMessages] = useState([]);
  const [textInputValue, setTextInputValue] = useState('');
  const [transcriptionMessages, setTranscriptionMessages] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [selectedLanguage, setSelectedLanguage] = useState(LANGUAGES[0].code);
  const [bigQueryLogs, setBigQueryLogs] = useState([]);
  const [webSocketStatus, setWebSocketStatus] = useState('N/A');


  const isRecordingRef = useRef(isRecording);
  const isSessionActiveRef = useRef(isSessionActive);
  const isMutedRef = useRef(isMuted);
  const playbackAudioContextRef = useRef(null);
  const localAudioContextRef = useRef(null);
  const mediaStreamRef = useRef(null);
  const scriptProcessorNodeRef = useRef(null);
  const audioChunkSentCountRef = useRef(0);
  const socketRef = useRef(null);
  const audioQueueRef = useRef([]);
  const isPlayingRef = useRef(false);
  const currentAudioSourceRef = useRef(null);
  const logsAreaRef = useRef(null);
  const chatAreaRef = useRef(null);

  useEffect(() => { isRecordingRef.current = isRecording; }, [isRecording]);
  useEffect(() => { isSessionActiveRef.current = isSessionActive; }, [isSessionActive]);
  useEffect(() => { isMutedRef.current = isMuted; }, [isMuted]);

  const addLogEntry = useCallback((type, content) => {
    if (type === 'gemini_audio' || type === 'mic_control') {
      // console.log(`[UI_PANEL_FILTERED] Type: ${type}, Content: "${content}"`);
      return;
    }
    const newEntry = {
      id: generateUniqueId(), type, content, timestamp: new Date().toLocaleTimeString()
    };
    setMessages(prev => [...prev, newEntry]);
  }, []);

  const fetchBigQueryLogs = useCallback(async () => {
    setIsLoading(true);
    try {
      const response = await fetch(`${HTTP_PROTOCOL}://${BACKEND_HOST}/api/logs`);
      if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
      const data = await response.json();
      const newLogEntries = data.map(log => ({
        id: generateUniqueId(),
        type: 'bigquery',
        content: typeof log === 'string' ? log : log.message || JSON.stringify(log),
        timestamp: log.timestamp ? new Date(log.timestamp).toLocaleTimeString() : new Date().toLocaleTimeString()
      }));
      newLogEntries.forEach(logEntry => {
        const logContentString = String(logEntry.content);
        const contentLowerCase = logContentString.toLowerCase();
        const errorKeywords = ['error', 'failed', 'exception', 'traceback', 'critical', 'err:', 'warn:', 'warning'];
        let isError = (logEntry.status && String(logEntry.status).toLowerCase().includes('error')) || errorKeywords.some(keyword => contentLowerCase.includes(keyword));
        console.log(`%c[BigQuery ${isError ? 'ERROR' : 'Log'}] ${logEntry.timestamp}: ${logContentString}`, isError ? 'color: #FF3131; font-weight: bold;' : 'color: #39FF14;');
      });
      setMessages(prevMessages => {
        const existingLogContents = new Set(prevMessages.filter(m => m.type === 'bigquery').map(m => m.content));
        const uniqueNewEntries = newLogEntries.filter(newLog => !existingLogContents.has(newLog.content));
        return [...prevMessages, ...uniqueNewEntries].sort((a, b) => new Date('1970/01/01 ' + a.timestamp) - new Date('1970/01/01 ' + b.timestamp));
      });
      // No need to update bigQueryLogs as it's not used elsewhere
    } catch (error) {
      console.error("Failed to fetch BigQuery logs:", error);
      addLogEntry('error', `Failed to fetch BigQuery logs: ${error.message}`);
    } finally {
      setIsLoading(false);
    }
  }, [addLogEntry]);

  useEffect(() => {
    fetchBigQueryLogs();
    const intervalId = setInterval(fetchBigQueryLogs, 15000);
    return () => clearInterval(intervalId);
  }, [fetchBigQueryLogs]);

  useEffect(() => { if (logsAreaRef.current) logsAreaRef.current.scrollTop = logsAreaRef.current.scrollHeight; }, [messages]);
  useEffect(() => { if (chatAreaRef.current) chatAreaRef.current.scrollTop = chatAreaRef.current.scrollHeight; }, [transcriptionMessages]);

  const getPlaybackAudioContext = useCallback(async (triggeredByAction) => {
    if (!playbackAudioContextRef.current || playbackAudioContextRef.current.state === 'closed') {
      try {
        addLogEntry('audio', 'Attempting to create Playback AudioContext.');
        playbackAudioContextRef.current = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: OUTPUT_SAMPLE_RATE });
        playbackAudioContextRef.current.onstatechange = () => addLogEntry('audio', `PlaybackCTX state changed to: ${playbackAudioContextRef.current?.state}`);
        addLogEntry('audio', `Playback AudioContext CREATED. Initial state: ${playbackAudioContextRef.current.state}, SampleRate: ${playbackAudioContextRef.current.sampleRate}`);
      } catch (e) {
        console.error('[CTX_PLAYBACK_MGR] FAILED to CREATE Playback AudioContext', e);
        addLogEntry('error', `FATAL PlaybackCTX ERROR: ${e.message}`);
        playbackAudioContextRef.current = null; return null;
      }
    }
    if (playbackAudioContextRef.current.state === 'suspended') {
      if (triggeredByAction && (triggeredByAction.toLowerCase().includes("user_action") || triggeredByAction.toLowerCase().includes("systemaction"))) {
        addLogEntry('audio', `PlaybackCTX State 'suspended'. Attempting RESUME by: ${triggeredByAction}.`);
        try {
          await playbackAudioContextRef.current.resume();
          addLogEntry('audio', `PlaybackCTX Resume attempt finished. State: ${playbackAudioContextRef.current.state}`);
        } catch (e) {
          console.error(`[CTX_PLAYBACK_MGR] FAILED to RESUME PlaybackCTX`, e);
          addLogEntry('error', `FAILED to RESUME PlaybackCTX: ${e.message}`);
        }
      }
    }
    if (playbackAudioContextRef.current?.state !== 'running') addLogEntry('warning', `PlaybackCTX not 'running'. State: ${playbackAudioContextRef.current?.state}`);
    return playbackAudioContextRef.current;
  }, [addLogEntry]);

  const playNextGeminiChunk = useCallback(async () => {
    if (isPlayingRef.current || audioQueueRef.current.length === 0) return;
    isPlayingRef.current = true;
    const arrayBuffer = audioQueueRef.current.shift();
    const audioCtx = await getPlaybackAudioContext("playNextGeminiChunk_SystemAction");
    if (!audioCtx || audioCtx.state !== 'running') {
      addLogEntry('error', `Playback FAIL: Audio system not ready (${audioCtx?.state})`);
      isPlayingRef.current = false; return;
    }
    try {
      if (!arrayBuffer || arrayBuffer.byteLength === 0 || arrayBuffer.byteLength % 2 !== 0) {
        addLogEntry('warning', 'Received empty or invalid audio chunk. Skipping.');
        isPlayingRef.current = false; if (audioQueueRef.current.length > 0) playNextGeminiChunk(); return;
      }
      const pcm16Data = new Int16Array(arrayBuffer);
      const float32Data = new Float32Array(pcm16Data.length);
      for (let i = 0; i < pcm16Data.length; i++) float32Data[i] = pcm16Data[i] / 32768.0;
      if (float32Data.length === 0) {
        addLogEntry('warning', 'Received empty audio chunk (after conversion). Skipping.');
        isPlayingRef.current = false; if (audioQueueRef.current.length > 0) playNextGeminiChunk(); return;
      }
      const audioBuffer = audioCtx.createBuffer(1, float32Data.length, OUTPUT_SAMPLE_RATE);
      audioBuffer.copyToChannel(float32Data, 0);
      const source = audioCtx.createBufferSource();
      source.buffer = audioBuffer;
      const gainNode = audioCtx.createGain();
      gainNode.gain.setValueAtTime(0.8, audioCtx.currentTime);
      source.connect(gainNode);
      gainNode.connect(audioCtx.destination);
      source.onended = () => {
        addLogEntry('gemini_audio', 'Audio chunk finished playing.');
        isPlayingRef.current = false;
        currentAudioSourceRef.current = null;
        if (audioQueueRef.current.length > 0) playNextGeminiChunk();
        source.disconnect(); gainNode.disconnect();
      };
      currentAudioSourceRef.current = source;
      addLogEntry('gemini_audio', 'Starting playback of Gemini audio chunk...');
      source.start();
    } catch (error) {
      currentAudioSourceRef.current = null;
      addLogEntry('error', `Playback Error: ${error.message}`);
      isPlayingRef.current = false;
      if (audioQueueRef.current.length > 0) playNextGeminiChunk();
    }
  }, [getPlaybackAudioContext, addLogEntry]);

  const stopSystemAudioPlayback = useCallback(() => {
    if (currentAudioSourceRef.current) {
      try {
        currentAudioSourceRef.current.stop();
        addLogEntry('gemini_audio', 'System audio playback stopped by barge-in.');
      } catch (e) {
        addLogEntry('warning', `Could not stop current audio source for barge-in: ${e.message}`);
      }
      currentAudioSourceRef.current = null;
    }
    audioQueueRef.current = [];
    isPlayingRef.current = false;
    addLogEntry('gemini_audio', 'Gemini audio queue cleared due to barge-in.');
  }, [addLogEntry]);

  const processAudio = useCallback((audioProcessingEvent) => {
    if (!isRecordingRef.current || isMutedRef.current || !socketRef.current || socketRef.current.readyState !== WebSocket.OPEN) {
      return;
    }
    if (isPlayingRef.current) {
      const inputData = audioProcessingEvent.inputBuffer.getChannelData(0);
      const hasAudioSignal = inputData.some(sample => Math.abs(sample) > 0.04);
      if (hasAudioSignal) {
        addLogEntry('barge_in', 'User speech detected during system playback. Initiating barge-in.');
        stopSystemAudioPlayback();
      }
    }
    const inputBuffer = audioProcessingEvent.inputBuffer;
    const pcmData = inputBuffer.getChannelData(0);
    const downsampledBuffer = new Int16Array(pcmData.length);
    for (let i = 0; i < pcmData.length; i++) {
      downsampledBuffer[i] = Math.max(-1, Math.min(1, pcmData[i])) * 32767;
    }
    if (socketRef.current && socketRef.current.readyState === WebSocket.OPEN) {
      socketRef.current.send(downsampledBuffer.buffer);
      audioChunkSentCountRef.current++;
    }
  }, [addLogEntry, stopSystemAudioPlayback]);

  const handleStartListening = useCallback(async (isResuming = false) => {
    if (isRecordingRef.current && !isResuming) {
      addLogEntry('mic_control', 'Mic already active. Start request ignored.');
      return;
    }
    if (!isSessionActiveRef.current) {
      addLogEntry('mic_control', 'Session not active. Cannot start microphone.');
      return;
    }
    addLogEntry('mic_control', isResuming ? 'Resume Microphone Input requested.' : 'Start Microphone Input requested as part of session.');

    if (!isResuming) {
        await getPlaybackAudioContext("handleStartListening_UserAction");
    }

    if (!mediaStreamRef.current || !localAudioContextRef.current || localAudioContextRef.current.state === 'closed' || !scriptProcessorNodeRef.current) {
      if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
        addLogEntry('error', 'getUserMedia not supported on your browser!');
        setIsSessionActive(false);
        return;
      }
      try {
        addLogEntry('mic', 'Requesting microphone access for new stream...');
        mediaStreamRef.current = await navigator.mediaDevices.getUserMedia({ audio: { sampleRate: INPUT_SAMPLE_RATE, channelCount: 1 } });
        addLogEntry('mic', 'Microphone access GRANTED.');

        localAudioContextRef.current = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: INPUT_SAMPLE_RATE });
        const source = localAudioContextRef.current.createMediaStreamSource(mediaStreamRef.current);
        scriptProcessorNodeRef.current = localAudioContextRef.current.createScriptProcessor(MIC_BUFFER_SIZE, 1, 1);
        scriptProcessorNodeRef.current.onaudioprocess = processAudio;
        source.connect(scriptProcessorNodeRef.current);
        scriptProcessorNodeRef.current.connect(localAudioContextRef.current.destination);
        addLogEntry('mic', 'Microphone processing chain (re)established.');
      } catch (err) {
        console.error('Failed to start microphone:', err);
        addLogEntry('error', `Mic Setup Error: ${err.message}. Please check permissions.`);
        setIsSessionActive(false);
        if (socketRef.current && socketRef.current.readyState === WebSocket.OPEN) {
            socketRef.current.close(1000, "Mic setup failed during session start");
        }
        return;
      }
    } else if (localAudioContextRef.current.state === 'suspended') {
      try {
        await localAudioContextRef.current.resume();
        addLogEntry('mic', 'Local AudioContext for microphone resumed.');
      } catch (e) {
        addLogEntry('error', `Could not resume local audio context for mic: ${e.message}`);
        return;
      }
    }
    setIsRecording(true);
    addLogEntry('mic_status', 'Microphone is NOW actively sending data.');
  }, [addLogEntry, processAudio, getPlaybackAudioContext, isRecordingRef, isSessionActiveRef, setIsSessionActive, mediaStreamRef, localAudioContextRef, scriptProcessorNodeRef, socketRef]); // Removed constants from dependencies

  const handlePauseListening = useCallback(() => {
    if (!isRecordingRef.current) {
      addLogEntry('mic_control', 'Not currently sending mic data. Pause request ignored.');
      return;
    }
    addLogEntry('mic_control', 'Pause Microphone Input requested by user.');
    setIsRecording(false);
    addLogEntry('mic_status', 'Microphone is NOW paused (not sending data).');
  }, [addLogEntry, isRecordingRef, setIsRecording]); // Added missing dependencies

  const handleStopListeningAndCleanupMic = useCallback(() => {
    addLogEntry('mic_control', 'Full Microphone Stop and Resource Cleanup requested.');
    setIsRecording(false);

    if (scriptProcessorNodeRef.current) {
      scriptProcessorNodeRef.current.disconnect();
      scriptProcessorNodeRef.current.onaudioprocess = null;
      scriptProcessorNodeRef.current = null;
      addLogEntry('mic_resource', 'ScriptProcessorNode disconnected and nullified.');
    }
    if (mediaStreamRef.current) {
      mediaStreamRef.current.getTracks().forEach(track => track.stop());
      mediaStreamRef.current = null;
      addLogEntry('mic_resource', 'MediaStream tracks stopped and nullified.');
    }
    if (localAudioContextRef.current) {
      if (localAudioContextRef.current.state !== 'closed') {
        localAudioContextRef.current.close().then(() => {
          addLogEntry('mic_resource', 'Local AudioContext for mic closed.');
        }).catch(e => {
          addLogEntry('error', `Error closing Local AudioContext for mic: ${e.message}`);
        });
      }
      localAudioContextRef.current = null;
    }
    audioChunkSentCountRef.current = 0;
    addLogEntry('mic_status', 'Microphone resources cleaned up.');
  }, [addLogEntry, setIsRecording, scriptProcessorNodeRef, mediaStreamRef, localAudioContextRef, audioChunkSentCountRef]); // Added missing dependencies

  const connectWebSocket = useCallback((language) => {
    if (socketRef.current && (socketRef.current.readyState === WebSocket.OPEN || socketRef.current.readyState === WebSocket.CONNECTING)) {
      if (socketRef.current.url.includes(`lang=${language}`)) {
        addLogEntry('ws', `WebSocket already open or connecting with ${language}.`);
        if(isSessionActiveRef.current && !isRecordingRef.current) {
            handleStartListening(false);
        }
        return;
      }
      addLogEntry('ws', `Closing existing WebSocket (url: ${socketRef.current.url}, state: ${socketRef.current.readyState}) before new connection for lang ${language}.`);
      socketRef.current.close(1000, 'New connection with different language initiated by connectWebSocket');
    }

    addLogEntry('ws', `Attempting to connect to WebSocket with language: ${language}...`);
    setWebSocketStatus('Connecting...');
    socketRef.current = new WebSocket(`${WS_PROTOCOL}://${BACKEND_HOST}/listen?lang=${language}`);
    socketRef.current.binaryType = 'arraybuffer';

    socketRef.current.onopen = () => {
      setWebSocketStatus('Open');
      addLogEntry('ws', `WebSocket Connected (Lang: ${language}).`);
      if (isSessionActiveRef.current) {
        addLogEntry('session_flow', 'Session is active. Proceeding to start microphone input via handleStartListening.');
        handleStartListening(false); // This will set isRecording to true
        setIsMuted(false); // Ensure mic is unmuted when session starts/restarts
      } else {
        addLogEntry('ws_warn', 'WebSocket opened, but session is NOT marked active. Mic not started.');
      }
    };

    socketRef.current.onmessage = (event) => {
      if (typeof event.data === 'string') {
        try {
          const receivedData = JSON.parse(event.data);
          if (receivedData.type && receivedData.type.endsWith('_update')) {
            addLogEntry(receivedData.type, `${receivedData.sender}: ${receivedData.text} (Final: ${receivedData.is_final})`);
            setTranscriptionMessages(prevMessages => {
              const existingMessageIndex = prevMessages.findIndex(msg => msg.id === receivedData.id);
              if (existingMessageIndex !== -1) {
                return prevMessages.map(msg =>
                  msg.id === receivedData.id
                    ? { ...msg, text: receivedData.text, is_final: receivedData.is_final }
                    : msg
                );
              } else { return [...prevMessages, { id: receivedData.id, text: receivedData.text, sender: receivedData.sender, is_final: receivedData.is_final }]; }
            });
          } else if (receivedData.type === 'error') {
            addLogEntry('error', `Server Error via WS: ${receivedData.message}`);
          } else { addLogEntry('ws_json_unhandled', `Unhandled JSON: ${event.data.substring(0,150)}...`);}
        } catch (e) { addLogEntry('error', `Failed to parse JSON from WS: ${e.message}. Raw: ${event.data.substring(0,150)}...`);}
      } else if (event.data instanceof ArrayBuffer) {
        audioQueueRef.current.push(event.data);
        if (!isPlayingRef.current) playNextGeminiChunk();
      } else { addLogEntry('ws_unknown_type', `Received unknown data type from WS: ${typeof event.data}`);}
    };

    socketRef.current.onerror = (error) => {
      console.error('WebSocket Error:', error);
      setWebSocketStatus('Error');
      addLogEntry('error', `WebSocket error. Details in console.`);
      if (isSessionActiveRef.current) {
        addLogEntry('session_flow', 'Session active during WebSocket error. Terminating session.');
        setIsSessionActive(false);
        handleStopListeningAndCleanupMic();
      }
    };

    socketRef.current.onclose = (event) => {
      setWebSocketStatus('Closed');
      addLogEntry('ws', `WebSocket Disconnected. Code: ${event.code}, Reason: "${event.reason || 'No reason given'}"`);
      const intentionalCloseReasons = [
        'User stopped session',
        'Language changed during active session - stopping session',
        'Component unmounting',
        'New connection with different language initiated by connectWebSocket',
        'Mic setup failed during session start',
        'getUserMedia not supported'
      ];
      if (!intentionalCloseReasons.includes(event.reason) && event.code !== 1000 && event.code !== 1005) {
        addLogEntry('error', `WebSocket closed unexpectedly (Code: ${event.code}, Reason: "${event.reason}"). Session terminated if active.`);
        if (isSessionActiveRef.current) {
          setIsSessionActive(false);
          handleStopListeningAndCleanupMic();
        }
      } else {
        addLogEntry('ws_info', `WebSocket closed intentionally or expectedly (Reason: "${event.reason}", Code: ${event.code}).`);
      }
      if (isSessionActiveRef.current && !intentionalCloseReasons.includes(event.reason) && event.code !== 1000) {
          addLogEntry('session_flow_warn', 'Unexpected WS close during active session. Ensuring session is marked inactive.');
          setIsSessionActive(false);
      }
    };
  }, [addLogEntry, playNextGeminiChunk, handleStartListening, handleStopListeningAndCleanupMic]);



  const handleToggleSession = useCallback(async () => {
    if (isSessionActiveRef.current) {
      addLogEntry('session_control', 'User requested to STOP session.');
      handleStopListeningAndCleanupMic();
      if (socketRef.current && (socketRef.current.readyState === WebSocket.OPEN || socketRef.current.readyState === WebSocket.CONNECTING)) {
        addLogEntry('ws_control', 'Closing WebSocket due to session stop request.');
        socketRef.current.close(1000, 'User stopped session');
      }
      setIsSessionActive(false);
      setIsMuted(false); // Reset mute state when session stops
      addLogEntry('session_status', 'Session INACTIVE.');
    } else {
      addLogEntry('session_control', 'User requested to START session.');
      await getPlaybackAudioContext("handleToggleSession_UserAction_Start");

      const currentLangName = LANGUAGES.find(l => l.code === selectedLanguage)?.name || selectedLanguage;
      addLogEntry('session_flow', `Attempting to connect WebSocket for session start (Language: ${currentLangName}).`);

      setIsSessionActive(true);
      setIsMuted(false); // Ensure mic is unmuted when starting a new session
      connectWebSocket(selectedLanguage);
      addLogEntry('session_status', 'Session PENDING (WebSocket connecting, Mic to start on WS open).');
    }
  }, [selectedLanguage, connectWebSocket, handleStopListeningAndCleanupMic, addLogEntry, getPlaybackAudioContext]);

  const handleMicMuteToggle = useCallback(() => {
    if (!isSessionActiveRef.current) return;
    if (isRecordingRef.current) {
      setIsMuted(prevMuted => {
        const newMutedState = !prevMuted;
        addLogEntry('mic_control', `Microphone ${newMutedState ? 'MUTED' : 'UNMUTED'}.`);
        return newMutedState;
      });
    } else {
      // If session is active but not recording (e.g., after explicit pause, or initial state)
      addLogEntry('mic_control', 'Mic button (unmute/start) pressed while not recording in active session. Attempting to start mic.');
      setIsMuted(false); // Ensure unmuted
      handleStartListening(); // This will set isRecording to true
    }
  }, [addLogEntry, handleStartListening]);

  const handleSendTextMessage = useCallback(() => {
    if (!textInputValue.trim()) return;
    const currentLangName = LANGUAGES.find(l => l.code === selectedLanguage)?.name || selectedLanguage;
    addLogEntry('user_text', `User typed (Lang: ${currentLangName}): "${textInputValue}"`);
    if (socketRef.current && socketRef.current.readyState === WebSocket.OPEN) {
      const messagePayload = { 
        type: 'text_message', text: textInputValue, language: selectedLanguage, 
        timestamp: new Date().toISOString(), id: generateUniqueId() 
      };
      socketRef.current.send(JSON.stringify(messagePayload));
      setTranscriptionMessages(prev => [...prev, {
        id: messagePayload.id, sender: 'user', text: textInputValue,
        is_final: true, timestamp: new Date().toLocaleTimeString()
      }]);
      setTextInputValue('');
    } else {
      addLogEntry('error', 'Cannot send text: WebSocket not connected or not open.');
    }
  }, [textInputValue, addLogEntry, selectedLanguage]);



  useEffect(() => {
    addLogEntry('status', 'Welcome! Click "Start Session" or type your query.');
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);


  const handleClearConsole = () => {
    setMessages([]);
    addLogEntry('console', 'Console cleared by user.');
  };

  const [fabActive, setFabActive] = useState(false);

  const handleFabClick = () => {
    if (!fabActive) {
      // Start WebSocket connection
      handleToggleSession();
      setFabActive(true);
    } else {
      // Stop WebSocket connection
      handleToggleSession();
      setFabActive(false);
    }
  };

  return (
    <div className="app-container">
      <div className="console-panel">
        <div className="console-header">
          <h2>Console</h2>
          <div className="console-header-controls">
            <select className="console-dropdown" defaultValue="conversations">
              <option value="conversations">Conversations</option>
            </select>
            {/* console-paused-button removed as its info is now in control-bar */}
          </div>
        </div>
        <div className="logs-area" ref={logsAreaRef}>
          {isLoading && <p className="loading-indicator">Loading...</p>}
          {messages.map(msg => (
            <div key={msg.id} className={`log-entry log-entry-${msg.type} ${msg.type === 'bigquery' ? 'log-entry-bigquery' : ''}`}>
              <span className="log-timestamp">[{msg.timestamp}] </span>
              <span className="log-prefix">{msg.type.toUpperCase()}: </span>
              <span className="log-message">{msg.content}</span>
            </div>
          ))}
        </div>
        <div className="text-input-area console-text-input-area">
          <input
            type="text"
            className="text-input"
            value={textInputValue}
            onChange={(e) => setTextInputValue(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handleSendTextMessage()}
            placeholder="Type something..."
            disabled={!isSessionActive}
          />
          <button onClick={handleSendTextMessage} className="control-button send-button" disabled={!textInputValue.trim() || !isSessionActive}>
            <FontAwesomeIcon icon={faPaperPlane} />
          </button>
        </div>
      </div>

      <div className="main-panel">
        <div className="main-panel-header">
          <div className="hdfc-header">
            <img src={require('./images/image.png')} alt="HDFC Bank" className="hdfc-logo-img" />
          </div>
        </div>
        <div className="results-content chat-area" ref={chatAreaRef}>
          {transcriptionMessages.length === 0 && (
            <div className="results-content-placeholder">
              <p>Audio transcriptions will appear here when a session is active.</p>
            </div>
          )}
          {transcriptionMessages.map(msg => (
            <div key={msg.id} className={`chat-bubble ${msg.sender === 'user' ? 'user-bubble' : 'ai-bubble'}`}>
              <div className="chat-bubble-text">{msg.text}</div>
            </div>
          ))}
        </div>
      </div>

        <div className="control-bar">
          <div className="control-tray main-controls">
            <button onClick={handleToggleSession} className="control-button icon-button session-button" title={isSessionActive ? "Stop Current Session" : "Start a New Session"}>
              <div className="icon-button-content">
                <FontAwesomeIcon icon={isSessionActive ? faStop : faPlay} />
                <span className="icon-button-text">{isSessionActive ? 'Stop' : 'Start'}</span>
              </div>
            </button>
            <button
              onClick={handleMicMuteToggle}
              className={`control-button icon-button mic-button ${isRecording && !isMutedRef.current ? 'active' : ''} ${isMutedRef.current ? 'muted' : ''}`}
              disabled={!isSessionActiveRef.current}
              title={isMutedRef.current ? "Unmute Microphone" : (isRecordingRef.current ? "Mute Microphone" : "Start Microphone")}
            >
              <div className="icon-button-content">
                <FontAwesomeIcon icon={isMutedRef.current ? faMicrophoneSlash : faMicrophone} />
                <span className="icon-button-text">{isMutedRef.current ? 'Muted' : 'Unmuted'}</span>
              </div>
            </button>
            <div className="audio-signal-placeholder">
              {isRecording && !isMuted && (
                <div className="audio-wave">
                  <span></span><span></span><span></span><span></span><span></span>
                </div>
              )}
            </div>
          </div>
          <div className="control-tray secondary-controls">
            <select
              value={selectedLanguage}
              onChange={(e) => setSelectedLanguage(e.target.value)}
              disabled={isSessionActiveRef.current}
              className="language-selector-dropdown"
              title="Select Language (Session restarts on change if active)"
            >
              {LANGUAGES.map(lang => (
                <option key={lang.code} value={lang.code}>{lang.name}</option>
              ))}
            </select>
            <div className="status-indicator icon-status-indicator websocket-status" title="WebSocket Connection Status">
              <div className="icon-status-content">
                <FontAwesomeIcon icon={faWifi} />
                <span className="icon-status-text">WS: {webSocketStatus}</span>
              </div>
            </div>
            <div className="status-indicator icon-status-indicator session-active-status" title="Session Status">
              <div className="icon-status-content">
                <FontAwesomeIcon icon={faPowerOff} />
                <span className="icon-status-text">{isSessionActiveRef.current ? 'Session: Active' : 'Session: Inactive'}</span>
              </div>
            </div>
          </div>
        </div>

        {/* Floating Action Button */}
        <button 
          className={`floating-action-button ${fabActive ? 'fab-active' : ''}`} 
          onClick={handleFabClick}
          title={fabActive ? "Stop Session" : "Start Session"}
        >
          <FontAwesomeIcon icon={fabActive ? faPause : faComments} />
        </button>
    </div>
  );
};

export default App;
