import { useEffect, useRef, useState, useCallback } from "react";
import type { WSMessage } from "../types";

const WS_URL = "ws://localhost:8000/ws";

export function useWebSocket() {
  const [message, setMessage]     = useState<WSMessage | null>(null);
  const [connected, setConnected] = useState(false);
  const ws = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<number | undefined>(undefined);

  const connect = useCallback(() => {
    if (ws.current?.readyState === WebSocket.OPEN) return;

    const socket = new WebSocket(WS_URL);

    socket.onopen = () => {
      setConnected(true);
      clearTimeout(reconnectTimer.current);
    };

    socket.onmessage = (e) => {
      try {
        setMessage(JSON.parse(e.data));
      } catch {}
    };

    socket.onclose = () => {
      setConnected(false);
      // reconnect after 2 seconds
      reconnectTimer.current = window.setTimeout(connect, 2000);
    };

    socket.onerror = () => socket.close();

    ws.current = socket;
  }, []);

  useEffect(() => {
    connect();
    return () => {
      if (reconnectTimer.current !== undefined) {
        clearTimeout(reconnectTimer.current);
      }
      ws.current?.close();
    };
  }, [connect]);

  return { message, connected };
}