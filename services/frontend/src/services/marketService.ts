export interface MarketDataCallback {
  (data: {
    timestamp: number;
    vn30Value: number;
    hnxValue: number;
  }): void;
}

export interface ErrorCallback {
  (error: Error): void;
}

interface WebSocketMessage {
  type: 'subscribe' | 'data' | 'historical' | 'catchup' | 'error' | 'subscribed' | 'historical_complete' | 'catchup_complete';
  timestamp?: number;
  vn30?: number;
  hnx?: number;
  error?: string;
  date?: string;
}

export class MarketStreamService {
  private ws: WebSocket | null = null;
  private httpHostname: string;
  private wsHostname: string;
  private onDataCallback: MarketDataCallback | null = null;
  private onErrorCallback: ErrorCallback | null = null;
  private onEndCallback: (() => void) | null = null;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private reconnectDelay = 1000;

  constructor(hostname: string = 'http://localhost:8080') {
    // Keep HTTP URL for REST API
    this.httpHostname = hostname.replace(/^ws/, 'http');
    // Convert to WebSocket URL for streaming
    this.wsHostname = hostname.replace(/^http/, 'ws');
  }

  /**
   * Get historical data for today using REST API
   * Defaults: from=9:00 AM Vietnam time, to=now
   * @returns Promise with array of historical data points
   */
  async getHistoricalData(): Promise<Array<{
    timestamp: number;
    vn30Value: number;
    hnxValue: number;
  }>> {
    try {
      console.log('Fetching historical data from REST API...');

      // Call REST endpoint with default parameters (from=9:00 AM VN time, to=now)
      const response = await fetch(`${this.httpHostname}/api/v1/market/historical`);

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: 'Unknown error' }));
        throw new Error(errorData.error || `HTTP ${response.status}: ${response.statusText}`);
      }

      const result = await response.json();

      if (!result.success) {
        throw new Error(result.error || 'Failed to fetch historical data');
      }

      // Transform backend response to frontend format
      const historicalData = result.data.map((point: any) => ({
        timestamp: point.Timestamp,
        vn30Value: point.VN30Value,
        hnxValue: point.HNXValue,
      }));

      console.log('Loaded', historicalData.length, 'historical points from REST API');
      return historicalData;
    } catch (error) {
      console.error('Failed to fetch historical data:', error);
      throw error;
    }
  }

  /**
   * Start streaming market data
   * @param onData Callback for each data point
   * @param onError Callback for errors
   * @param onEnd Callback when stream ends
   */
  startStream(
    onData: MarketDataCallback,
    onError: ErrorCallback,
    onEnd?: () => void
  ): void {
    // Stop any existing stream
    this.stopStream();

    this.onDataCallback = onData;
    this.onErrorCallback = onError;
    this.onEndCallback = onEnd || null;

    this.connect();
  }

  private connect(): void {
    const wsUrl = `${this.wsHostname}/ws`;
    console.log('Connecting to WebSocket:', wsUrl);

    this.ws = new WebSocket(wsUrl);

    this.ws.onopen = () => {
      console.log('WebSocket connected');
      this.reconnectAttempts = 0;

      // Subscribe to real-time stream
      const subscribeMsg: WebSocketMessage = {
        type: 'subscribe'
      };
      this.ws?.send(JSON.stringify(subscribeMsg));
    };

    this.ws.onmessage = (event) => {
      try {
        const msg: WebSocketMessage = JSON.parse(event.data);

        if (msg.type === 'data' && msg.timestamp && msg.vn30 !== undefined && msg.hnx !== undefined) {
          if (this.onDataCallback) {
            this.onDataCallback({
              timestamp: msg.timestamp,
              vn30Value: msg.vn30,
              hnxValue: msg.hnx,
            });
          }
        } else if (msg.type === 'error') {
          console.error('Server error:', msg.error);
          if (this.onErrorCallback) {
            this.onErrorCallback(new Error(msg.error || 'Server error'));
          }
        } else if (msg.type === 'subscribed') {
          console.log('Successfully subscribed to market data stream');
        }
      } catch (error) {
        console.error('Error parsing message:', error);
      }
    };

    this.ws.onerror = (error) => {
      console.error('WebSocket error:', error);
      if (this.onErrorCallback) {
        this.onErrorCallback(new Error('WebSocket connection error'));
      }
    };

    this.ws.onclose = () => {
      console.log('WebSocket disconnected');
      this.ws = null;

      if (this.onEndCallback) {
        this.onEndCallback();
      }

      // Auto-reconnect with exponential backoff
      if (this.reconnectAttempts < this.maxReconnectAttempts) {
        this.reconnectAttempts++;
        const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);
        console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

        setTimeout(() => {
          if (this.onDataCallback) { // Only reconnect if we still have callbacks (not stopped)
            this.connect();
          }
        }, delay);
      } else {
        console.log('Max reconnect attempts reached');
        if (this.onErrorCallback) {
          this.onErrorCallback(new Error('Max reconnect attempts reached'));
        }
      }
    };
  }

  /**
   * Stop the current stream
   */
  stopStream(): void {
    this.onDataCallback = null;
    this.onErrorCallback = null;
    this.onEndCallback = null;

    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  /**
   * Check if stream is active
   */
  isStreaming(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }
}
