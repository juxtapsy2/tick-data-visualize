import { useState, useEffect, useRef } from 'react';
import { MarketChart } from './components/MarketChart';
import type { MarketDataPoint } from './components/MarketChart';
import { MarketStreamService } from './services/marketService';
import './App.css';

function App() {
  const [marketData, setMarketData] = useState<MarketDataPoint[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [stats, setStats] = useState({
    totalPoints: 0,
    latestVN30: 0,
    latestHNX: 0,
  });

  const serviceRef = useRef<MarketStreamService | null>(null);

  useEffect(() => {
    // Initialize service and load data
    const initializeData = async () => {
      const wsHost = import.meta.env.VITE_WS_HOST ||
        (window.location.hostname === 'localhost'
          ? 'http://localhost:8080'
          : `${window.location.protocol}//${window.location.host}`);
      console.log('Initializing MarketStreamService with host:', wsHost);
      serviceRef.current = new MarketStreamService(wsHost);

      // Load historical data using smart defaults
      // Backend automatically returns data from 9:00 AM Vietnam time to now
      try {
        console.log('Loading historical data (defaults: 9:00 AM Vietnam → now)...');
        const historicalData = await serviceRef.current.getHistoricalData();

        if (historicalData.length > 0) {
          console.log('Loaded', historicalData.length, 'historical points');
          setMarketData(historicalData);

          // Update stats with latest point
          const latest = historicalData[historicalData.length - 1];
          setStats({
            totalPoints: historicalData.length,
            latestVN30: latest.vn30Value,
            latestHNX: latest.hnxValue,
          });
        }
      } catch (error) {
        console.error('Failed to load historical data:', error);
      }

      // Then start streaming new data
      console.log('Starting real-time stream...');
      startStreaming();
    };

    initializeData();

    // Cleanup on unmount
    return () => {
      if (serviceRef.current) {
        serviceRef.current.stopStream();
      }
    };
  }, []);

  const startStreaming = () => {
    if (!serviceRef.current) return;

    setError(null);
    setIsConnected(true);

    serviceRef.current.startStream(
      (data) => {
        setMarketData((prev) => {
          // Keep all data points - chart will zoom out as more data arrives
          return [...prev, data];
        });

        setStats((prevStats) => ({
          totalPoints: prevStats.totalPoints + 1,
          latestVN30: data.vn30Value,
          latestHNX: data.hnxValue,
        }));
      },
      (err) => {
        setError(err.message);
        setIsConnected(false);
      },
      () => {
        setIsConnected(false);
      }
    );
  };

  const handleReconnect = () => {
    if (serviceRef.current) {
      serviceRef.current.stopStream();
      startStreaming();
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>Market Data Streaming Platform</h1>
        <div className="connection-status">
          <span className={`status-indicator ${isConnected ? 'connected' : 'disconnected'}`} />
          {isConnected ? 'Connected' : 'Disconnected'}
          {!isConnected && !error && (
            <button onClick={handleReconnect} className="reconnect-button">
              Reconnect
            </button>
          )}
        </div>
      </header>

      {error && (
        <div className="error-banner">
          <strong>Connection Error:</strong> {error}
          <button onClick={handleReconnect} className="retry-button">
            Retry
          </button>
        </div>
      )}

      <div className="stats-panel">
        <div className="stat-card">
          <div className="stat-label">Total Points</div>
          <div className="stat-value">{stats.totalPoints}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">VN30 Index</div>
          <div className="stat-value vn30">{stats.latestVN30.toFixed(2)}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">F1</div>
          <div className="stat-value hnx">{stats.latestHNX.toFixed(2)}</div>
        </div>
      </div>

      <div className="chart-container">
        {marketData.length > 0 ? (
          <MarketChart data={marketData} />
        ) : (
          <div className="loading-state">
            {isConnected ? 'Waiting for data...' : 'Not connected'}
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
