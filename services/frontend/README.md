# Market Frontend

Real-time market data visualization using React, TypeScript, and ECharts.

## Features

- **Real-time Streaming**: gRPC-Web connection to market service via Envoy proxy
- **Interactive Charts**: ECharts line chart with dual Y-axes for VN30 and HNX indexes
- **Live Statistics**: Real-time display of latest values and total data points
- **Connection Management**: Auto-reconnect and error handling
- **Responsive Design**: Works on desktop and mobile devices

## Architecture

```
Browser → Frontend (React) → Envoy Proxy (gRPC-Web) → Market Service (gRPC)
```

### Components

1. **MarketChart**: ECharts component displaying two-line chart
2. **MarketStreamService**: gRPC-Web client for streaming market data
3. **App**: Main component connecting chart to stream

## Development

### Prerequisites

- Node.js 20+
- npm or yarn
- protoc (Protocol Buffer compiler)
- protoc-gen-grpc-web plugin

### Setup

```bash
# Install dependencies
npm install

# Generate proto files
./generate-proto.sh

# Start development server
npm run dev
```

The app will be available at `http://localhost:5173`

### Environment Variables

Create a `.env` file:

```bash
VITE_GRPC_HOST=http://localhost:9090
```

- `VITE_GRPC_HOST`: Envoy proxy URL (must support gRPC-Web)

## Building for Production

```bash
# Build the application
npm run build

# Preview production build
npm run preview
```

## Docker Deployment

### Using docker-compose (Recommended)

From the project root:

```bash
# Build and start all services
docker-compose up --build

# Access frontend at http://localhost:3000
```

### Standalone Docker Build

```bash
# Build image
docker build -t market-frontend .

# Run container
docker run -p 3000:80 market-frontend
```

## API Integration

### gRPC-Web Client

The frontend uses gRPC-Web to connect to the market service through Envoy proxy.

**Proto Definition**: `../../shared/proto/market.proto`

**Generated Files**:
- `src/proto/market_pb.d.ts` - TypeScript definitions
- `src/proto/market_pb.js` - JavaScript message classes
- `src/proto/MarketServiceClientPb.ts` - gRPC-Web client

**Service Methods**:
```typescript
// Stream real-time market data
streamMarketData(request: StreamRequest): ClientReadableStream<MarketData>

// Get historical data
getHistoricalData(request: HistoricalRequest): Promise<HistoricalResponse>
```

### Usage Example

```typescript
import { MarketStreamService } from './services/marketService';

const service = new MarketStreamService('http://localhost:9090');

service.startStream(
  (data) => {
    console.log('VN30:', data.vn30Value);
    console.log('HNX:', data.hnxValue);
  },
  (error) => {
    console.error('Stream error:', error);
  }
);
```

## Chart Configuration

### ECharts Options

- **Dual Y-Axes**: Separate scales for VN30 (left) and HNX (right)
- **Smooth Lines**: Bezier curve interpolation
- **Area Fill**: Semi-transparent gradient under lines
- **No Animation**: Optimized for real-time updates
- **Data Limit**: Shows last 100 points (configurable via `maxPoints` prop)

### Customization

Edit `src/components/MarketChart.tsx`:

```typescript
<MarketChart
  data={marketData}
  maxPoints={200}  // Show more/fewer points
/>
```

## Troubleshooting

### Connection Errors

**Error**: "Stream error: Failed to fetch"

**Solution**:
1. Verify Envoy proxy is running on port 9090
2. Check CORS configuration in `docker/envoy/envoy.yaml`
3. Ensure `VITE_GRPC_HOST` points to Envoy, not directly to market service

### Proto Generation Fails

**Error**: "protoc-gen-grpc-web: program not found"

**Solution**:
```bash
# Install via Homebrew (macOS)
brew install protoc-gen-grpc-web protoc-gen-js

# Or download from https://github.com/grpc/grpc-web/releases
```

### No Data Displayed

**Checklist**:
1. ✓ Market service is running and healthy
2. ✓ Data generator is inserting data
3. ✓ Envoy proxy is routing requests
4. ✓ Browser console shows no errors
5. ✓ Network tab shows gRPC-Web requests

## Technology Stack

- **React 18**: UI framework
- **TypeScript**: Type safety
- **Vite**: Build tool and dev server
- **ECharts**: Data visualization
- **gRPC-Web**: Binary protocol for browser
- **Protocol Buffers**: Data serialization

## Performance

- **Real-time Updates**: ~1 second latency (data generator → display)
- **Memory Management**: Automatic pruning to last 200 data points
- **Chart Optimization**: Animation disabled for smooth streaming
- **Bundle Size**: ~500KB gzipped

## License

Part of the Market Data Streaming Platform project.
