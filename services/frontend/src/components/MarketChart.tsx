
import { useEffect, useRef, useState } from 'react';
import * as echarts from 'echarts';
import type { ECharts } from 'echarts';

export interface MarketDataPoint {
  timestamp: number;
  vn30Value: number;
  hnxValue: number;
}

interface MarketChartProps {
  data: MarketDataPoint[];
}

export const MarketChart = ({ data }: MarketChartProps) => {
  const chartRef = useRef<HTMLDivElement>(null);
  const chartInstance = useRef<ECharts | null>(null);
  const [isInitialized, setIsInitialized] = useState(false);

  // Initialize chart
  useEffect(() => {
    if (!chartRef.current) return;

    chartInstance.current = echarts.init(chartRef.current);
    setIsInitialized(true);

    // Cleanup on unmount
    return () => {
      chartInstance.current?.dispose();
    };
  }, []);

  // Update chart when data changes
  useEffect(() => {
    if (!chartInstance.current || !isInitialized) return;

    // Show all data points - chart will "zoom out" as more data comes in
    const displayData = data;

    // Prepare data for chart
    const timestamps = displayData.map(d => {
      const date = new Date(d.timestamp * 1000);
      return date.toLocaleTimeString();
    });
    const vn30Values = displayData.map(d => d.vn30Value.toFixed(2));
    const f1Values = displayData.map(d => d.hnxValue.toFixed(2));

    const option: echarts.EChartsOption = {
      title: {
        text: 'Market Data Stream',
        left: 'center',
        textStyle: {
          color: '#333',
          fontSize: 20,
          fontWeight: 'bold',
        },
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
        },
      },
      legend: {
        data: ['VN30 Index', 'F1'],
        top: 40,
      },
      grid: {
        left: '3%',
        right: '3%',
        bottom: '10%',
        top: '15%',
        containLabel: true,
      },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: timestamps,
        axisLabel: {
          rotate: 45,
          // Show ~20 labels evenly distributed across the timeline
          interval: Math.max(Math.floor(timestamps.length / 20), 0),
        },
      },
      yAxis: {
        type: 'value',
        name: 'Index Value',
        position: 'left',
        scale: true,
        axisLabel: {
          formatter: '{value}',
        },
      },
      series: [
        {
          name: 'VN30 Index',
          type: 'line',
          data: vn30Values,
          smooth: true,
          symbol: 'none',
          lineStyle: {
            width: 2,
            color: '#5470c6',
          },
          areaStyle: {
            opacity: 0.1,
            color: '#5470c6',
          },
        },
        {
          name: 'F1',
          type: 'line',
          data: f1Values,
          smooth: true,
          symbol: 'none',
          lineStyle: {
            width: 2,
            color: '#91cc75',
          },
          areaStyle: {
            opacity: 0.1,
            color: '#91cc75',
          },
        },
      ],
      animation: false, // Disable animation for real-time updates
    };

    chartInstance.current.setOption(option);
  }, [data, isInitialized]);

  // Handle window resize
  useEffect(() => {
    const handleResize = () => {
      chartInstance.current?.resize();
    };

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  return (
    <div
      ref={chartRef}
      style={{
        width: '100%',
        height: '600px',
      }}
    />
  );
};
