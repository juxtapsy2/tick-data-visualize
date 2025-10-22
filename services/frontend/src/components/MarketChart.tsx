
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
    const hnxValues = displayData.map(d => d.hnxValue.toFixed(2));

    // Calculate dynamic y-axis ranges with padding (5x zoom = tighter range)
    const calculateAxisRange = (values: number[]) => {
      if (values.length === 0 || values.every(v => v === 0)) {
        return { min: 0, max: 100 }; // Default range
      }

      const validValues = values.filter(v => v > 0);
      if (validValues.length === 0) {
        return { min: 0, max: 100 }; // Default range
      }

      const min = Math.min(...validValues);
      const max = Math.max(...validValues);
      const range = max - min;

      // Aggressive zoom: 15% padding for better visibility of small variations
      // This creates a 5x tighter view compared to default auto-scaling
      const padding = Math.max(range * 0.15, 2);

      return {
        min: Math.floor(min - padding),
        max: Math.ceil(max + padding),
      };
    };

    const vn30NumericValues = displayData.map(d => d.vn30Value);
    const hnxNumericValues = displayData.map(d => d.hnxValue);

    const vn30Range = calculateAxisRange(vn30NumericValues);
    const hnxRange = calculateAxisRange(hnxNumericValues);

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
        data: ['VN30 Index', '41I1FA000'],
        top: 40,
      },
      grid: {
        left: '3%',
        right: '4%',
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
      yAxis: [
        {
          type: 'value',
          name: 'VN30',
          position: 'left',
          min: vn30Range.min,
          max: vn30Range.max,
          axisLabel: {
            formatter: '{value}',
          },
        },
        {
          type: 'value',
          name: '41I1FA000',
          position: 'right',
          min: hnxRange.min,
          max: hnxRange.max,
          axisLabel: {
            formatter: '{value}',
          },
        },
      ],
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
          yAxisIndex: 0,
        },
        {
          name: '41I1FA000',
          type: 'line',
          data: hnxValues,
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
          yAxisIndex: 1,
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
