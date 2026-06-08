interface SparklineProps {
  data: number[];
  stroke: string;
  fill?: string;
  height?: number;
}

// Sparkline draws a normalized polyline (and optional area) from data points.
export function Sparkline({ data, stroke, fill, height = 86 }: SparklineProps) {
  const w = 320;
  if (data.length < 2) {
    return <svg className="spark" viewBox={`0 0 ${w} ${height}`} preserveAspectRatio="none" />;
  }
  const max = Math.max(...data, 1);
  const min = Math.min(...data, 0);
  const span = max - min || 1;
  const stepX = w / (data.length - 1);
  const pts = data.map((v, i) => {
    const x = i * stepX;
    const y = height - ((v - min) / span) * (height - 6) - 3;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  const line = pts.join(" ");
  return (
    <svg className="spark" viewBox={`0 0 ${w} ${height}`} preserveAspectRatio="none">
      {fill && <polyline fill={fill} stroke="none" points={`0,${height} ${line} ${w},${height}`} />}
      <polyline fill="none" stroke={stroke} strokeWidth={2} points={line} />
    </svg>
  );
}
