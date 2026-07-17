import type { ThroughputPoint } from "../types";

interface Props {
  data: ThroughputPoint[];
}

export function ThroughputChart({ data }: Props) {
  if (data.length === 0) {
    return (
      <div className="chart-wrap" style={{ display: "flex", alignItems: "center", justifyContent: "center" }}>
        <span style={{ color: "var(--text-4)", fontSize: 11 }}>waiting for data...</span>
      </div>
    );
  }

  const values = data.map((point) => point.value);
  const maxValue = Math.max(...values, 1);
  const minValue = Math.min(...values, 0);
  const range = maxValue - minValue || 1;

  const points = data.map((point, index) => {
    const x = data.length === 1 ? 50 : (index / (data.length - 1)) * 100;
    const normalized = (point.value - minValue) / range;
    const y = 100 - normalized * 100;
    return `${x},${y}`;
  });

  const areaPath = `M ${points.join(" L ")} L 100,100 L 0,100 Z`;

  return (
    <div className="chart-wrap">
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" style={{ width: "100%", height: "100%" }}>
        <path d={areaPath} fill="rgba(167, 139, 250, 0.16)" />
        <polyline points={points.join(" ")} fill="none" stroke="#a78bfa" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
        {data.map((point, index) => {
          const x = data.length === 1 ? 50 : (index / (data.length - 1)) * 100;
          const normalized = (point.value - minValue) / range;
          const y = 100 - normalized * 100;
          return <circle key={`${point.time}-${index}`} cx={x} cy={y} r="1.2" fill="#a78bfa" />;
        })}
      </svg>
    </div>
  );
}