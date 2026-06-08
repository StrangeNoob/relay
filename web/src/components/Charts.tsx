import { Sparkline } from "./Sparkline";

interface ChartsProps { depth: number[]; throughput: number[]; }

export function Charts({ depth, throughput }: ChartsProps) {
  return (
    <div className="charts">
      <div className="panel">
        <div className="cap"><h3>Queue depth</h3><span className="now">ready · last 60s</span></div>
        <Sparkline data={depth} stroke="#d2603f" fill="rgba(210,96,63,.12)" />
      </div>
      <div className="panel">
        <div className="cap"><h3>Throughput</h3><span className="now">processed / s</span></div>
        <Sparkline data={throughput} stroke="#cbb48e" />
      </div>
    </div>
  );
}
