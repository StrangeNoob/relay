// formatCount abbreviates large counts (1240 -> "1.2k", 2_500_000 -> "2.5M").
export function formatCount(n: number): string {
  if (n < 1000) return String(n);
  if (n < 1_000_000) return trim(n / 1000) + "k";
  return trim(n / 1_000_000) + "M";
}

function trim(x: number): string {
  return x.toFixed(1).replace(/\.0$/, "");
}

// formatAge renders an elapsed duration in ms as a coarse age ("5s", "1m", "1h").
export function formatAge(ms: number): string {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  return `${h}h`;
}
