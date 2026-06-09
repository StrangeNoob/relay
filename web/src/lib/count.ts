// clampCount parses the enqueue-form count field: floors to an integer and clamps
// to [1, max], defaulting to 1 on empty/non-numeric input.
export function clampCount(raw: string, max = 10000): number {
  const n = Math.floor(Number(raw));
  if (!Number.isFinite(n) || n < 1) return 1;
  return n > max ? max : n;
}
