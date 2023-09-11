export function formatFixed(n: number, maxFrac: number): string {
  const k = Math.pow(10, maxFrac);
  return (Math.round(n * k) / k).toString();
}
