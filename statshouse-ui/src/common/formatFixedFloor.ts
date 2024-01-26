export function formatFixedFloor(n: number, maxFrac: number): string {
  const k = Math.pow(10, maxFrac);
  return (Math.floor(n * k) / k).toString();
}
