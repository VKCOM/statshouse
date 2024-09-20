export function fixMessageTrouble(search: string): string {
  if (search.replaceAll) {
    return search.replaceAll('+', '%20').replace(/\.$/gi, '%2E');
  }
  return search.replace(/\+/gi, '%20').replace(/\.$/gi, '%2E');
}
