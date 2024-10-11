export function getUrlObject(search: string): { hash?: string; search?: string } {
  if (search.length > 5000) {
    return { hash: search };
  } else {
    return { search };
  }
}
