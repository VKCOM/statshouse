const appVersion = localStorage.getItem('appVersion');
export function appVersionToggle() {
  if (appVersion) {
    localStorage.removeItem('appVersion');
  } else {
    localStorage.setItem('appVersion', '1');
  }
  document.location.reload();
}
