function readJson(key, fallback) {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch {
    return fallback;
  }
}

function writeJson(key, value) {
  localStorage.setItem(key, JSON.stringify(value));
}

export class UiStateStore {
  constructor(namespace = 'mufinances.ui') {
    this.namespace = namespace;
  }

  key(name) {
    return `${this.namespace}.${name}`;
  }

  get(name, fallback = null) {
    return readJson(this.key(name), fallback);
  }

  set(name, value) {
    writeJson(this.key(name), value);
    window.dispatchEvent(new CustomEvent('mufinances:ui-state-change', {
      detail: { name, value },
    }));
  }

  update(name, updater, fallback = null) {
    const next = updater(this.get(name, fallback));
    this.set(name, next);
    return next;
  }
}

export const uiState = new UiStateStore();

