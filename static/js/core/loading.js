const busyScopes = new Map();

function emitLoadingChange(scope, value) {
  window.dispatchEvent(new CustomEvent('mufinances:loading-change', {
    detail: { scope, loading: value },
  }));
}

export function setLoading(scope, value) {
  const name = scope || 'app';
  busyScopes.set(name, Boolean(value));
  emitLoadingChange(name, Boolean(value));
}

export function isLoading(scope = 'app') {
  return Boolean(busyScopes.get(scope));
}

export async function withLoading(scope, task) {
  setLoading(scope, true);
  try {
    return await task();
  } finally {
    setLoading(scope, false);
  }
}

export function showError(error, options = {}) {
  const detail = {
    scope: options.scope || 'app',
    message: error?.message || String(error || 'Something went wrong'),
    error,
  };
  window.dispatchEvent(new CustomEvent('mufinances:error', { detail }));
  return detail;
}

