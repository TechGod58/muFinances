const DEFAULT_HEADERS = {
  Accept: 'application/json',
};

function joinUrl(baseUrl, path) {
  const base = String(baseUrl || '').replace(/\/+$/, '');
  const suffix = String(path || '').replace(/^\/+/, '');
  return suffix ? `${base}/${suffix}` : base || '/';
}

export class ApiError extends Error {
  constructor(message, options = {}) {
    super(message);
    this.name = 'ApiError';
    this.status = options.status || 0;
    this.payload = options.payload || null;
  }
}

export class ApiClient {
  constructor(options = {}) {
    this.baseUrl = options.baseUrl || '';
    this.getToken = options.getToken || (() => null);
  }

  async request(path, options = {}) {
    const headers = {
      ...DEFAULT_HEADERS,
      ...(options.body === undefined ? {} : { 'Content-Type': 'application/json' }),
      ...(options.headers || {}),
    };

    const token = this.getToken();
    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }

    const response = await fetch(joinUrl(this.baseUrl, path), {
      ...options,
      headers,
      body: options.body === undefined || typeof options.body === 'string'
        ? options.body
        : JSON.stringify(options.body),
    });

    const contentType = response.headers.get('content-type') || '';
    const payload = contentType.includes('application/json')
      ? await response.json().catch(() => null)
      : await response.text().catch(() => '');

    if (!response.ok) {
      const message = payload && typeof payload === 'object' && payload.detail
        ? payload.detail
        : `Request failed with status ${response.status}`;
      throw new ApiError(message, { status: response.status, payload });
    }

    return payload;
  }

  get(path, options = {}) {
    return this.request(path, { ...options, method: 'GET' });
  }

  post(path, body, options = {}) {
    return this.request(path, { ...options, method: 'POST', body });
  }

  put(path, body, options = {}) {
    return this.request(path, { ...options, method: 'PUT', body });
  }

  delete(path, options = {}) {
    return this.request(path, { ...options, method: 'DELETE' });
  }
}

export function createDefaultApiClient() {
  return new ApiClient({
    getToken: () => window.muFinancesState?.token || window.state?.token || localStorage.getItem('mufinances.token'),
  });
}

