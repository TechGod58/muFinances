import { createDefaultApiClient } from './core/api.js';
import { uiState } from './core/ui-state.js';
import { isLoading, setLoading, showError, withLoading } from './core/loading.js';

window.muFinances = window.muFinances || {};
window.muFinances.api = window.muFinances.api || createDefaultApiClient();
window.muFinances.uiState = window.muFinances.uiState || uiState;
window.muFinances.loading = window.muFinances.loading || {
  isLoading,
  setLoading,
  showError,
  withLoading,
};
