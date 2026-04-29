const state = {
  scenarios: [],
  activeScenarioId: null,
  bootstrap: null,
  token: window.localStorage.getItem('mufinances.token'),
  sessionMode: window.sessionStorage.getItem('mufinances.sessionMode') || 'bearer',
  csrfToken: window.sessionStorage.getItem('mufinances.csrf') || '',
  activePeriod: window.localStorage.getItem('mufinances.period') || null,
  commandDeckCollapsed: window.localStorage.getItem('mufinances.commandDeckCollapsed') === 'true',
  marketLab: null,
  marketSearch: null,
  brokerage: null,
  ui: {
    pendingRequests: 0,
    lastError: null,
  },
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

const escapeHtml = (value) =>
  String(value ?? '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);

const safeClassToken = (value) => String(value ?? '').toLowerCase().replace(/[^a-z0-9_-]/g, '-');

const safeHref = (value) => {
  const href = String(value ?? '#');
  return href.startsWith('#') || href.startsWith('/') ? href : '#';
};

const setText = (selector, value) => {
  const el = typeof selector === 'string' ? $(selector) : selector;
  if (el) el.textContent = value ?? '';
};

const setBusy = (label = 'Loading') => {
  const busy = state.ui.pendingRequests > 0;
  document.body.classList.toggle('is-loading', busy);
  const status = $('#appStatus');
  if (status) {
    status.textContent = busy ? `${label}...` : state.ui.lastError || 'Ready';
    status.dataset.state = busy ? 'loading' : state.ui.lastError ? 'error' : 'ready';
  }
};

const toast = (message) => {
  const el = $('#toast');
  el.textContent = message;
  el.setAttribute('aria-label', message);
  el.classList.remove('hidden');
  window.clearTimeout(el._timeout);
  el._timeout = window.setTimeout(() => el.classList.add('hidden'), 2800);
};

const currency = (value) =>
  new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(value);

const statusPill = (value) => `<span class="status-pill ${safeClassToken(value)}">${escapeHtml(value)}</span>`;

const renderTable = (columns, rows, formatters = {}, caption = 'Data table') => {
  const safeCaption = escapeHtml(caption);
  const thead = `<thead><tr>${columns.map((col) => `<th scope="col">${escapeHtml(col.label)}</th>`).join('')}</tr></thead>`;
  const tbody = rows
    .map(
      (row) =>
        `<tr>${columns
          .map((col) => {
            const value = row[col.key];
            const formatter = formatters[col.key];
            const rendered = formatter ? formatter(value, row) : escapeHtml(value);
            return `<td>${rendered ?? ''}</td>`;
          })
          .join('')}</tr>`,
    )
    .join('');
  return `<div class="table-wrap" role="region" tabindex="0" aria-label="${safeCaption}"><table><caption class="sr-only">${safeCaption}</caption>${thead}<tbody>${tbody}</tbody></table></div>`;
};

class ApiError extends Error {
  constructor(message, { status = 0, path = '', payload = {} } = {}) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.path = path;
    this.payload = payload;
  }
}

const api = {
  url(path, params = {}) {
    const url = new URL(path, window.location.origin);
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') url.searchParams.set(key, value);
    });
    return `${url.pathname}${url.search}`;
  },
  async request(path, { method = 'GET', body = null, loadingLabel = 'Loading' } = {}) {
    state.ui.pendingRequests += 1;
    state.ui.lastError = null;
    setBusy(loadingLabel);
    try {
      const response = await fetch(path, {
        method,
        headers: body ? { 'Content-Type': 'application/json', ...authHeaders() } : authHeaders(),
        credentials: 'same-origin',
        body: body ? JSON.stringify(body) : null,
      });
      if (!response.ok) {
        if (response.status === 401) {
          clearSession();
          showAuthGate();
        }
        const payload = await response.json().catch(() => ({}));
        if (payload.code === 'password_change_required') {
          $('#passwordChangeDialog').showModal();
        }
        const error = new ApiError(payload.detail || `${method} ${path} failed`, { status: response.status, path, payload });
        state.ui.lastError = error.message;
        throw error;
      }
      return await response.json();
    } catch (error) {
      state.ui.lastError = error.message || 'Request failed';
      throw error;
    } finally {
      state.ui.pendingRequests = Math.max(0, state.ui.pendingRequests - 1);
      setBusy(loadingLabel);
    }
  },
  async get(path, params = null) {
    return this.request(params ? this.url(path, params) : path, { method: 'GET' });
  },
  async post(path, body) {
    return this.request(path, { method: 'POST', body, loadingLabel: 'Saving' });
  },
};

function authHeaders() {
  const headers = {};
  if (state.token) headers.Authorization = `Bearer ${state.token}`;
  if (state.csrfToken) headers['X-CSRF-Token'] = state.csrfToken;
  return headers;
}

async function downloadArtifact(url, fileName = 'mufinances-artifact') {
  const response = await fetch(url, { headers: authHeaders(), credentials: 'same-origin' });
  if (!response.ok) {
    throw new Error(`Download failed (${response.status})`);
  }
  const blob = await response.blob();
  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = objectUrl;
  anchor.download = fileName;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(objectUrl);
}

function showAuthGate() {
  $('#authGate').classList.remove('hidden');
  $('#appShell').classList.add('hidden');
}

function clearSession() {
  state.token = null;
  state.sessionMode = 'bearer';
  state.csrfToken = '';
  window.localStorage.removeItem('mufinances.token');
  window.sessionStorage.removeItem('mufinances.sessionMode');
  window.sessionStorage.removeItem('mufinances.csrf');
}

function showAppShell() {
  $('#authGate').classList.add('hidden');
  $('#appShell').classList.remove('hidden');
  applyCommandDeckState();
  applyRouteFromHash();
}

function applyCommandDeckState() {
  const shell = $('#appShell');
  const toggle = $('#commandDeckToggle');
  if (!shell || !toggle) return;
  const collapsed = state.commandDeckCollapsed;
  shell.classList.toggle('deck-collapsed', collapsed);
  toggle.setAttribute('aria-expanded', String(!collapsed));
  toggle.textContent = collapsed ? 'Open' : 'Hide';
}

function toggleCommandDeck() {
  state.commandDeckCollapsed = !state.commandDeckCollapsed;
  window.localStorage.setItem('mufinances.commandDeckCollapsed', String(state.commandDeckCollapsed));
  applyCommandDeckState();
}

function applyRouteFromHash() {
  const hash = window.location.hash || '#overview';
  const target = document.getElementById(hash.slice(1));
  $$('nav[aria-label="Primary sections"] a').forEach((link) => {
    const active = link.getAttribute('href') === hash;
    if (active) {
      link.setAttribute('aria-current', 'true');
    } else {
      link.removeAttribute('aria-current');
    }
    link.classList.toggle('active-route', active);
  });
  if (!target) return;
  target.classList.add('route-target');
  window.setTimeout(() => target.classList.remove('route-target'), 1200);
  if (!document.body.classList.contains('is-loading')) {
    target.scrollIntoView({ block: 'start' });
  }
}

async function handleLogin(formData) {
  const payload = Object.fromEntries(formData.entries());
  const response = await fetch('/api/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'same-origin',
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(error.detail || 'Sign in failed.');
  }
  const result = await response.json();
  state.sessionMode = result.session_mode || (result.token ? 'bearer' : 'cookie');
  state.csrfToken = result.csrf_token || '';
  window.sessionStorage.setItem('mufinances.sessionMode', state.sessionMode);
  if (state.csrfToken) window.sessionStorage.setItem('mufinances.csrf', state.csrfToken);
  if (state.sessionMode === 'cookie') {
    state.token = null;
    window.localStorage.removeItem('mufinances.token');
  } else {
    state.token = result.token;
    window.localStorage.setItem('mufinances.token', result.token);
  }
  if (result.user?.must_change_password) {
    $('#passwordChangeDialog').showModal();
    return;
  }
  await loadBootstrap();
  showAppShell();
}

async function handlePasswordChange(formData) {
  const payload = Object.fromEntries(formData.entries());
  await api.post('/api/auth/password', payload);
  $('#passwordChangeDialog').close();
  toast('Password updated.');
  await loadBootstrap();
  showAppShell();
}

async function validateSession() {
  if (!state.token && state.sessionMode !== 'cookie') return false;
  const response = await fetch('/api/auth/me', { headers: authHeaders(), credentials: 'same-origin' });
  if (!response.ok) {
    clearSession();
    return false;
  }
  return true;
}

async function loadAuthBootstrap() {
  const response = await fetch('/api/auth/bootstrap');
  if (!response.ok) return;
  const payload = await response.json();
  const panel = $('#loginForm');
  let hint = panel.querySelector('.sso-hint');
  if (!hint) {
    hint = document.createElement('p');
    hint.className = 'muted sso-hint';
    panel.insertBefore(hint, panel.querySelector('button'));
  }
  hint.textContent = payload.sso?.enabled
    ? `SSO is configured through ${payload.sso.name}.`
    : 'Local sign-in is active. SSO endpoints are ready for server configuration.';
}

async function loadBootstrap(scenarioId = null) {
  const bootstrap = await api.get('/api/bootstrap');
  bootstrap.me = await api.get('/api/auth/me');
  state.bootstrap = bootstrap;
  state.scenarios = bootstrap.scenarios;
  state.activeScenarioId = scenarioId || bootstrap.activeScenario?.id || null;

  if (state.activeScenarioId) {
    bootstrap.summary = await api.get(`/api/reports/summary?scenario_id=${state.activeScenarioId}`);
    bootstrap.ledgerBasis = await api.get(`/api/ledger-depth/basis-summary?scenario_id=${state.activeScenarioId}`);
    bootstrap.journals = await api.get(`/api/ledger-depth/journals?scenario_id=${state.activeScenarioId}`);
    bootstrap.lineItems = await api.get(`/api/scenarios/${state.activeScenarioId}/line-items`);
    bootstrap.drivers = await api.get(`/api/scenarios/${state.activeScenarioId}/drivers`);
    bootstrap.workflows = await api.get(`/api/workflows?scenario_id=${state.activeScenarioId}`);
    bootstrap.workflowOrchestration = await api.get(`/api/workflow-designer/workspace?scenario_id=${state.activeScenarioId}`);
    bootstrap.operatingBudget = await api.get(`/api/operating-budget/submissions?scenario_id=${state.activeScenarioId}`);
    bootstrap.budgetAssumptions = await api.get(`/api/operating-budget/assumptions?scenario_id=${state.activeScenarioId}`);
    bootstrap.budgetTransfers = await api.get(`/api/operating-budget/transfers?scenario_id=${state.activeScenarioId}`);
    bootstrap.enrollmentTerms = await api.get(`/api/enrollment/terms?scenario_id=${state.activeScenarioId}`);
    bootstrap.tuitionRates = await api.get(`/api/enrollment/tuition-rates?scenario_id=${state.activeScenarioId}`);
    bootstrap.enrollmentInputs = await api.get(`/api/enrollment/forecast-inputs?scenario_id=${state.activeScenarioId}`);
    bootstrap.tuitionRuns = await api.get(`/api/enrollment/tuition-forecast-runs?scenario_id=${state.activeScenarioId}`);
    bootstrap.positions = await api.get(`/api/campus-planning/positions?scenario_id=${state.activeScenarioId}`);
    bootstrap.facultyLoads = await api.get(`/api/campus-planning/faculty-loads?scenario_id=${state.activeScenarioId}`);
    bootstrap.grants = await api.get(`/api/campus-planning/grants?scenario_id=${state.activeScenarioId}`);
    bootstrap.capitalRequests = await api.get(`/api/campus-planning/capital-requests?scenario_id=${state.activeScenarioId}`);
    bootstrap.typedDrivers = await api.get(`/api/scenario-engine/drivers?scenario_id=${state.activeScenarioId}`);
    bootstrap.scenarioForecastRuns = await api.get(`/api/scenario-engine/forecast-runs?scenario_id=${state.activeScenarioId}`);
    bootstrap.forecastVariances = await api.get(`/api/scenario-engine/forecast-actual-variances?scenario_id=${state.activeScenarioId}`);
    bootstrap.driverGraph = await api.get(`/api/scenario-engine/driver-graph?scenario_id=${state.activeScenarioId}`);
    bootstrap.predictiveForecasting = await api.get(`/api/scenario-engine/predictive-workspace?scenario_id=${state.activeScenarioId}`);
    bootstrap.planningModels = await api.get(`/api/model-builder/models?scenario_id=${state.activeScenarioId}`);
    const firstModel = bootstrap.planningModels?.models?.[0];
    bootstrap.modelFormulas = firstModel ? await api.get(`/api/model-builder/models/${firstModel.id}/formulas`) : { formulas: [] };
    bootstrap.allocationRules = firstModel ? await api.get(`/api/model-builder/models/${firstModel.id}/allocation-rules`) : { allocation_rules: [] };
    bootstrap.modelRuns = firstModel ? await api.get(`/api/model-builder/models/${firstModel.id}/recalculation-runs`) : { runs: [] };
    bootstrap.modelGraph = firstModel ? await api.get(`/api/model-builder/models/${firstModel.id}/dependency-graph`) : { edges: [], cycles: [] };
    bootstrap.enterpriseModel = firstModel ? await api.get(`/api/model-builder/models/${firstModel.id}/enterprise-workspace`) : { cube: { dimensions: [] }, calculation_order: { steps: [] }, versions: [], invalidations: [], performance_tests: [] };
    bootstrap.profitability = await api.get(`/api/profitability/workspace?scenario_id=${state.activeScenarioId}`);
    bootstrap.reportDefinitions = await api.get('/api/reporting/reports');
    bootstrap.dashboardWidgets = await api.get(`/api/reporting/widgets?scenario_id=${state.activeScenarioId}`);
    bootstrap.reportExports = await api.get('/api/reporting/exports');
    bootstrap.financialStatement = await api.get(`/api/reporting/financial-statement?scenario_id=${state.activeScenarioId}`);
    bootstrap.abfVariance = await api.get(`/api/reporting/actual-budget-forecast-variance?scenario_id=${state.activeScenarioId}`);
    bootstrap.boardPackages = await api.get(`/api/reporting/board-packages?scenario_id=${state.activeScenarioId}`);
    bootstrap.exportArtifacts = await api.get(`/api/reporting/artifacts?scenario_id=${state.activeScenarioId}`);
    bootstrap.reportSnapshots = await api.get(`/api/reporting/snapshots?scenario_id=${state.activeScenarioId}`);
    bootstrap.scheduledExtractRuns = await api.get(`/api/reporting/scheduled-extract-runs?scenario_id=${state.activeScenarioId}`);
    bootstrap.biApiManifest = await api.get(`/api/reporting/bi-api-manifest?scenario_id=${state.activeScenarioId}`);
    bootstrap.productionReporting = await api.get(`/api/reporting/production-polish/workspace?scenario_id=${state.activeScenarioId}`);
    bootstrap.chartRendering = await api.get(`/api/reporting/chart-rendering/workspace?scenario_id=${state.activeScenarioId}`);
    bootstrap.productionPdf = await api.get(`/api/reporting/production-pdf/workspace?scenario_id=${state.activeScenarioId}`);
    bootstrap.varianceThresholds = await api.get(`/api/reporting/variance-thresholds?scenario_id=${state.activeScenarioId}`);
    bootstrap.varianceExplanations = await api.get(`/api/reporting/variance-explanations?scenario_id=${state.activeScenarioId}`);
    bootstrap.narrativeReports = await api.get(`/api/reporting/narratives?scenario_id=${state.activeScenarioId}`);
    bootstrap.closeChecklists = await api.get(`/api/close/checklists?scenario_id=${state.activeScenarioId}`);
    bootstrap.closeTemplates = await api.get('/api/close/templates');
    bootstrap.closeDependencies = await api.get(`/api/close/task-dependencies?scenario_id=${state.activeScenarioId}`);
    bootstrap.closeCalendar = await api.get(`/api/close/calendar?scenario_id=${state.activeScenarioId}`);
    bootstrap.reconciliations = await api.get(`/api/close/reconciliations?scenario_id=${state.activeScenarioId}`);
    bootstrap.reconciliationExceptions = await api.get(`/api/close/reconciliation-exceptions?scenario_id=${state.activeScenarioId}`);
    bootstrap.entityConfirmations = await api.get(`/api/close/entity-confirmations?scenario_id=${state.activeScenarioId}`);
    bootstrap.consolidationEntities = await api.get('/api/close/consolidation-entities');
    bootstrap.entityOwnerships = await api.get(`/api/close/entity-ownerships?scenario_id=${state.activeScenarioId}`);
    bootstrap.consolidationSettings = await api.get(`/api/close/consolidation-settings?scenario_id=${state.activeScenarioId}`);
    bootstrap.intercompanyMatches = await api.get(`/api/close/intercompany-matches?scenario_id=${state.activeScenarioId}`);
    bootstrap.eliminations = await api.get(`/api/close/eliminations?scenario_id=${state.activeScenarioId}`);
    bootstrap.consolidationRuns = await api.get(`/api/close/consolidation-runs?scenario_id=${state.activeScenarioId}`);
    bootstrap.auditPackets = await api.get(`/api/close/audit-packets?scenario_id=${state.activeScenarioId}`);
    bootstrap.consolidationAuditReports = await api.get(`/api/close/consolidation-audit-reports?scenario_id=${state.activeScenarioId}`);
    bootstrap.consolidationRules = await api.get(`/api/close/consolidation-rules?scenario_id=${state.activeScenarioId}`);
    bootstrap.ownershipChains = await api.get(`/api/close/ownership-chain-calculations?scenario_id=${state.activeScenarioId}`);
    bootstrap.currencyTranslationAdjustments = await api.get(`/api/close/currency-translation-adjustments?scenario_id=${state.activeScenarioId}`);
    bootstrap.statutoryPacks = await api.get(`/api/close/statutory-packs?scenario_id=${state.activeScenarioId}`);
    bootstrap.supplementalSchedules = await api.get(`/api/close/supplemental-schedules?scenario_id=${state.activeScenarioId}`);
    bootstrap.connectors = await api.get('/api/integrations/connectors');
    bootstrap.connectorMarketplace = await api.get('/api/integrations/marketplace');
    bootstrap.connectorHealth = await api.get('/api/integrations/health');
    bootstrap.importBatches = await api.get(`/api/integrations/imports?scenario_id=${state.activeScenarioId}`);
    bootstrap.importRejections = await api.get('/api/integrations/rejections');
    bootstrap.stagingBatches = await api.get(`/api/integrations/staging?scenario_id=${state.activeScenarioId}`);
    const firstStagingBatch = bootstrap.stagingBatches?.staging_batches?.[0];
    bootstrap.stagingRows = firstStagingBatch ? await api.get(`/api/integrations/staging/${firstStagingBatch.id}/rows`) : { rows: [] };
    bootstrap.mappingTemplates = await api.get('/api/integrations/mapping-templates');
    bootstrap.validationRules = await api.get('/api/integrations/validation-rules');
    bootstrap.credentials = await api.get('/api/integrations/credentials');
    bootstrap.retryEvents = await api.get('/api/integrations/retry-events');
    bootstrap.syncLogs = await api.get('/api/integrations/sync-logs');
    bootstrap.bankingCashImports = await api.get(`/api/integrations/banking-cash-imports?scenario_id=${state.activeScenarioId}`);
    bootstrap.crmEnrollmentImports = await api.get(`/api/integrations/crm-enrollment-imports?scenario_id=${state.activeScenarioId}`);
    bootstrap.syncJobs = await api.get('/api/integrations/sync-jobs');
    bootstrap.powerBiExports = await api.get(`/api/integrations/powerbi-exports?scenario_id=${state.activeScenarioId}`);
    bootstrap.dataHub = await api.get(`/api/data-hub/workspace?scenario_id=${state.activeScenarioId}`);
    bootstrap.automationRecommendations = await api.get(`/api/automation/recommendations?scenario_id=${state.activeScenarioId}`);
    bootstrap.automationGates = await api.get(`/api/automation/approval-gates?scenario_id=${state.activeScenarioId}`);
    bootstrap.agentPrompts = await api.get(`/api/automation/planning-agents/prompts?scenario_id=${state.activeScenarioId}`);
    bootstrap.agentActions = await api.get(`/api/automation/planning-agents/actions?scenario_id=${state.activeScenarioId}`);
    bootstrap.universityAgent = await api.get('/api/university-agent/workspace');
    bootstrap.aiExplanations = await api.get(`/api/ai-explainability/explanations?scenario_id=${state.activeScenarioId}`);
    bootstrap.workspaces = await api.get(`/api/workspaces?scenario_id=${state.activeScenarioId}`);
    bootstrap.guidanceTraining = await api.get(`/api/guidance/workspace?scenario_id=${state.activeScenarioId}`);
    bootstrap.operationsSummary = await api.get('/api/operations/summary');
    bootstrap.opsChecks = await api.get('/api/operations/checks');
    bootstrap.restoreTests = await api.get('/api/operations/restore-tests');
    bootstrap.runbooks = await api.get('/api/operations/runbooks');
    bootstrap.observability = await api.get('/api/observability/workspace');
    bootstrap.performanceReliability = await api.get(`/api/performance/workspace?scenario_id=${state.activeScenarioId}`);
    bootstrap.parallelCubed = await api.get(`/api/performance/parallel-cubed/workspace?scenario_id=${state.activeScenarioId}`);
    bootstrap.deploymentGovernance = await api.get('/api/deployment-governance/workspace');
    bootstrap.complianceStatus = await api.get('/api/compliance/status');
    bootstrap.auditVerification = await api.get('/api/compliance/audit/verify');
    bootstrap.sodReport = await api.get('/api/compliance/sod-report');
    bootstrap.retentionPolicies = await api.get('/api/compliance/retention-policies');
    bootstrap.certifications = await api.get(`/api/compliance/certifications?scenario_id=${state.activeScenarioId}`);
    bootstrap.taxCompliance = await api.get(`/api/compliance/tax/workspace?scenario_id=${state.activeScenarioId}`);
    bootstrap.enterpriseSecurity = await api.get('/api/security/enterprise-workspace');
    bootstrap.foundationBackups = await api.get('/api/foundation/backups');
    bootstrap.comments = await api.get('/api/evidence/comments');
    bootstrap.attachments = await api.get('/api/evidence/attachments');
    bootstrap.ux = await api.get(`/api/ux/bootstrap?scenario_id=${state.activeScenarioId}`);
    bootstrap.bulkPasteImports = await api.get(`/api/ux/bulk-paste?scenario_id=${state.activeScenarioId}`);
    bootstrap.officeWorkbooks = await api.get(`/api/office/workbooks?scenario_id=${state.activeScenarioId}`);
    bootstrap.officeImports = await api.get(`/api/office/roundtrip-imports?scenario_id=${state.activeScenarioId}`);
    bootstrap.officeNative = await api.get(`/api/office/native-workspace?scenario_id=${state.activeScenarioId}`);
    if (!state.activePeriod) {
      state.activePeriod = bootstrap.ux?.profile?.default_period || bootstrap.activeScenario?.start_period || null;
    }
  }

  renderScenarioSelect();
  renderProductivity();
  renderOfficeInterop();
  renderGuidanceTraining();
  renderWorkspaces();
  renderSummary();
  renderLedgerDepth();
  renderDetails();
  renderOperatingBudget();
  renderEnrollment();
  renderCampusPlanning();
  renderScenarioEngine();
  renderModelBuilder();
  renderProfitability();
  renderReporting();
  renderClose();
  renderDataHub();
  renderAutomation();
  renderAIExplainability();
  renderOperations();
  renderPerformanceReliability();
  renderDeploymentGovernance();
  renderCompliance();
  renderEnterpriseSecurity();
  renderEvidence();
  renderCapabilityMap();
}

function renderOfficeInterop() {
  $('#officeWorkbookTable').innerHTML = renderTable(
    [
      { key: 'workbook_type', label: 'Type' },
      { key: 'file_name', label: 'File' },
      { key: 'size_bytes', label: 'Bytes' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.officeWorkbooks?.workbooks || [],
    { status: (value) => statusPill(value) },
    'Office workbook history',
  );
  $('#officeImportTable').innerHTML = renderTable(
    [
      { key: 'workbook_key', label: 'Workbook' },
      { key: 'status', label: 'Status' },
      { key: 'accepted_rows', label: 'Accepted' },
      { key: 'rejected_rows', label: 'Rejected' },
    ],
    state.bootstrap.officeImports?.imports || [],
    { status: (value) => statusPill(value) },
    'Excel round trip import history',
  );
  $('#officeNamedRangeTable').innerHTML = renderTable(
    [
      { key: 'range_name', label: 'Name' },
      { key: 'sheet_name', label: 'Sheet' },
      { key: 'cell_ref', label: 'Cells' },
      { key: 'protected', label: 'Protected' },
    ],
    state.bootstrap.officeNative?.named_ranges || [],
    { protected: (value) => (value ? 'yes' : 'no') },
    'Excel named ranges',
  );
  $('#officeCellCommentTable').innerHTML = renderTable(
    [
      { key: 'sheet_name', label: 'Sheet' },
      { key: 'cell_ref', label: 'Cell' },
      { key: 'comment_text', label: 'Comment' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.officeNative?.cell_comments || [],
    { status: (value) => statusPill(value) },
    'Excel cell comments',
  );
  $('#officeActionTable').innerHTML = renderTable(
    [
      { key: 'action_type', label: 'Action' },
      { key: 'status', label: 'Status' },
      { key: 'message', label: 'Message' },
      { key: 'created_at', label: 'Created' },
    ],
    state.bootstrap.officeNative?.actions || [],
    { status: (value) => statusPill(value) },
    'Excel workspace actions',
  );
}

function renderScenarioSelect() {
  const select = $('#scenarioSelect');
  select.replaceChildren(
    ...state.scenarios.map((scenario) => {
      const option = document.createElement('option');
      option.value = String(scenario.id);
      option.textContent = `${scenario.name} - ${scenario.version}`;
      return option;
    }),
  );
  select.value = String(state.activeScenarioId || '');
}
function renderGuidanceTraining() {
  const payload = state.bootstrap.guidanceTraining || {};
  const role = payload.recommended_role || 'planner';
  $('#guidanceRoleSummary').textContent = `Recommended path: ${role}. Training content changes by role and current permissions.`;
  const checklistRows = (payload.checklists || []).flatMap((checklist) =>
    (checklist.tasks || []).map((task) => ({
      checklist_key: checklist.checklist_key,
      checklist: checklist.title,
      task_key: task.task_key,
      title: task.title,
      status: task.status,
      target: task.target,
    })),
  );
  $('#guidanceChecklistTable').innerHTML = renderTable(
    [
      { key: 'checklist', label: 'Checklist' },
      { key: 'title', label: 'Task' },
      { key: 'status', label: 'Status' },
      { key: 'target', label: 'Open' },
    ],
    checklistRows,
    {
      status: (value) => statusPill(value),
      target: (value) => value ? `<a href="${value}">Go</a>` : '',
    },
    'Guided task checklists',
  );
  $('#fieldHelpTable').innerHTML = renderTable(
    [
      { key: 'label', label: 'Field' },
      { key: 'help_text', label: 'Help' },
    ],
    payload.field_help || [],
    {},
    'Field help',
  );
  $('#processWalkthroughTable').innerHTML = renderTable(
    [
      { key: 'title', label: 'Walkthrough' },
      { key: 'role_key', label: 'Role' },
      { key: 'steps', label: 'Steps' },
    ],
    payload.walkthroughs || [],
    { steps: (value) => Array.isArray(value) ? value.join(' | ') : value },
    'Process walkthroughs',
  );
  $('#planningPlaybookTable').innerHTML = renderTable(
    [
      { key: 'title', label: 'Playbook' },
      { key: 'summary', label: 'Summary' },
      { key: 'sections', label: 'Sections' },
    ],
    payload.playbooks || [],
    { sections: (value) => Array.isArray(value) ? value.join(', ') : value },
    'Campus planning playbooks',
  );
  $('#trainingSessionTable').innerHTML = renderTable(
    [
      { key: 'mode_key', label: 'Mode' },
      { key: 'role_key', label: 'Role' },
      { key: 'status', label: 'Status' },
      { key: 'started_at', label: 'Started' },
    ],
    payload.training_sessions || [],
    { status: (value) => statusPill(value) },
    'Training mode sessions',
  );
}

function renderProductivity() {
  const ux = state.bootstrap.ux;
  if (!ux) return;
  const periodSelect = $('#periodSelect');
  if (periodSelect) {
    periodSelect.replaceChildren(
      ...(ux.periods || []).map((period) => {
        const option = document.createElement('option');
        option.value = period.period;
        option.textContent = `${period.period}${period.is_closed ? ' closed' : ''}`;
        return option;
      }),
    );
    periodSelect.value = state.activePeriod || '';
  }
  setText('#profileSummary', ux.profile ? `${ux.profile.display_name} | ${ux.profile.email || ''}` : '');
  $('#notificationCount').textContent = `${(ux.notifications || []).filter((item) => item.status === 'unread').length} unread`;
  $('#notificationTable').innerHTML = renderTable(
    [
      { key: 'severity', label: 'Severity' },
      { key: 'title', label: 'Title' },
      { key: 'message', label: 'Message' },
      { key: 'status', label: 'Status' },
    ],
    ux.notifications || [],
    {
      severity: (value) => statusPill(value),
      status: (value) => statusPill(value),
    },
  );
  $('#missingSubmissionTable').innerHTML = renderTable(
    [
      { key: 'department_code', label: 'Dept' },
      { key: 'department_name', label: 'Name' },
      { key: 'status', label: 'Status' },
      { key: 'line_count', label: 'Lines' },
    ],
    ux.missing_submissions?.rows || [],
    { status: (value) => statusPill(value) },
  );
  $('#departmentComparisonTable').innerHTML = renderTable(
    [
      { key: 'department_code', label: 'Dept' },
      { key: 'amount', label: 'Amount' },
      { key: 'variance_to_average', label: 'Vs avg' },
    ],
    ux.department_comparison?.rows || [],
    {
      amount: (value) => currency(value),
      variance_to_average: (value) => currency(value),
    },
  );
  $('#bulkPasteHistoryTable').innerHTML = renderTable(
    [
      { key: 'created_at', label: 'Created' },
      { key: 'status', label: 'Status' },
      { key: 'accepted_rows', label: 'Accepted' },
      { key: 'rejected_rows', label: 'Rejected' },
    ],
    state.bootstrap.bulkPasteImports?.imports || [],
    { status: (value) => statusPill(value) },
  );
}

function formatMetricValue(metric) {
  if (metric.kind === 'currency') return currency(metric.value);
  return new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(metric.value);
}

function renderWorkspaces() {
  const payload = state.bootstrap.workspaces;
  const container = $('#workspaceCards');
  if (!payload) {
    $('#workspaceCount').textContent = '0 visible';
    container.innerHTML = '';
    return;
  }
  $('#workspaceCount').textContent = `${payload.count} visible`;
  container.innerHTML = payload.workspaces
    .map(
      (workspace) => `
        <section class="panel workspace-card">
          <div class="section-header">
            <h3>${escapeHtml(workspace.title)}</h3>
            <span class="tag">${escapeHtml(String(workspace.key || '').replace('_', ' '))}</span>
          </div>
          <div class="grid two">
            ${workspace.metrics
              .map(
                (metric) => `
                  <div class="mini-metric">
                    <span class="label">${escapeHtml(metric.label)}</span>
                    <strong>${escapeHtml(formatMetricValue(metric))}</strong>
                  </div>
                `,
              )
              .join('')}
          </div>
          <h3>Work queue</h3>
          <div class="table-wrap">
            <table>
              <tbody>
                ${workspace.work_queue
                  .map((item) => `<tr><td><a href="${safeHref(item.href)}">${escapeHtml(item.label)}</a></td><td>${escapeHtml(item.count)}</td></tr>`)
                  .join('')}
              </tbody>
            </table>
          </div>
          <div class="hero-actions">
            ${workspace.quick_links.map((link) => `<a class="button-link" href="${safeHref(link.href)}">${escapeHtml(link.label)}</a>`).join('')}
          </div>
        </section>
      `,
    )
    .join('');
}

function renderSummary() {
  const summary = state.bootstrap.summary;
  const container = $('#summary');
  if (!summary) {
    container.innerHTML = '<section class="panel">No summary available.</section>';
    return;
  }

  container.innerHTML = [
    { label: 'Revenue', value: summary.revenue_total },
    { label: 'Expenses', value: summary.expense_total },
    { label: 'Net', value: summary.net_total },
  ]
    .map(
      (metric) => `
      <section class="panel metric">
        <span class="label">${metric.label}</span>
        <span class="value ${metric.value >= 0 ? 'positive' : 'negative'}">${currency(metric.value)}</span>
      </section>
    `,
    )
    .join('');

  const departmentRows = Object.entries(summary.by_department).map(([department, total]) => ({ department, total }));
  $('#departmentTable').innerHTML = renderTable(
    [
      { key: 'department', label: 'Department' },
      { key: 'total', label: 'Total' },
    ],
    departmentRows,
    {
      total: (value) => `<span class="${value >= 0 ? 'positive' : 'negative'}">${currency(value)}</span>`,
    },
  );

  const accountRows = Object.entries(summary.by_account).map(([account, total]) => ({ account, total }));
  $('#accountTable').innerHTML = renderTable(
    [
      { key: 'account', label: 'Account' },
      { key: 'total', label: 'Total' },
    ],
    accountRows,
    {
      total: (value) => `<span class="${value >= 0 ? 'positive' : 'negative'}">${currency(value)}</span>`,
    },
  );
}

function renderLedgerDepth() {
  $('#ledgerBasisTable').innerHTML = renderTable(
    [
      { key: 'ledger_basis', label: 'Basis' },
      { key: 'count', label: 'Rows' },
      { key: 'total', label: 'Total' },
    ],
    state.bootstrap.ledgerBasis?.basis || [],
    { total: (value) => `<span class="${value >= 0 ? 'positive' : 'negative'}">${currency(value)}</span>` },
  );

  $('#journalTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'account_code', label: 'Account' },
      { key: 'ledger_basis', label: 'Basis' },
      { key: 'amount', label: 'Amount' },
      { key: 'status', label: 'Status' },
      { key: 'reason', label: 'Reason' },
    ],
    state.bootstrap.journals?.journals || [],
    {
      amount: (value) => currency(value),
      status: (value) => statusPill(value),
    },
  );
}

function renderDetails() {
  const lineItems = state.bootstrap.lineItems || [];
  $('#lineItemCount').textContent = `${lineItems.length} rows`;
  $('#lineItemsTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'department_code', label: 'Dept' },
      { key: 'fund_code', label: 'Fund' },
      { key: 'account_code', label: 'Account' },
      { key: 'amount', label: 'Amount' },
      { key: 'source', label: 'Source' },
      { key: 'notes', label: 'Notes' },
    ],
    lineItems,
    {
      amount: (value) => `<span class="${value >= 0 ? 'positive' : 'negative'}">${currency(value)}</span>`,
    },
  );

  $('#driversTable').innerHTML = renderTable(
    [
      { key: 'driver_key', label: 'Key' },
      { key: 'label', label: 'Label' },
      { key: 'value', label: 'Value' },
      { key: 'expression', label: 'Expression' },
      { key: 'unit', label: 'Unit' },
    ],
    state.bootstrap.drivers || [],
  );

  $('#workflowTable').innerHTML = renderTable(
    [
      { key: 'name', label: 'Workflow' },
      { key: 'step', label: 'Step' },
      { key: 'status', label: 'Status' },
      { key: 'owner', label: 'Owner' },
      { key: 'updated_at', label: 'Updated' },
    ],
    state.bootstrap.workflows || [],
    {
      status: (value) => statusPill(value),
    },
  );

  const orchestration = state.bootstrap.workflowOrchestration || {};
  $('#workflowTemplateTable').innerHTML = renderTable(
    [
      { key: 'template_key', label: 'Template' },
      { key: 'name', label: 'Name' },
      { key: 'entity_type', label: 'Entity' },
      { key: 'active', label: 'Active' },
    ],
    orchestration.templates || [],
  );
  $('#workflowVisualTable').innerHTML = renderTable(
    [
      { key: 'template_id', label: 'Template' },
      { key: 'updated_at', label: 'Updated' },
      { key: 'created_by', label: 'By' },
    ],
    orchestration.visual_designs || [],
  );
  $('#processCalendarTable').innerHTML = renderTable(
    [
      { key: 'calendar_key', label: 'Calendar' },
      { key: 'process_type', label: 'Process' },
      { key: 'period', label: 'Period' },
      { key: 'status', label: 'Status' },
    ],
    orchestration.process_calendars || [],
  );
  $('#substituteApproverTable').innerHTML = renderTable(
    [
      { key: 'original_user_id', label: 'Original' },
      { key: 'substitute_user_id', label: 'Substitute' },
      { key: 'process_type', label: 'Process' },
      { key: 'active', label: 'Active' },
    ],
    orchestration.substitute_approvers || [],
  );
  $('#workflowCertificationPacketTable').innerHTML = renderTable(
    [
      { key: 'packet_key', label: 'Packet' },
      { key: 'process_type', label: 'Process' },
      { key: 'period', label: 'Period' },
      { key: 'status', label: 'Status' },
    ],
    orchestration.certification_packets || [],
  );
  $('#workflowCampaignMonitorTable').innerHTML = renderTable(
    [
      { key: 'campaign_key', label: 'Campaign' },
      { key: 'process_type', label: 'Process' },
      { key: 'completed_items', label: 'Complete' },
      { key: 'overdue_items', label: 'Overdue' },
      { key: 'escalated_items', label: 'Escalated' },
      { key: 'status', label: 'Status' },
    ],
    orchestration.campaign_monitors || [],
  );

  $('#integrationsTable').innerHTML = renderTable(
    [
      { key: 'name', label: 'Integration' },
      { key: 'category', label: 'Category' },
      { key: 'status', label: 'Status' },
      { key: 'direction', label: 'Direction' },
      { key: 'endpoint_hint', label: 'Endpoint' },
    ],
    state.bootstrap.integrations || [],
    {
      status: (value) => statusPill(value),
    },
  );

  $('#connectorTable').innerHTML = renderTable(
    [
      { key: 'connector_key', label: 'Key' },
      { key: 'name', label: 'Name' },
      { key: 'system_type', label: 'System' },
      { key: 'direction', label: 'Direction' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.connectors?.connectors || [],
    { status: (value) => statusPill(value) },
  );

  $('#importBatchTable').innerHTML = renderTable(
    [
      { key: 'connector_key', label: 'Connector' },
      { key: 'source_format', label: 'Format' },
      { key: 'status', label: 'Status' },
      { key: 'accepted_rows', label: 'Accepted' },
      { key: 'rejected_rows', label: 'Rejected' },
    ],
    state.bootstrap.importBatches?.imports || [],
    { status: (value) => statusPill(value) },
  );

  $('#importRejectionTable').innerHTML = renderTable(
    [
      { key: 'import_batch_id', label: 'Batch' },
      { key: 'row_number', label: 'Row' },
      { key: 'reason', label: 'Reason' },
    ],
    state.bootstrap.importRejections?.rejections || [],
  );

  $('#stagingBatchTable').innerHTML = renderTable(
    [
      { key: 'id', label: 'Preview' },
      { key: 'connector_key', label: 'Connector' },
      { key: 'status', label: 'Status' },
      { key: 'valid_rows', label: 'Valid' },
      { key: 'rejected_rows', label: 'Rejected' },
      { key: 'approved_rows', label: 'Approved' },
    ],
    state.bootstrap.stagingBatches?.staging_batches || [],
    { status: (value) => statusPill(value) },
    'Staged import previews',
  );

  $('#stagingRowTable').innerHTML = renderTable(
    [
      { key: 'row_number', label: 'Row' },
      { key: 'status', label: 'Status' },
      { key: 'validation', label: 'Validation' },
      { key: 'import_batch_id', label: 'Import batch' },
    ],
    state.bootstrap.stagingRows?.rows || [],
    {
      status: (value) => statusPill(value),
      validation: (value) => (value || []).map((item) => item.message).join('; '),
    },
    'Staged row drill-back',
  );

  $('#syncJobTable').innerHTML = renderTable(
    [
      { key: 'connector_key', label: 'Connector' },
      { key: 'job_type', label: 'Job' },
      { key: 'status', label: 'Status' },
      { key: 'records_processed', label: 'Processed' },
      { key: 'records_rejected', label: 'Rejected' },
    ],
    state.bootstrap.syncJobs?.sync_jobs || [],
    { status: (value) => statusPill(value) },
  );

  $('#powerBiExportTable').innerHTML = renderTable(
    [
      { key: 'dataset_name', label: 'Dataset' },
      { key: 'status', label: 'Status' },
      { key: 'row_count', label: 'Rows' },
      { key: 'created_at', label: 'Created' },
    ],
    state.bootstrap.powerBiExports?.exports || [],
    { status: (value) => statusPill(value) },
  );

  $('#mappingTemplateTable').innerHTML = renderTable(
    [
      { key: 'template_key', label: 'Template' },
      { key: 'connector_key', label: 'Connector' },
      { key: 'import_type', label: 'Type' },
      { key: 'active', label: 'Active' },
    ],
    state.bootstrap.mappingTemplates?.templates || [],
  );

  $('#validationRuleTable').innerHTML = renderTable(
    [
      { key: 'rule_key', label: 'Rule' },
      { key: 'import_type', label: 'Type' },
      { key: 'field_name', label: 'Field' },
      { key: 'operator', label: 'Operator' },
      { key: 'severity', label: 'Severity' },
    ],
    state.bootstrap.validationRules?.rules || [],
    { severity: (value) => statusPill(value) },
  );

  $('#credentialTable').innerHTML = renderTable(
    [
      { key: 'connector_key', label: 'Connector' },
      { key: 'credential_key', label: 'Credential' },
      { key: 'masked_value', label: 'Masked' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.credentials?.credentials || [],
    { status: (value) => statusPill(value) },
  );

  $('#retryEventTable').innerHTML = renderTable(
    [
      { key: 'connector_key', label: 'Connector' },
      { key: 'operation_type', label: 'Operation' },
      { key: 'status', label: 'Status' },
      { key: 'attempts', label: 'Attempts' },
      { key: 'next_retry_at', label: 'Next retry' },
    ],
    state.bootstrap.retryEvents?.retry_events || [],
    { status: (value) => statusPill(value) },
  );

  $('#syncLogTable').innerHTML = renderTable(
    [
      { key: 'connector_key', label: 'Connector' },
      { key: 'event_type', label: 'Event' },
      { key: 'status', label: 'Status' },
      { key: 'created_at', label: 'Created' },
    ],
    state.bootstrap.syncLogs?.sync_logs || [],
    { status: (value) => statusPill(value) },
  );

  $('#bankingCashImportTable').innerHTML = renderTable(
    [
      { key: 'bank_account', label: 'Account' },
      { key: 'transaction_date', label: 'Date' },
      { key: 'amount', label: 'Amount' },
      { key: 'description', label: 'Description' },
    ],
    state.bootstrap.bankingCashImports?.cash_imports || [],
    { amount: (value) => currency(value) },
  );

  $('#crmEnrollmentImportTable').innerHTML = renderTable(
    [
      { key: 'pipeline_stage', label: 'Stage' },
      { key: 'term', label: 'Term' },
      { key: 'headcount', label: 'Headcount' },
      { key: 'yield_rate', label: 'Yield' },
    ],
    state.bootstrap.crmEnrollmentImports?.crm_imports || [],
    { yield_rate: (value) => `${Math.round(Number(value) * 100)}%` },
  );

  $('#adapterTable').innerHTML = renderTable(
    [
      { key: 'adapter_key', label: 'Adapter' },
      { key: 'system_type', label: 'System' },
      { key: 'auth_type', label: 'Auth' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.connectorMarketplace?.adapters || [],
    { status: (value) => statusPill(value) },
    'Connector marketplace adapters',
  );
  $('#connectorHealthTable').innerHTML = renderTable(
    [
      { key: 'connector_key', label: 'Connector' },
      { key: 'system_type', label: 'System' },
      { key: 'status', label: 'Status' },
      { key: 'latency_ms', label: 'MS' },
      { key: 'message', label: 'Message' },
    ],
    state.bootstrap.connectorHealth?.connectors || [],
    { status: (value) => statusPill(value) },
    'Connector health dashboard',
  );
  $('#authFlowTable').innerHTML = renderTable(
    [
      { key: 'connector_key', label: 'Connector' },
      { key: 'adapter_key', label: 'Adapter' },
      { key: 'auth_type', label: 'Auth' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.connectorMarketplace?.auth_flows || [],
    { status: (value) => statusPill(value) },
    'Connector auth flows',
  );
  $('#mappingPresetTable').innerHTML = renderTable(
    [
      { key: 'preset_key', label: 'Preset' },
      { key: 'adapter_key', label: 'Adapter' },
      { key: 'import_type', label: 'Type' },
      { key: 'description', label: 'Description' },
    ],
    state.bootstrap.connectorMarketplace?.mapping_presets || [],
    {},
    'Connector mapping presets',
  );
  $('#sourceDrillbackTable').innerHTML = renderTable(
    [
      { key: 'connector_key', label: 'Connector' },
      { key: 'source_record_id', label: 'Source ID' },
      { key: 'target_type', label: 'Target' },
      { key: 'target_id', label: 'Target ID' },
    ],
    state.bootstrap.connectorMarketplace?.source_drillbacks || [],
    {},
    'Connector source drill back',
  );
}

function renderOperatingBudget() {
  const submissions = state.bootstrap.operatingBudget?.submissions || [];
  $('#submissionTable').innerHTML = renderTable(
    [
      { key: 'department_code', label: 'Dept' },
      { key: 'status', label: 'Status' },
      { key: 'line_count', label: 'Lines' },
      { key: 'recurring_total', label: 'Recurring' },
      { key: 'one_time_total', label: 'One-time' },
      { key: 'owner', label: 'Owner' },
    ],
    submissions,
    {
      status: (value) => statusPill(value),
      recurring_total: (value) => currency(value),
      one_time_total: (value) => currency(value),
    },
  );

  $('#assumptionTable').innerHTML = renderTable(
    [
      { key: 'department_code', label: 'Dept' },
      { key: 'assumption_key', label: 'Key' },
      { key: 'value', label: 'Value' },
      { key: 'unit', label: 'Unit' },
    ],
    state.bootstrap.budgetAssumptions?.assumptions || [],
  );

  $('#transferTable').innerHTML = renderTable(
    [
      { key: 'from_department_code', label: 'From' },
      { key: 'to_department_code', label: 'To' },
      { key: 'account_code', label: 'Account' },
      { key: 'amount', label: 'Amount' },
      { key: 'status', label: 'Status' },
      { key: 'reason', label: 'Reason' },
    ],
    state.bootstrap.budgetTransfers?.transfers || [],
    {
      amount: (value) => currency(value),
      status: (value) => statusPill(value),
    },
  );
}

function renderEnrollment() {
  $('#enrollmentTermTable').innerHTML = renderTable(
    [
      { key: 'term_code', label: 'Term' },
      { key: 'term_name', label: 'Name' },
      { key: 'period', label: 'Period' },
      { key: 'census_date', label: 'Census' },
    ],
    state.bootstrap.enrollmentTerms?.terms || [],
  );

  $('#tuitionRateTable').innerHTML = renderTable(
    [
      { key: 'program_code', label: 'Program' },
      { key: 'residency', label: 'Residency' },
      { key: 'rate_per_credit', label: 'Rate' },
      { key: 'default_credit_load', label: 'Credits' },
      { key: 'effective_term', label: 'Term' },
    ],
    state.bootstrap.tuitionRates?.rates || [],
    { rate_per_credit: (value) => currency(value) },
  );

  $('#enrollmentInputTable').innerHTML = renderTable(
    [
      { key: 'term_code', label: 'Term' },
      { key: 'program_code', label: 'Program' },
      { key: 'residency', label: 'Residency' },
      { key: 'headcount', label: 'HC' },
      { key: 'fte', label: 'FTE' },
      { key: 'discount_rate', label: 'Discount' },
    ],
    state.bootstrap.enrollmentInputs?.inputs || [],
  );

  $('#tuitionRunTable').innerHTML = renderTable(
    [
      { key: 'term_code', label: 'Term' },
      { key: 'status', label: 'Status' },
      { key: 'gross_revenue', label: 'Gross' },
      { key: 'discount_amount', label: 'Discount' },
      { key: 'net_revenue', label: 'Net' },
    ],
    state.bootstrap.tuitionRuns?.runs || [],
    {
      status: (value) => statusPill(value),
      gross_revenue: (value) => currency(value),
      discount_amount: (value) => currency(value),
      net_revenue: (value) => currency(value),
    },
  );
}

function renderCampusPlanning() {
  $('#positionTable').innerHTML = renderTable(
    [
      { key: 'position_code', label: 'Code' },
      { key: 'title', label: 'Title' },
      { key: 'department_code', label: 'Dept' },
      { key: 'total_compensation', label: 'Comp' },
    ],
    state.bootstrap.positions?.positions || [],
    { total_compensation: (value) => currency(value) },
  );
  $('#facultyLoadTable').innerHTML = renderTable(
    [
      { key: 'term_code', label: 'Term' },
      { key: 'course_code', label: 'Course' },
      { key: 'sections', label: 'Sections' },
      { key: 'faculty_fte', label: 'FTE' },
      { key: 'adjunct_cost', label: 'Adjunct' },
    ],
    state.bootstrap.facultyLoads?.faculty_loads || [],
    { adjunct_cost: (value) => currency(value) },
  );
  $('#grantTable').innerHTML = renderTable(
    [
      { key: 'grant_code', label: 'Grant' },
      { key: 'sponsor', label: 'Sponsor' },
      { key: 'burn_rate', label: 'Burn' },
      { key: 'remaining_award', label: 'Remaining' },
    ],
    state.bootstrap.grants?.grants || [],
    { remaining_award: (value) => currency(value) },
  );
  $('#capitalTable').innerHTML = renderTable(
    [
      { key: 'request_code', label: 'Request' },
      { key: 'project_name', label: 'Project' },
      { key: 'status', label: 'Status' },
      { key: 'annual_depreciation', label: 'Depreciation' },
    ],
    state.bootstrap.capitalRequests?.capital_requests || [],
    {
      status: (value) => statusPill(value),
      annual_depreciation: (value) => currency(value),
    },
  );
}

function renderScenarioEngine() {
  $('#typedDriverTable').innerHTML = renderTable(
    [
      { key: 'driver_key', label: 'Key' },
      { key: 'driver_type', label: 'Type' },
      { key: 'value', label: 'Value' },
      { key: 'unit', label: 'Unit' },
    ],
    state.bootstrap.typedDrivers?.drivers || [],
  );
  $('#scenarioForecastRunTable').innerHTML = renderTable(
    [
      { key: 'method_key', label: 'Method' },
      { key: 'account_code', label: 'Account' },
      { key: 'period_start', label: 'Start' },
      { key: 'period_end', label: 'End' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.scenarioForecastRuns?.runs || [],
    { status: (value) => statusPill(value) },
  );

  $('#forecastVarianceTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'department_code', label: 'Dept' },
      { key: 'account_code', label: 'Account' },
      { key: 'forecast_amount', label: 'Forecast' },
      { key: 'actual_amount', label: 'Actual' },
      { key: 'variance_amount', label: 'Variance' },
    ],
    state.bootstrap.forecastVariances?.variances || [],
    {
      forecast_amount: (value) => currency(value),
      actual_amount: (value) => currency(value),
      variance_amount: (value) => currency(value),
    },
  );

  const graph = state.bootstrap.driverGraph || { edges: [], cycles: [] };
  $('#driverGraphTable').innerHTML = renderTable(
    [
      { key: 'from', label: 'From' },
      { key: 'to', label: 'To' },
      { key: 'cycle', label: 'Cycle' },
    ],
    graph.edges.map((edge) => ({ ...edge, cycle: graph.has_cycles ? 'review' : '' })),
  );

  const predictive = state.bootstrap.predictiveForecasting || {};
  $('#forecastModelChoiceTable').innerHTML = renderTable(
    [
      { key: 'choice_key', label: 'Choice' },
      { key: 'selected_method', label: 'Method' },
      { key: 'seasonality_mode', label: 'Seasonality' },
      { key: 'confidence_level', label: 'Confidence' },
      { key: 'status', label: 'Status' },
    ],
    predictive.model_choices || [],
    { confidence_level: (value) => `${Math.round(Number(value) * 100)}%` },
  );
  $('#forecastBacktestTable').innerHTML = renderTable(
    [
      { key: 'method_key', label: 'Method' },
      { key: 'period_start', label: 'Start' },
      { key: 'period_end', label: 'End' },
      { key: 'accuracy_score', label: 'Accuracy' },
      { key: 'mape', label: 'MAPE' },
      { key: 'rmse', label: 'RMSE' },
    ],
    predictive.backtests || [],
    {
      accuracy_score: (value) => `${Math.round(Number(value) * 100)}%`,
      mape: (value) => `${Math.round(Number(value) * 100)}%`,
      rmse: (value) => currency(value),
    },
  );
  $('#forecastRecommendationTable').innerHTML = renderTable(
    [
      { key: 'account_code', label: 'Account' },
      { key: 'department_code', label: 'Dept' },
      { key: 'recommended_method', label: 'Recommended' },
      { key: 'created_at', label: 'Created' },
    ],
    predictive.recommendations || [],
  );
  $('#forecastDriverExplanationTable').innerHTML = renderTable(
    [
      { key: 'driver_key', label: 'Driver' },
      { key: 'account_code', label: 'Account' },
      { key: 'contribution_score', label: 'Contribution' },
      { key: 'explanation', label: 'Explanation' },
    ],
    predictive.driver_explanations || [],
    { contribution_score: (value) => `${Math.round(Number(value) * 100)}%` },
  );
}

function renderModelBuilder() {
  $('#planningModelTable').innerHTML = renderTable(
    [
      { key: 'model_key', label: 'Key' },
      { key: 'name', label: 'Name' },
      { key: 'status', label: 'Status' },
      { key: 'updated_at', label: 'Updated' },
    ],
    state.bootstrap.planningModels?.models || [],
    { status: (value) => statusPill(value) },
    'Planning models',
  );
  $('#modelFormulaTable').innerHTML = renderTable(
    [
      { key: 'formula_key', label: 'Key' },
      { key: 'expression', label: 'Expression' },
      { key: 'target_account_code', label: 'Target' },
      { key: 'period_start', label: 'Start' },
      { key: 'period_end', label: 'End' },
    ],
    state.bootstrap.modelFormulas?.formulas || [],
    {},
    'Model formulas',
  );
  $('#allocationRuleTable').innerHTML = renderTable(
    [
      { key: 'rule_key', label: 'Key' },
      { key: 'source_account_code', label: 'Source' },
      { key: 'target_account_code', label: 'Target' },
      { key: 'basis_account_code', label: 'Basis' },
      { key: 'target_department_codes', label: 'Departments' },
    ],
    state.bootstrap.allocationRules?.allocation_rules || [],
    { target_department_codes: (value) => Array.isArray(value) ? value.join(', ') : value },
    'Allocation rules',
  );
  $('#modelRunTable').innerHTML = renderTable(
    [
      { key: 'id', label: 'Run' },
      { key: 'status', label: 'Status' },
      { key: 'formula_count', label: 'Formulas' },
      { key: 'allocation_count', label: 'Allocations' },
      { key: 'ledger_entry_count', label: 'Ledger entries' },
    ],
    state.bootstrap.modelRuns?.runs || [],
    { status: (value) => statusPill(value) },
    'Model recalculation runs',
  );
  const graph = state.bootstrap.modelGraph || { edges: [], cycles: [] };
  $('#modelGraphTable').innerHTML = renderTable(
    [
      { key: 'from', label: 'From' },
      { key: 'to', label: 'To' },
      { key: 'cycle', label: 'Cycle' },
    ],
    graph.edges.map((edge) => ({ ...edge, cycle: graph.has_cycles ? 'review' : '' })),
    {},
    'Model formula dependency graph',
  );
  const enterprise = state.bootstrap.enterpriseModel || {};
  $('#modelCubeDimensionTable').innerHTML = renderTable(
    [
      { key: 'dimension_key', label: 'Dimension' },
      { key: 'role', label: 'Role' },
      { key: 'density', label: 'Density' },
      { key: 'member_count', label: 'Members' },
    ],
    enterprise.cube?.dimensions || [],
    { density: (value) => statusPill(value) },
    'Enterprise cube dimensions',
  );
  $('#modelCalculationOrderTable').innerHTML = renderTable(
    [
      { key: 'step', label: 'Step' },
      { key: 'type', label: 'Type' },
      { key: 'key', label: 'Key' },
      { key: 'label', label: 'Label' },
    ],
    enterprise.calculation_order?.steps || [],
    {},
    'Enterprise model calculation order',
  );
  $('#modelVersionTable').innerHTML = renderTable(
    [
      { key: 'version_key', label: 'Version' },
      { key: 'status', label: 'Status' },
      { key: 'published_by', label: 'By' },
      { key: 'published_at', label: 'Published' },
    ],
    enterprise.versions || [],
    { status: (value) => statusPill(value) },
    'Published model versions',
  );
  $('#modelInvalidationTable').innerHTML = renderTable(
    [
      { key: 'formula_key', label: 'Formula' },
      { key: 'reason', label: 'Reason' },
      { key: 'status', label: 'Status' },
      { key: 'created_at', label: 'Created' },
    ],
    enterprise.invalidations || [],
    { status: (value) => statusPill(value) },
    'Model dependency invalidations',
  );
  $('#modelPerformanceTable').innerHTML = renderTable(
    [
      { key: 'test_key', label: 'Test' },
      { key: 'cube_cell_count', label: 'Cells' },
      { key: 'elapsed_ms', label: 'MS' },
      { key: 'status', label: 'Status' },
    ],
    enterprise.performance_tests || [],
    { status: (value) => statusPill(value) },
    'Large model performance tests',
  );
}

function renderProfitability() {
  const profitability = state.bootstrap.profitability || {};
  $('#profitabilityPoolTable').innerHTML = renderTable(
    [
      { key: 'pool_key', label: 'Pool' },
      { key: 'source_department_code', label: 'Source dept' },
      { key: 'source_account_code', label: 'Source acct' },
      { key: 'allocation_basis', label: 'Basis' },
      { key: 'target_type', label: 'Target' },
    ],
    profitability.cost_pools || [],
  );
  $('#profitabilityAllocationRunTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'status', label: 'Status' },
      { key: 'total_source_cost', label: 'Source cost' },
      { key: 'total_allocated_cost', label: 'Allocated' },
    ],
    profitability.allocation_runs || [],
    {
      status: (value) => statusPill(value),
      total_source_cost: (value) => currency(value),
      total_allocated_cost: (value) => currency(value),
    },
  );
  $('#profitabilityTraceTable').innerHTML = renderTable(
    [
      { key: 'pool_key', label: 'Pool' },
      { key: 'target_code', label: 'Target' },
      { key: 'basis_value', label: 'Basis' },
      { key: 'allocation_percent', label: 'Percent' },
      { key: 'allocated_amount', label: 'Amount' },
    ],
    profitability.trace_lines || [],
    {
      allocation_percent: (value) => `${(Number(value) * 100).toFixed(2)}%`,
      allocated_amount: (value) => currency(value),
    },
  );
  $('#profitabilityBeforeAfterTable').innerHTML = renderTable(
    [
      { key: 'department_code', label: 'Department' },
      { key: 'before_allocation', label: 'Before' },
      { key: 'allocated_cost', label: 'Allocated' },
      { key: 'after_allocation', label: 'After' },
    ],
    profitability.before_after || [],
    {
      before_allocation: (value) => currency(value),
      allocated_cost: (value) => currency(value),
      after_allocation: (value) => currency(value),
    },
  );
  $('#programMarginTable').innerHTML = renderTable(
    [
      { key: 'program_code', label: 'Program' },
      { key: 'revenue', label: 'Revenue' },
      { key: 'expense', label: 'Expense' },
      { key: 'allocated_cost', label: 'Allocated' },
      { key: 'margin_percent', label: 'Margin' },
    ],
    profitability.program_margins || [],
    {
      revenue: (value) => currency(value),
      expense: (value) => currency(value),
      allocated_cost: (value) => currency(value),
      margin_percent: (value) => `${(Number(value) * 100).toFixed(2)}%`,
    },
  );
  const grantRows = (profitability.grant_profitability || []).map((row) => ({ type: 'Grant', key: row.grant_code, revenue: row.award, expense: -row.spent, allocated_cost: row.allocated_cost, net: row.remaining_after_allocation }));
  const fundRows = (profitability.fund_profitability || []).map((row) => ({ type: 'Fund', key: row.fund_code, revenue: row.revenue, expense: row.expense, allocated_cost: row.allocated_cost, net: row.net_after_allocation }));
  $('#fundGrantProfitabilityTable').innerHTML = renderTable(
    [
      { key: 'type', label: 'Type' },
      { key: 'key', label: 'Key' },
      { key: 'revenue', label: 'Revenue' },
      { key: 'expense', label: 'Expense' },
      { key: 'allocated_cost', label: 'Allocated' },
      { key: 'net', label: 'Net' },
    ],
    [...grantRows, ...fundRows],
    {
      revenue: (value) => currency(value),
      expense: (value) => currency(value),
      allocated_cost: (value) => currency(value),
      net: (value) => currency(value),
    },
  );
}

function renderReporting() {
  const reports = state.bootstrap.reportDefinitions?.reports || [];
  $('#reportDefinitionTable').innerHTML = renderTable(
    [
      { key: 'id', label: 'ID' },
      { key: 'name', label: 'Name' },
      { key: 'report_type', label: 'Type' },
      { key: 'row_dimension', label: 'Rows' },
      { key: 'column_dimension', label: 'Columns' },
    ],
    reports,
  );

  $('#dashboardWidgetTable').innerHTML = renderTable(
    [
      { key: 'name', label: 'Widget' },
      { key: 'widget_type', label: 'Type' },
      { key: 'metric_key', label: 'Metric' },
      { key: 'value', label: 'Value' },
    ],
    state.bootstrap.dashboardWidgets?.widgets || [],
    { value: (value) => currency(value) },
  );

  $('#financialStatementTable').innerHTML = renderTable(
    [
      { key: 'label', label: 'Section' },
      { key: 'amount', label: 'Amount' },
    ],
    state.bootstrap.financialStatement?.sections || [],
    { amount: (value) => `<span class="${value >= 0 ? 'positive' : 'negative'}">${currency(value)}</span>` },
  );

  $('#scheduledExportTable').innerHTML = renderTable(
    [
      { key: 'report_definition_id', label: 'Report' },
      { key: 'export_format', label: 'Format' },
      { key: 'schedule_cron', label: 'Schedule' },
      { key: 'status', label: 'Status' },
      { key: 'destination', label: 'Destination' },
    ],
    state.bootstrap.reportExports?.exports || [],
    { status: (value) => statusPill(value) },
  );

  $('#abfVarianceTable').innerHTML = renderTable(
    [
      { key: 'key', label: 'Dept/account' },
      { key: 'actual', label: 'Actual' },
      { key: 'budget', label: 'Budget' },
      { key: 'forecast', label: 'Forecast' },
      { key: 'actual_vs_budget', label: 'A-B' },
    ],
    state.bootstrap.abfVariance?.rows || [],
    {
      actual: (value) => currency(value),
      budget: (value) => currency(value),
      forecast: (value) => currency(value),
      actual_vs_budget: (value) => currency(value),
    },
  );

  $('#boardPackageTable').innerHTML = renderTable(
    [
      { key: 'package_name', label: 'Package' },
      { key: 'period_start', label: 'Start' },
      { key: 'period_end', label: 'End' },
      { key: 'status', label: 'Status' },
      { key: 'created_at', label: 'Created' },
    ],
    state.bootstrap.boardPackages?.packages || [],
    { status: (value) => statusPill(value) },
  );

  $('#exportArtifactTable').innerHTML = renderTable(
    [
      { key: 'artifact_type', label: 'Type' },
      { key: 'file_name', label: 'File' },
      { key: 'content_type', label: 'Content type' },
      { key: 'status', label: 'Status' },
      { key: 'size_bytes', label: 'Bytes' },
      { key: 'download_url', label: 'Download' },
    ],
    state.bootstrap.exportArtifacts?.artifacts || [],
    {
      status: (value) => statusPill(value),
      download_url: (value, row) => value ? `<button class="download-artifact-button" data-url="${escapeHtml(value)}" data-file="${escapeHtml(row.file_name)}">Download</button>` : '',
    },
  );

  $('#snapshotTable').innerHTML = renderTable(
    [
      { key: 'snapshot_key', label: 'Snapshot' },
      { key: 'snapshot_type', label: 'Type' },
      { key: 'retention_until', label: 'Retain until' },
      { key: 'created_at', label: 'Created' },
    ],
    state.bootstrap.reportSnapshots?.snapshots || [],
  );

  $('#scheduledExtractRunTable').innerHTML = renderTable(
    [
      { key: 'extract_key', label: 'Extract' },
      { key: 'destination', label: 'Destination' },
      { key: 'row_count', label: 'Rows' },
      { key: 'status', label: 'Status' },
      { key: 'artifact_id', label: 'Artifact' },
    ],
    state.bootstrap.scheduledExtractRuns?.runs || [],
    { status: (value) => statusPill(value) },
  );

  $('#productionPdfArtifactTable').innerHTML = renderTable(
    [
      { key: 'file_name', label: 'File' },
      { key: 'artifact_type', label: 'Type' },
      { key: 'page_count', label: 'Pages' },
      { key: 'validation_status', label: 'Validation' },
      { key: 'download_url', label: 'Download' },
    ],
    state.bootstrap.productionPdf?.artifacts || [],
    {
      page_count: (_value, row) => row.metadata?.page_count || '',
      validation_status: (_value, row) => statusPill(row.metadata?.validation_status || row.status),
      download_url: (value, row) => value ? `<button class="download-artifact-button" data-url="${escapeHtml(value)}" data-file="${escapeHtml(row.file_name)}">Download</button>` : '',
    },
  );

  $('#exportValidationTable').innerHTML = renderTable(
    [
      { key: 'artifact_id', label: 'Artifact' },
      { key: 'status', label: 'Status' },
      { key: 'issues', label: 'Issues' },
      { key: 'created_at', label: 'Created' },
    ],
    state.bootstrap.productionPdf?.validations || [],
    {
      status: (value) => statusPill(value),
      issues: (value) => Array.isArray(value) ? value.join(', ') : '',
    },
  );

  const manifest = state.bootstrap.biApiManifest;
  $('#biApiManifestTable').innerHTML = renderTable(
    [
      { key: 'schema_version', label: 'Schema' },
      { key: 'row_count', label: 'Rows' },
      { key: 'requires_auth', label: 'Auth' },
      { key: 'retention_supported', label: 'Retention' },
    ],
    manifest ? [{ ...manifest, requires_auth: manifest.controls?.requires_auth, retention_supported: manifest.controls?.retention_supported }] : [],
  );

  $('#varianceThresholdTable').innerHTML = renderTable(
    [
      { key: 'threshold_key', label: 'Threshold' },
      { key: 'amount_threshold', label: 'Amount' },
      { key: 'percent_threshold', label: 'Percent' },
      { key: 'require_explanation', label: 'Required' },
    ],
    state.bootstrap.varianceThresholds?.thresholds || [],
    {
      amount_threshold: (value) => currency(value),
      percent_threshold: (value) => value === null || value === undefined ? '' : `${Math.round(Number(value) * 100)}%`,
    },
  );

  $('#varianceExplanationTable').innerHTML = renderTable(
    [
      { key: 'variance_key', label: 'Variance' },
      { key: 'variance_amount', label: 'Amount' },
      { key: 'status', label: 'Status' },
      { key: 'explanation_text', label: 'Commentary' },
      { key: 'ai_draft_text', label: 'AI draft' },
    ],
    state.bootstrap.varianceExplanations?.explanations || [],
    {
      variance_amount: (value) => currency(value),
      status: (value) => statusPill(value),
    },
  );

  $('#narrativeReportTable').innerHTML = renderTable(
    [
      { key: 'title', label: 'Narrative' },
      { key: 'status', label: 'Status' },
      { key: 'created_by', label: 'Created by' },
      { key: 'created_at', label: 'Created' },
      { key: 'approved_by', label: 'Approved by' },
    ],
    state.bootstrap.narrativeReports?.narratives || [],
    { status: (value) => statusPill(value) },
  );

  const production = state.bootstrap.productionReporting || {};
  $('#pixelStatementTable').innerHTML = renderTable(
    [
      { key: 'label', label: 'Line' },
      { key: 'amount', label: 'Amount' },
      { key: 'page_number', label: 'Page' },
      { key: 'x', label: 'X' },
      { key: 'y', label: 'Y' },
      { key: 'width', label: 'Width' },
    ],
    production.pixel_financial_statement?.rows || [],
    { amount: (value) => currency(value) },
    'Pixel controlled financial statement',
  );
  $('#paginationProfileTable').innerHTML = renderTable(
    [
      { key: 'name', label: 'Profile' },
      { key: 'page_size', label: 'Size' },
      { key: 'orientation', label: 'Orientation' },
      { key: 'rows_per_page', label: 'Rows/page' },
    ],
    production.pagination_profiles || [],
    {},
    'PDF pagination profiles',
  );
  $('#reportFootnoteTable').innerHTML = renderTable(
    [
      { key: 'marker', label: 'Marker' },
      { key: 'target_type', label: 'Target' },
      { key: 'footnote_text', label: 'Text' },
      { key: 'display_order', label: 'Order' },
    ],
    production.footnotes || [],
    {},
    'Report footnotes',
  );
  $('#pageBreakTable').innerHTML = renderTable(
    [
      { key: 'report_book_id', label: 'Book' },
      { key: 'section_key', label: 'Section' },
      { key: 'page_number', label: 'Page' },
      { key: 'break_before', label: 'Break' },
    ],
    production.page_breaks || [],
    { break_before: (value) => value ? 'before' : '' },
    'Report page breaks',
  );
  $('#chartFormatTable').innerHTML = renderTable(
    [
      { key: 'name', label: 'Chart' },
      { key: 'chart_type', label: 'Type' },
      { key: 'format', label: 'Format' },
    ],
    (production.charts || []).map((chart) => ({ ...chart, format: chart.config?.format ? Object.keys(chart.config.format).join(', ') : '' })),
    {},
    'Chart formatting',
  );
  $('#boardReleaseTable').innerHTML = renderTable(
    [
      { key: 'package_key', label: 'Package' },
      { key: 'status', label: 'Status' },
      { key: 'approved_by', label: 'Approved by' },
      { key: 'released_by', label: 'Released by' },
    ],
    production.release_reviews || [],
    { status: (value) => statusPill(value) },
    'Recurring board package release reviews',
  );
  const chartRendering = state.bootstrap.chartRendering || {};
  $('#chartRenderTable').innerHTML = renderTable(
    [
      { key: 'file_name', label: 'File' },
      { key: 'render_format', label: 'Format' },
      { key: 'renderer', label: 'Renderer' },
      { key: 'size_bytes', label: 'Bytes' },
      { key: 'created_at', label: 'Created' },
    ],
    chartRendering.renders || [],
    {},
    'PNG/SVG chart render artifacts',
  );
  $('#dashboardChartSnapshotTable').innerHTML = renderTable(
    [
      { key: 'snapshot_key', label: 'Snapshot' },
      { key: 'status', label: 'Status' },
      { key: 'chart_id', label: 'Chart' },
      { key: 'render_id', label: 'Render' },
      { key: 'created_at', label: 'Created' },
    ],
    chartRendering.snapshots || [],
    { status: (value) => statusPill(value) },
    'Retained dashboard chart snapshots',
  );

  const exportSelect = $('#exportReportSelect');
  if (exportSelect) {
    exportSelect.innerHTML = reports
      .map((report) => `<option value="${report.id}">${report.name}</option>`)
      .join('');
  }
  const packageSelect = $('#artifactPackageSelect');
  if (packageSelect) {
    const packages = state.bootstrap.boardPackages?.packages || [];
    packageSelect.innerHTML = '<option value="">None</option>' + packages
      .map((item) => `<option value="${item.id}">${item.package_name}</option>`)
      .join('');
  }
  const extractSelect = $('#extractExportSelect');
  if (extractSelect) {
    const exports = state.bootstrap.reportExports?.exports || [];
    extractSelect.innerHTML = '<option value="">None</option>' + exports
      .map((item) => `<option value="${item.id}">${item.export_format} to ${item.destination}</option>`)
      .join('');
  }
  const explanationSelect = $('#varianceExplanationSelect');
  if (explanationSelect) {
    const explanations = state.bootstrap.varianceExplanations?.explanations || [];
    explanationSelect.innerHTML = explanations
      .map((item) => `<option value="${item.variance_key}">${item.variance_key}</option>`)
      .join('');
  }
  const narrativePackageSelect = $('#narrativePackageSelect');
  if (narrativePackageSelect) {
    const packages = state.bootstrap.boardPackages?.packages || [];
    narrativePackageSelect.innerHTML = '<option value="">None</option>' + packages
      .map((item) => `<option value="${item.id}">${item.package_name}</option>`)
      .join('');
  }
}

function renderClose() {
  $('#closeTemplateTable').innerHTML = renderTable(
    [
      { key: 'template_key', label: 'Template' },
      { key: 'title', label: 'Title' },
      { key: 'owner_role', label: 'Owner role' },
      { key: 'due_day_offset', label: 'Offset' },
      { key: 'active', label: 'Active' },
    ],
    state.bootstrap.closeTemplates?.templates || [],
  );

  $('#closeCalendarTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'close_start', label: 'Start' },
      { key: 'close_due', label: 'Due' },
      { key: 'lock_state', label: 'Lock' },
      { key: 'locked_by', label: 'Locked by' },
    ],
    state.bootstrap.closeCalendar?.calendar || [],
    { lock_state: (value) => statusPill(value) },
  );

  $('#closeChecklistTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'checklist_key', label: 'Key' },
      { key: 'title', label: 'Title' },
      { key: 'owner', label: 'Owner' },
      { key: 'dependency_status', label: 'Deps' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.closeChecklists?.items || [],
    {
      status: (value) => statusPill(value),
      dependency_status: (value) => statusPill(value),
    },
  );

  $('#closeDependencyTable').innerHTML = renderTable(
    [
      { key: 'task_key', label: 'Task' },
      { key: 'depends_on_key', label: 'Depends on' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.closeDependencies?.dependencies || [],
    { status: (value) => statusPill(value) },
  );

  $('#reconciliationTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'account_code', label: 'Account' },
      { key: 'book_balance', label: 'Book' },
      { key: 'source_balance', label: 'Source' },
      { key: 'variance', label: 'Variance' },
      { key: 'preparer', label: 'Preparer' },
      { key: 'reviewer', label: 'Reviewer' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.reconciliations?.reconciliations || [],
    {
      book_balance: (value) => currency(value),
      source_balance: (value) => currency(value),
      variance: (value) => currency(value),
      status: (value) => statusPill(value),
    },
  );

  $('#reconciliationExceptionTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'account_code', label: 'Account' },
      { key: 'severity', label: 'Severity' },
      { key: 'aging_days', label: 'Aging' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.reconciliationExceptions?.exceptions || [],
    {
      severity: (value) => statusPill(value),
      status: (value) => statusPill(value),
    },
  );

  $('#entityConfirmationTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'entity_code', label: 'Entity' },
      { key: 'confirmation_type', label: 'Type' },
      { key: 'status', label: 'Status' },
      { key: 'confirmed_by', label: 'Confirmed by' },
    ],
    state.bootstrap.entityConfirmations?.confirmations || [],
    { status: (value) => statusPill(value) },
  );

  $('#consolidationEntityTable').innerHTML = renderTable(
    [
      { key: 'entity_code', label: 'Entity' },
      { key: 'entity_name', label: 'Name' },
      { key: 'parent_entity_code', label: 'Parent' },
      { key: 'base_currency', label: 'Currency' },
      { key: 'gaap_basis', label: 'GAAP' },
    ],
    state.bootstrap.consolidationEntities?.entities || [],
  );

  $('#entityOwnershipTable').innerHTML = renderTable(
    [
      { key: 'parent_entity_code', label: 'Parent' },
      { key: 'child_entity_code', label: 'Child' },
      { key: 'ownership_percent', label: 'Ownership' },
      { key: 'effective_period', label: 'Period' },
    ],
    state.bootstrap.entityOwnerships?.ownerships || [],
    { ownership_percent: (value) => `${Number(value).toFixed(2)}%` },
  );

  $('#consolidationSettingTable').innerHTML = renderTable(
    [
      { key: 'gaap_basis', label: 'GAAP' },
      { key: 'reporting_currency', label: 'Currency' },
      { key: 'translation_method', label: 'Translation' },
      { key: 'enabled', label: 'Enabled' },
    ],
    state.bootstrap.consolidationSettings?.settings || [],
  );

  $('#intercompanyTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'source_entity_code', label: 'Source' },
      { key: 'target_entity_code', label: 'Target' },
      { key: 'account_code', label: 'Account' },
      { key: 'variance', label: 'Variance' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.intercompanyMatches?.matches || [],
    {
      variance: (value) => currency(value),
      status: (value) => statusPill(value),
    },
  );

  $('#eliminationTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'entity_code', label: 'Entity' },
      { key: 'account_code', label: 'Account' },
      { key: 'amount', label: 'Amount' },
      { key: 'review_status', label: 'Review' },
      { key: 'reason', label: 'Reason' },
    ],
    state.bootstrap.eliminations?.eliminations || [],
    {
      amount: (value) => currency(value),
      review_status: (value) => statusPill(value),
    },
  );

  $('#consolidationRunTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'status', label: 'Status' },
      { key: 'total_before_eliminations', label: 'Before' },
      { key: 'total_eliminations', label: 'Elims' },
      { key: 'consolidated_total', label: 'Consolidated' },
    ],
    state.bootstrap.consolidationRuns?.runs || [],
    {
      status: (value) => statusPill(value),
      total_before_eliminations: (value) => currency(value),
      total_eliminations: (value) => currency(value),
      consolidated_total: (value) => currency(value),
    },
  );

  $('#auditPacketTable').innerHTML = renderTable(
    [
      { key: 'packet_key', label: 'Packet' },
      { key: 'status', label: 'Status' },
      { key: 'created_by', label: 'Created by' },
      { key: 'created_at', label: 'Created' },
    ],
    state.bootstrap.auditPackets?.audit_packets || [],
    { status: (value) => statusPill(value) },
  );

  $('#consolidationAuditReportTable').innerHTML = renderTable(
    [
      { key: 'report_key', label: 'Report' },
      { key: 'report_type', label: 'Type' },
      { key: 'created_by', label: 'Created by' },
      { key: 'created_at', label: 'Created' },
    ],
    state.bootstrap.consolidationAuditReports?.audit_reports || [],
  );

  $('#consolidationRuleTable').innerHTML = renderTable(
    [
      { key: 'rule_key', label: 'Rule' },
      { key: 'rule_type', label: 'Type' },
      { key: 'priority', label: 'Priority' },
      { key: 'active', label: 'Active' },
    ],
    state.bootstrap.consolidationRules?.rules || [],
  );

  $('#ownershipChainTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'parent_entity_code', label: 'Parent' },
      { key: 'child_entity_code', label: 'Child' },
      { key: 'effective_ownership_percent', label: 'Effective' },
      { key: 'minority_interest_percent', label: 'Minority' },
    ],
    state.bootstrap.ownershipChains?.ownership_chains || [],
    {
      effective_ownership_percent: (value) => `${Number(value).toFixed(2)}%`,
      minority_interest_percent: (value) => `${Number(value).toFixed(2)}%`,
    },
  );

  $('#currencyTranslationAdjustmentTable').innerHTML = renderTable(
    [
      { key: 'entity_code', label: 'Entity' },
      { key: 'account_code', label: 'Account' },
      { key: 'average_rate', label: 'Avg' },
      { key: 'closing_rate', label: 'Close' },
      { key: 'cta_amount', label: 'CTA' },
    ],
    state.bootstrap.currencyTranslationAdjustments?.currency_translation_adjustments || [],
    { cta_amount: (value) => currency(value) },
  );

  $('#statutoryPackTable').innerHTML = renderTable(
    [
      { key: 'pack_key', label: 'Pack' },
      { key: 'book_basis', label: 'Book' },
      { key: 'reporting_currency', label: 'Currency' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.statutoryPacks?.statutory_packs || [],
  );

  $('#supplementalScheduleTable').innerHTML = renderTable(
    [
      { key: 'schedule_key', label: 'Schedule' },
      { key: 'schedule_type', label: 'Type' },
      { key: 'created_at', label: 'Created' },
    ],
    state.bootstrap.supplementalSchedules?.supplemental_schedules || [],
  );
}

function renderAutomation() {
  $('#automationRecommendationTable').innerHTML = renderTable(
    [
      { key: 'assistant_type', label: 'Assistant' },
      { key: 'severity', label: 'Severity' },
      { key: 'subject_key', label: 'Subject' },
      { key: 'status', label: 'Status' },
      { key: 'recommendation', label: 'Recommendation' },
    ],
    state.bootstrap.automationRecommendations?.recommendations || [],
    {
      status: (value) => statusPill(value),
      severity: (value) => statusPill(value),
    },
  );

  $('#automationGateTable').innerHTML = renderTable(
    [
      { key: 'recommendation_id', label: 'Rec' },
      { key: 'gate_key', label: 'Gate' },
      { key: 'required_permission', label: 'Permission' },
      { key: 'status', label: 'Status' },
      { key: 'decided_by', label: 'Decided by' },
    ],
    state.bootstrap.automationGates?.approval_gates || [],
    { status: (value) => statusPill(value) },
  );
  $('#agentPromptTable').innerHTML = renderTable(
    [
      { key: 'agent_type', label: 'Agent' },
      { key: 'prompt_text', label: 'Prompt' },
      { key: 'status', label: 'Status' },
      { key: 'created_at', label: 'Created' },
    ],
    state.bootstrap.agentPrompts?.prompts || [],
    { status: (value) => statusPill(value) },
    'AI planning agent prompt audit',
  );
  $('#agentActionTable').innerHTML = renderTable(
    [
      { key: 'agent_type', label: 'Agent' },
      { key: 'action_type', label: 'Action' },
      { key: 'guard_status', label: 'Guard' },
      { key: 'status', label: 'Status' },
      { key: 'summary', label: 'Summary' },
    ],
    (state.bootstrap.agentActions?.actions || []).map((action) => ({ ...action, summary: action.proposal?.summary || '' })),
    {
      guard_status: (value) => statusPill(value),
      status: (value) => statusPill(value),
    },
    'Guarded AI planning agent actions',
  );
  const universityAgent = state.bootstrap.universityAgent || {};
  $('#universityAgentToolTable').innerHTML = renderTable(
    [
      { key: 'tool_key', label: 'Tool' },
      { key: 'required_scope', label: 'Scope' },
      { key: 'action_type', label: 'Action' },
      { key: 'approval_required', label: 'Approval' },
      { key: 'enabled', label: 'Enabled' },
    ],
    universityAgent.tools || [],
    {
      approval_required: (value) => value ? 'required' : 'not required',
      enabled: (value) => value ? 'yes' : 'no',
    },
    'External University Agent tool registry',
  );
  $('#universityAgentRequestTable').innerHTML = renderTable(
    [
      { key: 'request_key', label: 'Request' },
      { key: 'client_key', label: 'Client' },
      { key: 'tool_key', label: 'Tool' },
      { key: 'signature_status', label: 'Signature' },
      { key: 'policy_status', label: 'Policy' },
      { key: 'approval_status', label: 'Approval' },
      { key: 'status', label: 'Status' },
    ],
    universityAgent.requests || [],
    {
      signature_status: (value) => statusPill(value),
      policy_status: (value) => statusPill(value),
      approval_status: (value) => statusPill(value),
      status: (value) => statusPill(value),
    },
    'Signed University Agent requests',
  );
  $('#universityAgentPolicyTable').innerHTML = renderTable(
    [
      { key: 'policy_key', label: 'Policy' },
      { key: 'client_key', label: 'Client' },
      { key: 'tool_key', label: 'Tool' },
      { key: 'allowed_actions', label: 'Allowed' },
      { key: 'max_amount', label: 'Max amount' },
      { key: 'status', label: 'Status' },
    ],
    universityAgent.policies || [],
    {
      allowed_actions: (value) => Array.isArray(value) ? value.join(', ') : value,
      max_amount: (value) => value === null || value === undefined ? '' : currency(value),
      status: (value) => statusPill(value),
    },
    'Scoped allowed-action policies',
  );
  $('#universityAgentAuditTable').innerHTML = renderTable(
    [
      { key: 'client_key', label: 'Client' },
      { key: 'event_type', label: 'Event' },
      { key: 'created_at', label: 'Created' },
    ],
    universityAgent.audit_logs || [],
    {},
    'Agent audit logs and callback queue',
  );
}

function renderAIExplainability() {
  const explanations = state.bootstrap.aiExplanations?.explanations || [];
  $('#aiExplanationTable').innerHTML = renderTable(
    [
      { key: 'subject_key', label: 'Subject' },
      { key: 'confidence', label: 'Confidence' },
      { key: 'status', label: 'Status' },
      { key: 'citation_count', label: 'Citations' },
      { key: 'explanation_text', label: 'Explanation' },
    ],
    explanations.map((item) => ({ ...item, citation_count: item.citations?.length || 0 })),
    {
      confidence: (value) => `${Math.round(Number(value || 0) * 100)}%`,
      status: (value) => statusPill(value),
    },
    'AI cited explanations',
  );
  const traces = explanations[0]?.source_traces || [];
  $('#aiTraceTable').innerHTML = renderTable(
    [
      { key: 'trace_order', label: 'Step' },
      { key: 'source_type', label: 'Source' },
      { key: 'source_id', label: 'ID' },
      { key: 'transformation', label: 'Transformation' },
    ],
    traces,
    {},
    'AI source traces',
  );
}

function renderMarketLab() {
  const payload = state.marketLab;
  if (!payload) return;
  $('#marketTicker').innerHTML = (payload.ticker || [])
    .map((item) => {
      const direction = Number(item.change_amount || 0) >= 0 ? 'positive' : 'negative';
      return `<span class="ticker-chip"><strong>${item.symbol} ${currency(item.price)}</strong><span class="${direction}">${currency(item.change_amount)} / ${(Number(item.change_percent || 0) * 100).toFixed(2)}%</span></span>`;
    })
    .join('');
  $('#marketSearchTable').innerHTML = renderTable(
    [
      { key: 'symbol', label: 'Symbol' },
      { key: 'name', label: 'Name' },
      { key: 'price', label: 'Price' },
      { key: 'change_percent', label: 'Change' },
      { key: 'provider', label: 'Provider' },
    ],
    state.marketSearch?.results || [],
    {
      price: (value) => currency(value),
      change_percent: (value) => `${(Number(value || 0) * 100).toFixed(2)}%`,
    },
    'Market symbol search results',
  );
  $('#marketFavoriteTable').innerHTML = renderTable(
    [
      { key: 'symbol', label: 'Symbol' },
      { key: 'price', label: 'Price' },
      { key: 'change_amount', label: 'Change' },
      { key: 'as_of', label: 'As of' },
    ],
    payload.favorites || [],
    {
      price: (value) => currency(value),
      change_amount: (value) => currency(value),
    },
    'Market watchlist favorites',
  );
  $('#paperAccountTable').innerHTML = renderTable(
    [
      { key: 'cash_balance', label: 'Cash' },
      { key: 'holdings_value', label: 'Holdings' },
      { key: 'total_equity', label: 'Equity' },
      { key: 'total_pnl', label: 'P&L' },
    ],
    payload.account ? [payload.account] : [],
    {
      cash_balance: (value) => currency(value),
      holdings_value: (value) => currency(value),
      total_equity: (value) => currency(value),
      total_pnl: (value) => currency(value),
    },
    'Paper trading account',
  );
  $('#paperPositionTable').innerHTML = renderTable(
    [
      { key: 'symbol', label: 'Symbol' },
      { key: 'quantity', label: 'Qty' },
      { key: 'average_cost', label: 'Avg cost' },
      { key: 'market_value', label: 'Value' },
      { key: 'unrealized_pnl', label: 'P&L' },
    ],
    payload.positions || [],
    {
      average_cost: (value) => currency(value),
      market_value: (value) => currency(value),
      unrealized_pnl: (value) => currency(value),
    },
    'Paper trading positions',
  );
  $('#paperTradeTable').innerHTML = renderTable(
    [
      { key: 'symbol', label: 'Symbol' },
      { key: 'side', label: 'Side' },
      { key: 'quantity', label: 'Qty' },
      { key: 'price', label: 'Price' },
      { key: 'notional', label: 'Notional' },
      { key: 'created_at', label: 'Time' },
    ],
    payload.trades || [],
    {
      price: (value) => currency(value),
      notional: (value) => currency(value),
    },
    'Paper trade history',
  );
  renderBrokerageConnectors();
}

function renderBrokerageConnectors() {
  const payload = state.brokerage || {};
  $('#brokerageConnectionTable').innerHTML = renderTable(
    [
      { key: 'connection_name', label: 'Name' },
      { key: 'provider_key', label: 'Provider' },
      { key: 'mode', label: 'Mode' },
      { key: 'provider_environment', label: 'Env' },
      { key: 'consent_status', label: 'Consent' },
      { key: 'auth_flow_status', label: 'Auth' },
      { key: 'status', label: 'Status' },
      { key: 'trading_enabled', label: 'Trading' },
    ],
    payload.connections || [],
    {
      status: (value) => `<span class="status-pill ${value || 'review'}">${value || 'none'}</span>`,
      consent_status: (value) => `<span class="status-pill ${value || 'review'}">${value || 'none'}</span>`,
      auth_flow_status: (value) => `<span class="status-pill ${value || 'review'}">${value || 'none'}</span>`,
      trading_enabled: () => 'disabled',
    },
    'Brokerage connector connections',
  );
  $('#brokerageAccountTable').innerHTML = renderTable(
    [
      { key: 'account_name', label: 'Account' },
      { key: 'account_type', label: 'Type' },
      { key: 'cash_balance', label: 'Cash' },
      { key: 'buying_power', label: 'Buying power' },
    ],
    payload.accounts || [],
    {
      cash_balance: (value) => currency(value || 0),
      buying_power: (value) => currency(value || 0),
    },
    'Synced brokerage accounts',
  );
  $('#brokerageHoldingTable').innerHTML = renderTable(
    [
      { key: 'symbol', label: 'Symbol' },
      { key: 'quantity', label: 'Qty' },
      { key: 'average_cost', label: 'Avg cost' },
      { key: 'market_value', label: 'Value' },
      { key: 'unrealized_pnl', label: 'P&L' },
    ],
    payload.holdings || [],
    {
      average_cost: (value) => currency(value || 0),
      market_value: (value) => currency(value || 0),
      unrealized_pnl: (value) => currency(value || 0),
    },
    'Synced brokerage holdings',
  );
  $('#brokerageSyncTable').innerHTML = renderTable(
    [
      { key: 'run_type', label: 'Run' },
      { key: 'status', label: 'Status' },
      { key: 'message', label: 'Message' },
      { key: 'created_at', label: 'Created' },
    ],
    payload.sync_runs || [],
    { status: (value) => `<span class="status-pill ${value || 'review'}">${value || 'none'}</span>` },
    'Brokerage connector sync log',
  );
  $('#brokerageConsentTable').innerHTML = renderTable(
    [
      { key: 'connection_name', label: 'Connection' },
      { key: 'consent_version', label: 'Version' },
      { key: 'status', label: 'Status' },
      { key: 'created_at', label: 'Created' },
    ],
    payload.consents || [],
    { status: (value) => `<span class="status-pill ${value || 'review'}">${value || 'none'}</span>` },
    'Read-only brokerage user consent records',
  );
  $('#brokerageAuditTable').innerHTML = renderTable(
    [
      { key: 'entity_type', label: 'Entity' },
      { key: 'action', label: 'Action' },
      { key: 'created_at', label: 'Created' },
    ],
    payload.audit_trail || [],
    {},
    'Brokerage audit trail',
  );
  const providerSelect = $('#brokerageProviderSelect');
  if (providerSelect) {
    const providers = payload.providers || [];
    providerSelect.innerHTML = providers
      .map((provider) => `<option value="${provider.provider_key}">${provider.name}</option>`)
      .join('');
  }
}

function renderDataHub() {
  const payload = state.bootstrap.dataHub || {};
  $('#masterDataChangeTable').innerHTML = renderTable(
    [
      { key: 'dimension_kind', label: 'Dimension' },
      { key: 'code', label: 'Code' },
      { key: 'change_type', label: 'Change' },
      { key: 'effective_from', label: 'Effective' },
      { key: 'status', label: 'Status' },
    ],
    payload.change_requests || [],
    { status: (value) => statusPill(value) },
    'Master data change requests',
  );
  $('#masterDataMappingTable').innerHTML = renderTable(
    [
      { key: 'source_system', label: 'Source' },
      { key: 'source_code', label: 'Source code' },
      { key: 'target_dimension', label: 'Target' },
      { key: 'target_code', label: 'Target code' },
      { key: 'active', label: 'Active' },
    ],
    payload.mappings || [],
    { active: (value) => (value ? 'yes' : 'no') },
    'Master data mapping crosswalks',
  );
  $('#metadataApprovalTable').innerHTML = renderTable(
    [
      { key: 'entity_type', label: 'Entity' },
      { key: 'entity_id', label: 'ID' },
      { key: 'status', label: 'Status' },
      { key: 'requested_by', label: 'Requested by' },
    ],
    payload.metadata_approvals || [],
    { status: (value) => statusPill(value) },
    'Metadata approvals',
  );
  $('#dataLineageTable').innerHTML = renderTable(
    [
      { key: 'source_id', label: 'Source' },
      { key: 'transform_type', label: 'Transform' },
      { key: 'target_id', label: 'Target' },
      { key: 'record_count', label: 'Rows' },
      { key: 'amount_total', label: 'Amount' },
    ],
    payload.lineage || [],
    { amount_total: (value) => currency(value || 0) },
    'Source to report lineage',
  );
}

function renderOperations() {
  $('#opsCheckTable').innerHTML = renderTable(
    [
      { key: 'check_key', label: 'Check' },
      { key: 'category', label: 'Category' },
      { key: 'status', label: 'Status' },
      { key: 'checked_at', label: 'Checked' },
    ],
    state.bootstrap.opsChecks?.checks || [],
    { status: (value) => statusPill(value) },
  );

  $('#restoreTestTable').innerHTML = renderTable(
    [
      { key: 'backup_key', label: 'Backup' },
      { key: 'status', label: 'Status' },
      { key: 'source_size_bytes', label: 'Bytes' },
      { key: 'tested_at', label: 'Tested' },
    ],
    state.bootstrap.restoreTests?.restore_tests || [],
    { status: (value) => statusPill(value) },
  );

  $('#opsBackupTable').innerHTML = renderTable(
    [
      { key: 'backup_key', label: 'Backup' },
      { key: 'size_bytes', label: 'Bytes' },
      { key: 'created_by', label: 'Created by' },
      { key: 'created_at', label: 'Created' },
    ],
    state.bootstrap.foundationBackups?.backups || [],
  );

  $('#runbookTable').innerHTML = renderTable(
    [
      { key: 'runbook_key', label: 'Key' },
      { key: 'title', label: 'Title' },
      { key: 'category', label: 'Category' },
      { key: 'status', label: 'Status' },
    ],
    state.bootstrap.runbooks?.runbooks || [],
    { status: (value) => statusPill(value) },
  );

  const observability = state.bootstrap.observability || {};
  $('#healthProbeTable').innerHTML = renderTable(
    [
      { key: 'probe_key', label: 'Probe' },
      { key: 'status', label: 'Status' },
      { key: 'latency_ms', label: 'Ms' },
      { key: 'trace_id', label: 'Trace' },
    ],
    observability.health_probes || [],
    { status: (value) => statusPill(value) },
    'Operational health probe history',
  );
  $('#alertEventTable').innerHTML = renderTable(
    [
      { key: 'severity', label: 'Severity' },
      { key: 'message', label: 'Message' },
      { key: 'source', label: 'Source' },
      { key: 'status', label: 'Status' },
    ],
    observability.alerts || [],
    { status: (value) => statusPill(value) },
    'Alert-ready failure events',
  );
  $('#observabilityMetricTable').innerHTML = renderTable(
    [
      { key: 'metric_key', label: 'Metric' },
      { key: 'metric_type', label: 'Type' },
      { key: 'value', label: 'Value' },
      { key: 'unit', label: 'Unit' },
    ],
    observability.metrics || [],
    {},
    'Operational metrics',
  );
  $('#backupDrillTable').innerHTML = renderTable(
    [
      { key: 'drill_key', label: 'Drill' },
      { key: 'backup_key', label: 'Backup' },
      { key: 'backup_size_bytes', label: 'Bytes' },
      { key: 'status', label: 'Status' },
    ],
    observability.backup_restore_drills || [],
    { status: (value) => statusPill(value) },
    'Backup restore drill records',
  );

  const restoreSelect = $('#restoreBackupSelect');
  if (restoreSelect) {
    restoreSelect.innerHTML = (state.bootstrap.foundationBackups?.backups || [])
      .map((backup) => `<option value="${backup.backup_key}">${backup.backup_key}</option>`)
      .join('');
  }
}

function renderPerformanceReliability() {
  const payload = state.bootstrap.performanceReliability || {};
  $('#benchmarkHarnessTable').innerHTML = renderTable(
    [
      { key: 'run_key', label: 'Run' },
      { key: 'dataset_key', label: 'Dataset' },
      { key: 'row_count', label: 'Rows' },
      { key: 'backend', label: 'Backend' },
      { key: 'status', label: 'Status' },
      { key: 'completed_at', label: 'Completed' },
    ],
    payload.benchmark_runs || [],
    { status: (value) => statusPill(value) },
    'Performance benchmark harness runs',
  );
  const parallelCubed = state.bootstrap.parallelCubed || {};
  $('#parallelCubedRunTable').innerHTML = renderTable(
    [
      { key: 'run_key', label: 'Run' },
      { key: 'work_type', label: 'Work' },
      { key: 'worker_count', label: 'Workers' },
      { key: 'logical_cores', label: 'Cores' },
      { key: 'partition_count', label: 'Partitions' },
      { key: 'throughput_per_second', label: 'Rows/sec' },
      { key: 'reduce_status', label: 'Reduce' },
      { key: 'status', label: 'Status' },
    ],
    parallelCubed.runs || [],
    { status: (value) => statusPill(value), reduce_status: (value) => statusPill(value) },
    'Parallel Cubed multi-core benchmark runs',
  );
  $('#parallelCubedPartitionTable').innerHTML = renderTable(
    [
      { key: 'partition_key', label: 'Partition' },
      { key: 'work_type', label: 'Work' },
      { key: 'worker_id', label: 'Worker' },
      { key: 'input_count', label: 'Input' },
      { key: 'output_count', label: 'Output' },
      { key: 'elapsed_ms', label: 'Ms' },
      { key: 'status', label: 'Status' },
    ],
    parallelCubed.partitions || [],
    { status: (value) => statusPill(value) },
    'Partition execution and safe merge history',
  );
  $('#performanceLoadTestTable').innerHTML = renderTable(
    [
      { key: 'test_type', label: 'Type' },
      { key: 'backend', label: 'Backend' },
      { key: 'row_count', label: 'Rows' },
      { key: 'elapsed_ms', label: 'Ms' },
      { key: 'throughput_per_second', label: 'Rows/sec' },
      { key: 'status', label: 'Status' },
    ],
    payload.load_tests || [],
    { status: (value) => statusPill(value) },
    'Performance load and calculation test history',
  );
  $('#indexStrategyTable').innerHTML = renderTable(
    [
      { key: 'table_name', label: 'Table' },
      { key: 'index_name', label: 'Index' },
      { key: 'columns', label: 'Columns' },
      { key: 'status', label: 'Status' },
    ],
    payload.index_recommendations || [],
    {
      columns: (value) => Array.isArray(value) ? value.join(', ') : value,
      status: (value) => statusPill(value),
    },
    'Index strategy recommendations',
  );
  $('#backgroundJobTable').innerHTML = renderTable(
    [
      { key: 'job_key', label: 'Job' },
      { key: 'job_type', label: 'Type' },
      { key: 'priority', label: 'Priority' },
      { key: 'attempts', label: 'Attempts' },
      { key: 'status', label: 'Status' },
    ],
    payload.background_jobs || [],
    { status: (value) => statusPill(value) },
    'Background job queue',
  );
  $('#cacheInvalidationTable').innerHTML = renderTable(
    [
      { key: 'cache_key', label: 'Cache' },
      { key: 'scope', label: 'Scope' },
      { key: 'status', label: 'Status' },
      { key: 'created_at', label: 'Created' },
    ],
    payload.cache_invalidations || [],
    { status: (value) => statusPill(value) },
    'Cache invalidation events',
  );
  $('#restoreAutomationTable').innerHTML = renderTable(
    [
      { key: 'run_key', label: 'Run' },
      { key: 'backup_key', label: 'Backup' },
      { key: 'verify_only', label: 'Verify only' },
      { key: 'status', label: 'Status' },
      { key: 'created_at', label: 'Created' },
    ],
    payload.restore_automations || [],
    {
      verify_only: (value) => value ? 'yes' : 'no',
      status: (value) => statusPill(value),
    },
    'Backup restore automation history',
  );
}

function renderDeploymentGovernance() {
  const payload = state.bootstrap.deploymentGovernance || {};
  $('#environmentSettingsTable').innerHTML = renderTable(
    [
      { key: 'environment_key', label: 'Env' },
      { key: 'tenant_key', label: 'Tenant' },
      { key: 'database_backend', label: 'DB' },
      { key: 'sso_required', label: 'SSO' },
      { key: 'status', label: 'Status' },
    ],
    payload.environments || [],
    {
      sso_required: (value) => value ? 'yes' : 'no',
      status: (value) => statusPill(value),
    },
    'Deployment environment settings',
  );
  $('#environmentPromotionTable').innerHTML = renderTable(
    [
      { key: 'release_version', label: 'Version' },
      { key: 'from_environment', label: 'From' },
      { key: 'to_environment', label: 'To' },
      { key: 'status', label: 'Status' },
    ],
    payload.promotions || [],
    { status: (value) => statusPill(value) },
    'Environment promotions',
  );
  $('#configSnapshotTable').innerHTML = renderTable(
    [
      { key: 'snapshot_key', label: 'Snapshot' },
      { key: 'environment_key', label: 'Env' },
      { key: 'direction', label: 'Direction' },
      { key: 'status', label: 'Status' },
    ],
    payload.config_snapshots || [],
    { status: (value) => statusPill(value) },
    'Deployment config snapshots',
  );
  $('#rollbackPlanTable').innerHTML = renderTable(
    [
      { key: 'migration_key', label: 'Migration' },
      { key: 'rollback_strategy', label: 'Strategy' },
      { key: 'status', label: 'Status' },
    ],
    payload.rollback_plans || [],
    { status: (value) => statusPill(value) },
    'Migration rollback plans',
  );
  $('#releaseNoteTable').innerHTML = renderTable(
    [
      { key: 'release_version', label: 'Version' },
      { key: 'title', label: 'Title' },
      { key: 'status', label: 'Status' },
      { key: 'published_at', label: 'Published' },
    ],
    payload.release_notes || [],
    { status: (value) => statusPill(value) },
    'Release notes',
  );
  $('#adminDiagnosticTable').innerHTML = renderTable(
    [
      { key: 'diagnostic_key', label: 'Diagnostic' },
      { key: 'scope', label: 'Scope' },
      { key: 'status', label: 'Status' },
      { key: 'created_at', label: 'Created' },
    ],
    payload.diagnostics || [],
    { status: (value) => statusPill(value) },
    'Admin diagnostics',
  );
  $('#readinessChecklistTable').innerHTML = renderTable(
    [
      { key: 'category', label: 'Category' },
      { key: 'title', label: 'Item' },
      { key: 'status', label: 'Status' },
      { key: 'updated_at', label: 'Updated' },
    ],
    payload.readiness_items || [],
    { status: (value) => statusPill(value) },
    'Operational readiness checklist',
  );
}

function renderCompliance() {
  const verification = state.bootstrap.auditVerification || {};
  $('#auditIntegrityTable').innerHTML = renderTable(
    [
      { key: 'valid', label: 'Valid' },
      { key: 'verified', label: 'Verified' },
      { key: 'unsealed', label: 'Unsealed' },
      { key: 'failures', label: 'Failures' },
    ],
    [{
      valid: verification.valid ? 'yes' : 'review',
      verified: verification.verified || 0,
      unsealed: verification.unsealed || 0,
      failures: verification.failures?.length || 0,
    }],
    { valid: (value) => `<span class="status-pill ${value === 'yes' ? 'approved' : 'review'}">${value}</span>` },
    'Audit integrity',
  );
  $('#sodViolationTable').innerHTML = renderTable(
    [
      { key: 'rule_key', label: 'Rule' },
      { key: 'severity', label: 'Severity' },
      { key: 'type', label: 'Type' },
      { key: 'actor', label: 'Actor' },
    ],
    (state.bootstrap.sodReport?.violations || []).map((item) => ({ ...item, actor: item.actor || item.user?.email || '' })),
    { severity: (value) => statusPill(value) },
    'Segregation of duties violations',
  );
  $('#retentionPolicyTable').innerHTML = renderTable(
    [
      { key: 'policy_key', label: 'Policy' },
      { key: 'entity_type', label: 'Entity' },
      { key: 'retention_years', label: 'Years' },
      { key: 'disposition_action', label: 'Action' },
      { key: 'legal_hold', label: 'Hold' },
    ],
    state.bootstrap.retentionPolicies?.policies || [],
    { legal_hold: (value) => value ? 'yes' : '' },
    'Retention policies',
  );
  $('#certificationTable').innerHTML = renderTable(
    [
      { key: 'certification_key', label: 'Certification' },
      { key: 'control_area', label: 'Area' },
      { key: 'period', label: 'Period' },
      { key: 'status', label: 'Status' },
      { key: 'owner', label: 'Owner' },
    ],
    state.bootstrap.certifications?.certifications || [],
    { status: (value) => statusPill(value) },
    'Compliance certifications',
  );
  const tax = state.bootstrap.taxCompliance || {};
  $('#taxClassificationTable').innerHTML = renderTable(
    [
      { key: 'activity_name', label: 'Activity' },
      { key: 'tax_status', label: 'Tax' },
      { key: 'activity_tag', label: 'Tag' },
      { key: 'net_ubti', label: 'Net UBTI' },
      { key: 'review_status', label: 'Review' },
    ],
    tax.classifications || [],
    {
      tax_status: (value) => statusPill(value),
      review_status: (value) => statusPill(value),
    },
    'Tax activity classifications',
  );
  $('#form990SupportTable').innerHTML = renderTable(
    [
      { key: 'period', label: 'Period' },
      { key: 'form_part', label: 'Part' },
      { key: 'line_number', label: 'Line' },
      { key: 'column_code', label: 'Column' },
      { key: 'amount', label: 'Amount' },
    ],
    tax.form990_support_fields || [],
    {},
    'Form 990 support fields',
  );
  $('#taxRuleSourceTable').innerHTML = renderTable(
    [
      { key: 'source_key', label: 'Source' },
      { key: 'rule_area', label: 'Area' },
      { key: 'latest_known_version', label: 'Version' },
      { key: 'next_check_at', label: 'Next check' },
    ],
    tax.rule_sources || [],
    {},
    'Tax rule source registry',
  );
  $('#taxAlertTable').innerHTML = renderTable(
    [
      { key: 'severity', label: 'Severity' },
      { key: 'message', label: 'Message' },
      { key: 'status', label: 'Status' },
    ],
    tax.tax_alerts || [],
    { status: (value) => statusPill(value), severity: (value) => statusPill(value) },
    'Tax change alerts',
  );
}

function renderEnterpriseSecurity() {
  const security = state.bootstrap.enterpriseSecurity || {};
  $('#ssoProductionTable').innerHTML = renderTable(
    [
      { key: 'provider_key', label: 'Provider' },
      { key: 'environment', label: 'Env' },
      { key: 'required_claim', label: 'Claim' },
      { key: 'group_claim', label: 'Group' },
      { key: 'status', label: 'Status' },
    ],
    security.sso_production_settings || [],
    { status: (value) => statusPill(value) },
  );
  $('#adOuMappingTable').innerHTML = renderTable(
    [
      { key: 'mapping_key', label: 'Mapping' },
      { key: 'role_key', label: 'Role' },
      { key: 'dimension_kind', label: 'Dim' },
      { key: 'dimension_code', label: 'Code' },
      { key: 'active', label: 'Active' },
    ],
    security.ad_ou_group_mappings || [],
  );
  $('#domainVpnCheckTable').innerHTML = renderTable(
    [
      { key: 'check_key', label: 'Check' },
      { key: 'host', label: 'Host' },
      { key: 'client_host', label: 'Client' },
      { key: 'allowed', label: 'Allowed' },
      { key: 'reason', label: 'Reason' },
    ],
    security.domain_vpn_checks || [],
    { allowed: (value) => value ? 'yes' : 'no' },
  );
  $('#impersonationTable').innerHTML = renderTable(
    [
      { key: 'admin_user_id', label: 'Admin' },
      { key: 'target_user_id', label: 'Target' },
      { key: 'status', label: 'Status' },
      { key: 'reason', label: 'Reason' },
    ],
    security.impersonation_sessions || [],
    { status: (value) => statusPill(value) },
  );
  $('#sodPolicyTable').innerHTML = renderTable(
    [
      { key: 'rule_key', label: 'Rule' },
      { key: 'severity', label: 'Severity' },
      { key: 'conflict_type', label: 'Type' },
      { key: 'left_value', label: 'Left' },
      { key: 'right_value', label: 'Right' },
    ],
    security.sod_report?.rules || [],
    { severity: (value) => statusPill(value) },
  );
  $('#accessReviewTable').innerHTML = renderTable(
    [
      { key: 'review_key', label: 'Review' },
      { key: 'status', label: 'Status' },
      { key: 'reviewer_user_id', label: 'Reviewer' },
      { key: 'findings', label: 'Findings' },
    ],
    security.access_reviews || [],
    {
      status: (value) => statusPill(value),
      findings: (value) => Array.isArray(value) ? value.length : 0,
    },
  );
}

function renderEvidence() {
  $('#commentTable').innerHTML = renderTable(
    [
      { key: 'entity_type', label: 'Entity' },
      { key: 'entity_id', label: 'ID' },
      { key: 'visibility', label: 'Visibility' },
      { key: 'comment_text', label: 'Comment' },
      { key: 'created_by', label: 'By' },
    ],
    state.bootstrap.comments?.comments || [],
  );

  $('#attachmentTable').innerHTML = renderTable(
    [
      { key: 'entity_type', label: 'Entity' },
      { key: 'entity_id', label: 'ID' },
      { key: 'file_name', label: 'File' },
      { key: 'retention_until', label: 'Retain until' },
      { key: 'created_by', label: 'By' },
    ],
    state.bootstrap.attachments?.attachments || [],
  );
}

async function renderCapabilityMap() {
  const capabilities = await api.get('/api/capabilities');
  const roadmap = await api.get('/api/roadmap');
  $('#capabilityMap').innerHTML = `
    <div class="grid two">
      <div>
        <h3>Current modules</h3>
        <ul>${capabilities.current_modules.map((item) => `<li>${item}</li>`).join('')}</ul>
        <h3>Different by design</h3>
        <ul>${capabilities.different_from_prophix.map((item) => `<li>${item}</li>`).join('')}</ul>
      </div>
      <div>
        <h3>Campus extensions</h3>
        <ul>${capabilities.campus_extensions_to_build_next.map((item) => `<li>${item}</li>`).join('')}</ul>
        <h3>Roadmap</h3>
        <p class="muted">Phase 1</p>
        <ul>${roadmap.phase_1.map((item) => `<li>${item}</li>`).join('')}</ul>
      </div>
    </div>
  `;
}

async function handleForecast() {
  if (!state.activeScenarioId) return;
  const result = await api.post(`/api/scenarios/${state.activeScenarioId}/forecast/run`, {});
  toast(`Forecast created ${result.created_line_items.length} rows.`);
  await loadBootstrap(state.activeScenarioId);
}

async function ensureWorkflowTemplate() {
  const existing = state.bootstrap.workflowOrchestration?.templates?.[0];
  if (existing) return existing;
  const userId = state.bootstrap.me?.id || 1;
  return api.post('/api/workflow-designer/templates', {
    template_key: `process-${Date.now().toString().slice(-5)}`,
    name: 'Close and budget campaign review',
    entity_type: 'close_campaign',
    active: true,
    steps: [
      { step_key: 'preparer', label: 'Preparer review', approver_user_id: userId, escalation_hours: 24, escalation_user_id: userId, notification_template: 'Preparer review is ready.' },
      { step_key: 'certifier', label: 'Certification review', approver_user_id: userId, escalation_hours: 48, escalation_user_id: userId, notification_template: 'Certification review is ready.' },
    ],
  });
}

async function handleWorkflowVisualSave() {
  const template = await ensureWorkflowTemplate();
  await api.post('/api/workflow-designer/visual-designs', {
    template_id: template.id,
    layout: {
      nodes: template.steps.map((step, index) => ({ id: step.step_key, label: step.label, x: 80 + index * 220, y: 80 })),
      edges: template.steps.slice(1).map((step, index) => ({ from: template.steps[index].step_key, to: step.step_key })),
    },
  });
  toast('Workflow visual layout saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleProcessCalendarCreate() {
  if (!state.activeScenarioId) return;
  const period = state.activePeriod || state.bootstrap.activeScenario?.start_period || '2026-08';
  await api.post('/api/workflow-designer/process-calendars', {
    scenario_id: state.activeScenarioId,
    calendar_key: `close-${period}`,
    process_type: 'close',
    period,
    milestones: [
      { key: 'submissions-due', title: 'Submissions due', offset_days: 3 },
      { key: 'certification', title: 'Certification packet', offset_days: 6 },
    ],
    status: 'active',
  });
  toast('Reusable process calendar saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleSubstituteApproverCreate() {
  const userId = state.bootstrap.me?.id || 1;
  const now = new Date();
  const end = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
  await api.post('/api/workflow-designer/substitute-approvers', {
    original_user_id: userId,
    substitute_user_id: userId,
    process_type: 'close',
    starts_at: now.toISOString(),
    ends_at: end.toISOString(),
    active: true,
  });
  toast('Substitute approver saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleWorkflowCertificationAssemble() {
  if (!state.activeScenarioId) return;
  const period = state.activePeriod || state.bootstrap.activeScenario?.start_period || '2026-08';
  await api.post('/api/workflow-designer/certification-packets', {
    scenario_id: state.activeScenarioId,
    process_type: 'close',
    period,
  });
  toast('Workflow certification packet assembled.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleWorkflowCampaignMonitor() {
  if (!state.activeScenarioId) return;
  const period = state.activePeriod || state.bootstrap.activeScenario?.start_period || '2026-08';
  const result = await api.post('/api/workflow-designer/campaign-monitors', {
    scenario_id: state.activeScenarioId,
    process_type: 'close',
    period,
  });
  toast(`Campaign monitor updated: ${result.completed_items}/${result.total_items} complete.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleScenarioCreate(formData) {
  const payload = Object.fromEntries(formData.entries());
  await api.post('/api/scenarios', payload);
  toast('Scenario created.');
  await loadBootstrap();
}

async function handleLineCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  const payload = { ...raw, amount: Number(raw.amount) };
  await api.post(`/api/scenarios/${state.activeScenarioId}/line-items`, payload);
  toast('Line item added.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleGuidedManualSave(formData) {
  if (!state.activeScenarioId) return;
  await handleLineCreate(formData);
  toast('Guided data entry saved.');
}

async function handleJournalCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/ledger-depth/journals', {
    scenario_id: state.activeScenarioId,
    ...raw,
    amount: Number(raw.amount),
  });
  toast('Journal adjustment created.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleScenarioApprove() {
  if (!state.activeScenarioId) return;
  await api.post(`/api/scenarios/${state.activeScenarioId}/approve`, {});
  toast('Scenario approved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleScenarioPublish() {
  if (!state.activeScenarioId) return;
  await api.post(`/api/scenarios/${state.activeScenarioId}/publish`, {});
  toast('Scenario published and locked.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleProfileSave() {
  await api.post('/api/ux/profile', {
    display_name: state.bootstrap.ux?.profile?.display_name || 'Admin',
    default_scenario_id: state.activeScenarioId,
    default_period: state.activePeriod,
    preferences: { compact_grid: true },
  });
  toast('Profile saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleGridValidate() {
  const inputs = ['#gridDept', '#gridFund', '#gridAccount', '#gridPeriod', '#gridAmount'].map((selector) => $(selector));
  inputs.forEach((input) => input?.setAttribute('aria-invalid', 'false'));
  const row = {
    department_code: $('#gridDept').value,
    fund_code: $('#gridFund').value,
    account_code: $('#gridAccount').value,
    period: $('#gridPeriod').value,
    amount: $('#gridAmount').value,
  };
  const result = await api.post('/api/ux/grids/validate', { scenario_id: state.activeScenarioId, rows: [row] });
  const message = $('#gridValidationMessage');
  if (result.valid) {
    message.textContent = 'Grid row is valid.';
    message.className = 'positive';
    return;
  }
  inputs.forEach((input) => input?.setAttribute('aria-invalid', 'true'));
  message.textContent = result.messages.map((item) => item.message).join(' ');
  message.className = 'negative';
}

async function handleBulkPaste() {
  const result = await api.post('/api/ux/bulk-paste', {
    scenario_id: state.activeScenarioId,
    paste_text: $('#bulkPasteText').value,
  });
  toast(`Bulk paste ${result.accepted_rows} accepted, ${result.rejected_rows} rejected.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleSubmissionCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/operating-budget/submissions', { scenario_id: state.activeScenarioId, ...raw });
  toast('Submission created.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleAssumptionCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  const payload = {
    scenario_id: state.activeScenarioId,
    ...raw,
    value: Number(raw.value),
    department_code: raw.department_code || null,
  };
  await api.post('/api/operating-budget/assumptions', payload);
  toast('Assumption saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleTransferCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/operating-budget/transfers', {
    scenario_id: state.activeScenarioId,
    ...raw,
    amount: Number(raw.amount),
  });
  toast('Transfer requested.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleTermCreate(formData) {
  if (!state.activeScenarioId) return;
  await api.post('/api/enrollment/terms', { scenario_id: state.activeScenarioId, ...Object.fromEntries(formData.entries()) });
  toast('Enrollment term saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleTuitionRateCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/enrollment/tuition-rates', {
    scenario_id: state.activeScenarioId,
    ...raw,
    rate_per_credit: Number(raw.rate_per_credit),
    default_credit_load: Number(raw.default_credit_load),
  });
  toast('Tuition rate saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleEnrollmentInputCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/enrollment/forecast-inputs', {
    scenario_id: state.activeScenarioId,
    ...raw,
    headcount: Number(raw.headcount),
    fte: Number(raw.fte),
    retention_rate: Number(raw.retention_rate),
    yield_rate: Number(raw.yield_rate),
    discount_rate: Number(raw.discount_rate),
  });
  toast('Enrollment input saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleTuitionRun(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  const result = await api.post('/api/enrollment/tuition-forecast-runs', {
    scenario_id: state.activeScenarioId,
    term_code: raw.term_code,
  });
  toast(`Tuition forecast posted ${currency(result.net_revenue)}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function postCampus(path, formData, numericFields) {
  if (!state.activeScenarioId) return;
  const payload = { scenario_id: state.activeScenarioId, ...Object.fromEntries(formData.entries()) };
  for (const field of numericFields) payload[field] = Number(payload[field]);
  await api.post(path, payload);
  toast('Campus planning record saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleTypedDriverCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/scenario-engine/drivers', {
    scenario_id: state.activeScenarioId,
    ...raw,
    value: Number(raw.value),
    locked: false,
  });
  toast('Typed driver saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleScenarioClone(formData) {
  if (!state.activeScenarioId) return;
  const clone = await api.post(`/api/scenario-engine/scenarios/${state.activeScenarioId}/clone`, Object.fromEntries(formData.entries()));
  toast(`Scenario cloned: ${clone.name}.`);
  await loadBootstrap(clone.id);
}

async function handleScenarioForecast(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  const result = await api.post('/api/scenario-engine/forecast-runs', {
    scenario_id: state.activeScenarioId,
    ...raw,
    department_code: raw.department_code || null,
    driver_key: raw.driver_key || null,
    confidence: Number(raw.confidence),
  });
  toast(`Forecast posted ${result.created_lines.length} rows.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleActualsIngest(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/scenario-engine/actuals', {
    scenario_id: state.activeScenarioId,
    source_version: raw.source_version,
    rows: [
      {
        scenario_id: state.activeScenarioId,
        department_code: raw.department_code,
        fund_code: raw.fund_code,
        account_code: raw.account_code,
        period: raw.period,
        amount: Number(raw.amount),
        notes: 'Actuals ingestion',
      },
    ],
  });
  toast('Actuals ingested.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleForecastVarianceRun() {
  if (!state.activeScenarioId) return;
  const result = await api.post(`/api/scenario-engine/forecast-actual-variances/run?scenario_id=${state.activeScenarioId}`, {});
  toast(`Variance calculated for ${result.count} rows.`);
  await loadBootstrap(state.activeScenarioId);
}

async function ensureForecastModelChoice() {
  const existing = state.bootstrap.predictiveForecasting?.model_choices?.[0];
  if (existing) return existing;
  const choice = await api.post('/api/scenario-engine/model-choices', {
    scenario_id: state.activeScenarioId,
    choice_key: `tuition-predictive-${Date.now().toString().slice(-5)}`,
    account_code: 'TUITION',
    department_code: 'SCI',
    selected_method: 'seasonal',
    seasonality_mode: 'monthly',
    confidence_level: 0.85,
  });
  await api.post('/api/scenario-engine/tuning-profiles', {
    choice_id: choice.id,
    seasonality_strength: 1,
    confidence_level: 0.85,
    confidence_spread: 0.15,
    driver_weights: { tuition_growth: 0.7 },
  });
  return choice;
}

async function handleForecastModelSelect() {
  if (!state.activeScenarioId) return;
  const choice = await ensureForecastModelChoice();
  toast(`Predictive model selected: ${choice.selected_method}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleForecastModelTune() {
  if (!state.activeScenarioId) return;
  const choice = await ensureForecastModelChoice();
  await api.post('/api/scenario-engine/tuning-profiles', {
    choice_id: choice.id,
    seasonality_strength: 1.15,
    confidence_level: 0.9,
    confidence_spread: 0.1,
    driver_weights: { tuition_growth: 0.75, retention: 0.25 },
  });
  toast('Forecast tuning profile saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleForecastBacktest() {
  if (!state.activeScenarioId) return;
  const choice = await ensureForecastModelChoice();
  const result = await api.post('/api/scenario-engine/backtests', {
    choice_id: choice.id,
    period_start: '2026-08',
    period_end: '2026-09',
  });
  toast(`Backtest accuracy ${Math.round(Number(result.accuracy_score) * 100)}%.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleForecastRecommendationCompare() {
  if (!state.activeScenarioId) return;
  const result = await api.post('/api/scenario-engine/recommendations/compare', {
    scenario_id: state.activeScenarioId,
    account_code: 'TUITION',
    department_code: 'SCI',
    methods: ['straight_line', 'seasonal', 'historical_trend', 'rolling_average'],
  });
  toast(`Recommended method: ${result.recommended_method}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleForecastDriverExplain() {
  if (!state.activeScenarioId) return;
  const result = await api.post(`/api/scenario-engine/driver-explanations/run?scenario_id=${state.activeScenarioId}&account_code=TUITION&department_code=SCI`, {});
  toast(`Driver explanations created: ${result.count}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleModelCreate() {
  if (!state.activeScenarioId) return;
  const key = `model-${Date.now().toString().slice(-5)}`;
  await api.post('/api/model-builder/models', {
    scenario_id: state.activeScenarioId,
    model_key: key,
    name: 'Campus planning model',
    description: 'User-defined formulas and allocation rules.',
    status: 'active',
  });
  toast('Planning model created.');
  await loadBootstrap(state.activeScenarioId);
}

function activeModelId() {
  return state.bootstrap.planningModels?.models?.[0]?.id || null;
}

async function ensureActiveModel() {
  if (activeModelId()) return activeModelId();
  await handleModelCreate();
  return activeModelId();
}

async function handleModelFormulaCreate() {
  if (!state.activeScenarioId) return;
  const modelId = await ensureActiveModel();
  if (!modelId) return;
  const period = state.activePeriod || state.bootstrap.activeScenario?.start_period || '2026-08';
  await api.post('/api/scenario-engine/drivers', {
    scenario_id: state.activeScenarioId,
    driver_key: 'model_units',
    label: 'Model units',
    driver_type: 'count',
    unit: 'units',
    value: 10,
    locked: false,
  });
  await api.post('/api/model-builder/formulas', {
    model_id: modelId,
    formula_key: `formula_${Date.now().toString().slice(-5)}`,
    label: 'Model generated line',
    expression: 'model_units * 100',
    target_account_code: 'MODEL_REV',
    target_department_code: 'MODEL',
    target_fund_code: 'GEN',
    period_start: period,
    period_end: period,
    active: true,
  });
  toast('Model formula added.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleAllocationRuleCreate() {
  if (!state.activeScenarioId) return;
  const modelId = await ensureActiveModel();
  if (!modelId) return;
  const period = state.activePeriod || state.bootstrap.activeScenario?.start_period || '2026-08';
  await api.post('/api/model-builder/allocation-rules', {
    model_id: modelId,
    rule_key: `alloc_${Date.now().toString().slice(-5)}`,
    label: 'Shared cost allocation',
    source_account_code: 'UTILITIES',
    source_department_code: 'OPS',
    target_account_code: 'UTILITIES_ALLOC',
    target_fund_code: 'GEN',
    basis_account_code: 'SALARY',
    basis_driver_key: null,
    target_department_codes: ['ART', 'SCI'],
    period_start: period,
    period_end: period,
    active: true,
  });
  toast('Allocation rule added.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleModelRecalculate() {
  const modelId = activeModelId();
  if (!modelId) {
    toast('Create a model first.');
    return;
  }
  const result = await api.post(`/api/model-builder/models/${modelId}/recalculate`, {});
  toast(`Model recalculation ${result.status}: ${result.ledger_entry_count} ledger rows.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleModelCubeBuild() {
  const modelId = await ensureActiveModel();
  if (!modelId) return;
  const result = await api.post(`/api/model-builder/models/${modelId}/cube/build`, {});
  toast(`Cube built with ${result.cell_count} populated cells.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleModelPublish() {
  const modelId = await ensureActiveModel();
  if (!modelId) return;
  const result = await api.post(`/api/model-builder/models/${modelId}/publish`, {});
  toast(`Model published as ${result.version_key}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleModelPerformanceTest() {
  const modelId = await ensureActiveModel();
  if (!modelId) return;
  const result = await api.post(`/api/model-builder/models/${modelId}/performance-test`, {});
  toast(`Performance test ${result.status}: ${result.elapsed_ms} ms.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleProfitabilityPoolCreate() {
  if (!state.activeScenarioId) return;
  await api.post('/api/profitability/cost-pools', {
    scenario_id: state.activeScenarioId,
    pool_key: `svc-${Date.now().toString().slice(-5)}`,
    name: 'Service center allocation',
    source_department_code: 'OPS',
    source_account_code: 'SUPPLIES',
    allocation_basis: 'revenue',
    target_type: 'department',
    target_codes: ['ART', 'SCI'],
    active: true,
  });
  toast('Profitability cost pool saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleProfitabilityAllocationRun() {
  if (!state.activeScenarioId) return;
  const period = state.activePeriod || state.bootstrap.activeScenario?.start_period || '2026-08';
  const result = await api.post('/api/profitability/allocation-runs', {
    scenario_id: state.activeScenarioId,
    period,
    pool_keys: [],
  });
  toast(`Allocated ${currency(result.total_allocated_cost)} across profitability targets.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleProfitabilitySnapshot() {
  if (!state.activeScenarioId) return;
  const start = state.bootstrap.activeScenario?.start_period || '2026-08';
  const end = state.bootstrap.activeScenario?.end_period || start;
  await api.post(`/api/profitability/snapshots?scenario_id=${state.activeScenarioId}&period_start=${start}&period_end=${end}&snapshot_type=profitability_package`, {});
  toast('Profitability snapshot created.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleAuditSeal() {
  const result = await api.post('/api/compliance/audit/seal', {});
  toast(`Audit sealed: ${result.sealed} rows.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleAuditVerify() {
  const result = await api.get('/api/compliance/audit/verify');
  toast(`Audit verification ${result.valid ? 'passed' : 'needs review'}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleRetentionPolicyCreate() {
  await api.post('/api/compliance/retention-policies', {
    policy_key: `audit-${Date.now().toString().slice(-5)}`,
    entity_type: 'audit_logs',
    retention_years: 7,
    disposition_action: 'archive',
    legal_hold: false,
    active: true,
  });
  toast('Retention policy saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleCertificationCreate() {
  if (!state.activeScenarioId) return;
  const period = state.activePeriod || state.bootstrap.activeScenario?.start_period || '2026-08';
  await api.post('/api/compliance/certifications', {
    scenario_id: state.activeScenarioId,
    certification_key: `close-cert-${period}-${Date.now().toString().slice(-5)}`,
    control_area: 'close',
    period,
    owner: state.bootstrap.ux?.profile?.email || 'finance.admin',
    notes: 'Certification created from compliance workspace.',
  });
  toast('Certification created.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleTaxActivityClassify() {
  if (!state.activeScenarioId) return;
  const period = state.activePeriod || state.bootstrap.activeScenario?.start_period || '2026-08';
  const result = await api.post('/api/compliance/tax/classifications', {
    scenario_id: state.activeScenarioId,
    activity_name: 'Auxiliary taxable activity review',
    tax_status: 'taxable',
    activity_tag: 'unrelated_business',
    income_type: 'unrelated_business',
    ubit_code: 'campus-auxiliary',
    regularly_carried_on: true,
    substantially_related: false,
    debt_financed: false,
    amount: 1000,
    expense_offset: 250,
    form990_part: 'VIII',
    form990_line: '11',
    form990_column: 'C',
    review_status: 'needs_review',
    notes: `Created from compliance workspace for ${period}.`,
    metadata: { source: 'workspace_quick_action' },
  });
  await api.post('/api/compliance/tax/form990', {
    scenario_id: state.activeScenarioId,
    period,
    form_part: 'VIII',
    line_number: '11',
    column_code: 'C',
    description: 'Unrelated business revenue support',
    amount: result.amount,
    basis: { classification_key: result.classification_key },
    review_status: 'needs_review',
  });
  toast('Tax classification created.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleTaxUpdateCheckRun() {
  const source = state.bootstrap.taxCompliance?.rule_sources?.[0];
  if (!source) {
    toast('No tax rule source is registered.');
    return;
  }
  const result = await api.post('/api/compliance/tax/update-checks', {
    source_key: source.source_key,
    observed_version: source.latest_known_version,
    detail: { checked_from: 'compliance_workspace' },
  });
  toast(`Tax update check ${result.status}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleSSOProductionConfigure() {
  await api.post('/api/security/sso-production-settings', {
    provider_key: 'campus-sso',
    environment: 'production',
    metadata_url: 'https://login.microsoftonline.com/manchester.edu/.well-known/openid-configuration',
    required_claim: 'email',
    group_claim: 'groups',
    jit_provisioning: true,
    status: 'ready',
  });
  toast('SSO production wiring saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleADOUGroupMappingCreate() {
  await api.post('/api/security/ad-ou-group-mappings', {
    mapping_key: `finance-ou-${Date.now().toString().slice(-5)}`,
    ad_group_dn: 'CN=muFinances Finance,OU=Groups,DC=manchester,DC=edu',
    allowed_ou_dn: 'OU=muFinances Users,OU=Finance,DC=manchester,DC=edu',
    role_key: 'budget.office',
    dimension_kind: 'department',
    dimension_code: 'SCI',
    active: true,
  });
  toast('AD/OU group mapping saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleDomainVPNCheckRun() {
  await api.post('/api/security/domain-vpn-checks', {
    host: 'mufinances.manchester.edu',
    client_host: '10.30.44.12',
    forwarded_host: 'mufinances.manchester.edu',
    forwarded_for: '10.30.44.12',
  });
  toast('Domain/VPN enforcement check recorded.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleAdminImpersonationStart() {
  const target = (state.bootstrap.enterpriseSecurity?.users || []).find((user) => user.id !== state.bootstrap.me?.id);
  if (!target) {
    toast('Create another user before impersonation.');
    return;
  }
  await api.post('/api/security/impersonations', {
    target_user_id: target.id,
    reason: 'Access support validation',
  });
  toast('Impersonation token issued and audited.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleSoDPolicyCreate() {
  await api.post('/api/security/sod-policies', {
    rule_key: `sod-${Date.now().toString().slice(-5)}`,
    name: 'Finance admin and budget office review',
    conflict_type: 'role_pair',
    left_value: 'finance.admin',
    right_value: 'budget.office',
    severity: 'high',
    active: true,
  });
  toast('SoD policy saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleAccessReviewCreate() {
  await api.post('/api/security/access-reviews', {
    review_key: `access-review-${Date.now().toString().slice(-5)}`,
    reviewer_user_id: state.bootstrap.me?.id || 1,
    scenario_id: state.activeScenarioId,
    scope: { roles: true, dimensions: true, sso: true },
  });
  toast('User access review created.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleReportDefinitionCreate(formData) {
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/reporting/reports', { ...raw, filters: {} });
  toast('Report definition saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleDashboardWidgetCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/reporting/widgets', {
    scenario_id: state.activeScenarioId,
    ...raw,
    config: {},
  });
  toast('Dashboard widget saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleScheduledExportCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/reporting/exports', {
    scenario_id: state.activeScenarioId,
    ...raw,
    report_definition_id: Number(raw.report_definition_id),
  });
  toast('Export scheduled.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleCloseChecklistCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/close/checklists', { scenario_id: state.activeScenarioId, ...raw });
  toast('Close checklist item saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleCloseTemplateCreate(formData) {
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/close/templates', {
    template_key: raw.template_key,
    title: raw.title,
    owner_role: raw.owner_role,
    due_day_offset: Number(raw.due_day_offset),
    dependency_keys: raw.dependency_keys ? raw.dependency_keys.split(',').map((item) => item.trim()).filter(Boolean) : [],
    active: true,
  });
  toast('Close task template saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleCloseCalendarCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/close/calendar', { scenario_id: state.activeScenarioId, ...raw });
  toast('Close calendar saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleCloseTemplatesInstantiate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  const result = await api.post(`/api/close/templates/instantiate?scenario_id=${state.activeScenarioId}&period=${encodeURIComponent(raw.period)}`, {});
  toast(`Generated ${result.count} close tasks.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handlePeriodLock(lockState) {
  if (!state.activeScenarioId) return;
  const period = $('#periodLockInput')?.value || '2026-08';
  await api.post(`/api/close/calendar/${encodeURIComponent(period)}/lock?scenario_id=${state.activeScenarioId}`, { lock_state: lockState });
  toast(`Period ${period} ${lockState}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleReconciliationCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/close/reconciliations', {
    scenario_id: state.activeScenarioId,
    ...raw,
    source_balance: Number(raw.source_balance),
  });
  toast('Reconciliation saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleEntityConfirmationCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/close/entity-confirmations', { scenario_id: state.activeScenarioId, ...raw });
  toast('Entity confirmation requested.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleConsolidationEntityCreate(formData) {
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/close/consolidation-entities', {
    ...raw,
    parent_entity_code: raw.parent_entity_code || null,
    active: true,
  });
  toast('Consolidation entity saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleEntityOwnershipCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/close/entity-ownerships', {
    scenario_id: state.activeScenarioId,
    ...raw,
    ownership_percent: Number(raw.ownership_percent),
  });
  toast('Ownership percentage saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleConsolidationSettingCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/close/consolidation-settings', {
    scenario_id: state.activeScenarioId,
    ...raw,
    enabled: true,
  });
  toast('Consolidation setting saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleConsolidationRuleCreate() {
  if (!state.activeScenarioId) return;
  await api.post('/api/close/consolidation-rules', {
    scenario_id: state.activeScenarioId,
    rule_key: `stat-rule-${Date.now().toString().slice(-5)}`,
    rule_type: 'statutory_schedule',
    source_filter: { account_family: 'all' },
    action: { schedule_type: 'multi_book_bridge', required: true },
    priority: 50,
    active: true,
  });
  toast('Consolidation rule saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleIntercompanyCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/close/intercompany-matches', {
    scenario_id: state.activeScenarioId,
    ...raw,
    source_amount: Number(raw.source_amount),
    target_amount: Number(raw.target_amount),
  });
  toast('Intercompany match saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleEliminationCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/close/eliminations', {
    scenario_id: state.activeScenarioId,
    ...raw,
    amount: Number(raw.amount),
  });
  toast('Elimination posted.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleConsolidationRun(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  const result = await api.post('/api/close/consolidation-runs', {
    scenario_id: state.activeScenarioId,
    period: raw.period,
  });
  toast(`Consolidation complete: ${currency(result.consolidated_total)}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleStatutoryPackAssemble() {
  if (!state.activeScenarioId) return;
  const run = state.bootstrap.consolidationRuns?.runs?.[0];
  if (!run) {
    toast('Run consolidation first.');
    return;
  }
  const result = await api.post('/api/close/statutory-packs', {
    consolidation_run_id: run.id,
    book_basis: 'US_GAAP',
    reporting_currency: 'USD',
  });
  toast(`Statutory pack assembled: ${result.pack_key}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleConnectorCreate(formData) {
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/integrations/connectors', { ...raw, config: { mode: 'localhost' } });
  toast('Connector saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function prepareImportPayload(formData, includeInvalidRows = false) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/integrations/connectors', {
    connector_key: raw.connector_key,
    name: raw.source_system || raw.connector_key,
    system_type: 'file',
    direction: 'inbound',
    config: { mode: 'export_file', source_system: raw.source_system || 'external export' },
  });
  const parsedRows = parseExportRows(raw.export_rows || '', includeInvalidRows);
  const rows = parsedRows.length
    ? parsedRows
    : [
        {
          department_code: raw.department_code,
          fund_code: raw.fund_code,
          account_code: raw.account_code,
          period: raw.period,
          amount: Number(raw.amount),
          notes: 'Browser import row',
        },
      ];
  return { raw, rows };
}

async function handleImportRun(formData) {
  if (!state.activeScenarioId) return;
  const { raw, rows } = await prepareImportPayload(formData);
  await api.post('/api/integrations/imports', {
    scenario_id: state.activeScenarioId,
    connector_key: raw.connector_key,
    source_format: raw.source_format,
    import_type: 'ledger',
    rows,
  });
  toast(`Import completed: ${rows.length} row${rows.length === 1 ? '' : 's'} submitted.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleImportPreview(formData) {
  if (!state.activeScenarioId) return;
  const { raw, rows } = await prepareImportPayload(formData, true);
  const preview = await api.post('/api/integrations/staging/preview', {
    scenario_id: state.activeScenarioId,
    connector_key: raw.connector_key,
    source_format: raw.source_format,
    import_type: 'ledger',
    source_name: raw.source_system || 'external export',
    rows,
  });
  toast(`Preview ready: ${preview.valid_rows} valid, ${preview.rejected_rows} rejected.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleGuidedImportRun(formData) {
  await handleImportRun(formData);
  toast('Guided import completed.');
}

async function handleGuidedExportRun(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/integrations/powerbi-exports', {
    scenario_id: state.activeScenarioId,
    dataset_name: raw.dataset_name,
  });
  toast('Guided export created.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleExcelTemplateCreate() {
  if (!state.activeScenarioId) return;
  const result = await api.post(`/api/office/excel-template?scenario_id=${state.activeScenarioId}`, {});
  $('#officeInteropMessage').textContent = `Excel template ready: ${result.file_name}. Base64 returned by API for round-trip import.`;
  toast('Excel template created.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleWorkbookPackageCreate() {
  if (!state.activeScenarioId) return;
  const result = await api.post(`/api/office/workbook-package?scenario_id=${state.activeScenarioId}`, {});
  $('#officeInteropMessage').textContent = `Workbook package ready: ${result.file_name}.`;
  toast('Workbook package created.');
  await loadBootstrap(state.activeScenarioId);
}

function latestOfficeWorkbook() {
  return state.bootstrap.officeWorkbooks?.workbooks?.[0] || null;
}

async function handleOfficeWorkbookRefresh() {
  const workbook = latestOfficeWorkbook();
  if (!workbook) {
    toast('Create an Excel template first.');
    return;
  }
  const result = await api.post(`/api/office/workbooks/${encodeURIComponent(workbook.workbook_key)}/refresh`, {});
  $('#officeInteropMessage').textContent = result.message;
  toast('Excel workspace refreshed.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleOfficeWorkbookPublish() {
  const workbook = latestOfficeWorkbook();
  if (!workbook) {
    toast('Create an Excel template first.');
    return;
  }
  const result = await api.post(`/api/office/workbooks/${encodeURIComponent(workbook.workbook_key)}/publish`, {});
  $('#officeInteropMessage').textContent = result.message;
  toast('Excel workspace published.');
  await loadBootstrap(state.activeScenarioId);
}

async function handlePowerPointRefresh() {
  if (!state.activeScenarioId) return;
  const result = await api.post(`/api/office/powerpoint-refresh?scenario_id=${state.activeScenarioId}`, {});
  $('#officeInteropMessage').textContent = `PowerPoint deck refreshed: ${result.file_name}.`;
  toast('PowerPoint board deck refreshed.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleOfficeCommentCreate() {
  const workbook = latestOfficeWorkbook();
  if (!workbook || !state.activeScenarioId) {
    toast('Create an Excel template first.');
    return;
  }
  const cell = $('#officeCommentCellInput').value || 'E2';
  const comment = $('#officeCommentTextInput').value || 'Review this budget variance';
  await api.post('/api/office/cell-comments', {
    scenario_id: state.activeScenarioId,
    workbook_key: workbook.workbook_key,
    sheet_name: 'LedgerInput',
    cell_ref: cell,
    comment_text: comment,
  });
  toast('Excel cell comment recorded.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleExcelRoundtripImport(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  const result = await api.post('/api/office/excel-import', {
    scenario_id: state.activeScenarioId,
    workbook_key: raw.workbook_key,
    workbook_base64: raw.workbook_base64,
    sheet_name: 'LedgerInput',
  });
  $('#officeInteropMessage').textContent = `Round-trip import ${result.status}: ${result.accepted_rows} accepted, ${result.rejected_rows} rejected.`;
  toast('Excel round-trip import processed.');
  await loadBootstrap(state.activeScenarioId);
}

function parseExportRows(text, includeInvalidRows = false) {
  const trimmed = String(text || '').trim();
  if (!trimmed) return [];
  const delimiter = trimmed.includes('\t') ? '\t' : ',';
  const lines = trimmed.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];
  const headers = splitDelimitedLine(lines[0], delimiter).map(normalizeImportHeader);
  return lines.slice(1).map((line) => {
    const values = splitDelimitedLine(line, delimiter);
    const row = {};
    headers.forEach((header, index) => {
      if (!header) return;
      row[header] = values[index] ?? '';
    });
    if (row.amount !== undefined) row.amount = Number(String(row.amount).replace(/[$,]/g, ''));
    return row;
  }).filter((row) => includeInvalidRows || (row.department_code && row.fund_code && row.account_code && row.period && Number.isFinite(Number(row.amount))));
}

function splitDelimitedLine(line, delimiter) {
  const values = [];
  let current = '';
  let inQuotes = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];
    if (char === '"' && next === '"') {
      current += '"';
      index += 1;
    } else if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === delimiter && !inQuotes) {
      values.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  values.push(current.trim());
  return values;
}

function normalizeImportHeader(header) {
  const key = String(header || '').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
  const aliases = {
    dept: 'department_code',
    department: 'department_code',
    department_code: 'department_code',
    cost_center: 'department_code',
    fund: 'fund_code',
    fund_code: 'fund_code',
    account: 'account_code',
    account_code: 'account_code',
    gl_account: 'account_code',
    period: 'period',
    fiscal_period: 'period',
    month: 'period',
    amount: 'amount',
    value: 'amount',
    dollars: 'amount',
    notes: 'notes',
    description: 'notes',
    comment: 'notes',
  };
  return aliases[key] || key;
}

async function handleSyncJobRun(formData) {
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/integrations/sync-jobs', raw);
  toast('Sync job completed.');
  await loadBootstrap(state.activeScenarioId);
}

async function handlePowerBiExportCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/integrations/powerbi-exports', {
    scenario_id: state.activeScenarioId,
    dataset_name: raw.dataset_name,
  });
  toast('Power BI export ready.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleMappingTemplateCreate(formData) {
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/integrations/mapping-templates', {
    template_key: raw.template_key,
    connector_key: raw.connector_key,
    import_type: raw.import_type,
    mapping: { [raw.source_field]: raw.target_field },
    active: true,
  });
  toast('Mapping template saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleValidationRuleCreate(formData) {
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/integrations/validation-rules', {
    ...raw,
    expected_value: raw.expected_value || null,
    severity: 'error',
    active: true,
  });
  toast('Validation rule saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleCredentialCreate(formData) {
  await api.post('/api/integrations/credentials', Object.fromEntries(formData.entries()));
  toast('Credential stored.');
  await loadBootstrap(state.activeScenarioId);
}

function firstConnector() {
  return state.bootstrap.connectors?.connectors?.[0] || null;
}

async function ensureConnector() {
  const existing = firstConnector();
  if (existing) return existing;
  await api.post('/api/integrations/connectors', {
    connector_key: 'erp-gl',
    name: 'ERP General Ledger',
    system_type: 'erp',
    direction: 'inbound',
    config: { adapter_key: 'erp_gl', mode: 'marketplace' },
  });
  await loadBootstrap(state.activeScenarioId);
  return firstConnector();
}

async function handleConnectorAuthStart() {
  const connector = await ensureConnector();
  if (!connector) return;
  const adapterKey = connector.config?.adapter_key || 'erp_gl';
  await api.post('/api/integrations/auth-flows', {
    connector_key: connector.connector_key,
    adapter_key: adapterKey,
    credential_ref: `vault://${connector.connector_key}/api-token`,
  });
  toast('Connector auth flow recorded.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleConnectorHealthRun() {
  const connector = await ensureConnector();
  if (!connector) return;
  const result = await api.post(`/api/integrations/connectors/${connector.connector_key}/health`, {});
  toast(`Connector health: ${result.status}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleMappingPresetApply() {
  const connector = await ensureConnector();
  if (!connector) return;
  const preset = state.bootstrap.connectorMarketplace?.mapping_presets?.find((item) => item.adapter_key === (connector.config?.adapter_key || 'erp_gl')) || state.bootstrap.connectorMarketplace?.mapping_presets?.[0];
  if (!preset) {
    toast('No mapping preset available.');
    return;
  }
  await api.post('/api/integrations/mapping-presets/apply', {
    connector_key: connector.connector_key,
    preset_key: preset.preset_key,
  });
  toast('Mapping preset applied.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleMasterDataChangeRequest() {
  if (!state.activeScenarioId) return;
  const code = `B39_${Date.now().toString().slice(-4)}`;
  await api.post('/api/data-hub/change-requests', {
    dimension_kind: 'account',
    code,
    name: `Governed account ${code}`,
    change_type: 'create',
    effective_from: state.activePeriod || '2026-08',
    metadata: { account_group: 'Operating', requested_from: 'data_hub_workspace' },
  });
  toast('Master data change requested.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleMasterDataChangeApprove() {
  const first = state.bootstrap.dataHub?.change_requests?.find((item) => item.status === 'pending');
  if (!first) {
    toast('No pending master data request.');
    return;
  }
  await api.post(`/api/data-hub/change-requests/${first.id}/approve`, {});
  toast('Master data change approved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleMasterDataMappingCreate() {
  const suffix = Date.now().toString().slice(-5);
  await api.post('/api/data-hub/mappings', {
    mapping_key: `erp-account-${suffix}`,
    source_system: 'ERP',
    source_dimension: 'account',
    source_code: `ERP-${suffix}`,
    target_dimension: 'account',
    target_code: 'SUPPLIES',
    effective_from: state.activePeriod || '2026-08',
    active: true,
  });
  toast('Master data mapping saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleMetadataApprovalRequest() {
  await api.post('/api/data-hub/metadata-approvals', {
    entity_type: 'dimension_member',
    entity_id: 'account:SUPPLIES',
    metadata: { steward: 'Budget Office', approval_note: 'B39 metadata governance' },
  });
  toast('Metadata approval requested.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleDataLineageBuild() {
  if (!state.activeScenarioId) return;
  const result = await api.post(`/api/data-hub/lineage/build?scenario_id=${state.activeScenarioId}&target_type=report&target_id=financial_statement`, {});
  toast(`Lineage built with ${result.count} source group${result.count === 1 ? '' : 's'}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleAutomationRun(assistantType) {
  if (!state.activeScenarioId) return;
  const result = await api.post('/api/automation/run', {
    scenario_id: state.activeScenarioId,
    assistant_type: assistantType,
  });
  toast(`${assistantType} assistant created ${result.count} recommendations.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handlePlanningAgentRun(agentType) {
  if (!state.activeScenarioId) return;
  const prompts = {
    budget_update: 'Add 2500 to science supplies for 2026-08',
    bulk_adjustment: 'Increase supplies by 3% for 2026-08',
    report_question: 'What is the current net position by revenue and expense?',
    anomaly_explanation: 'Explain the largest unusual ledger movement',
  };
  const result = await api.post('/api/automation/planning-agents/run', {
    scenario_id: state.activeScenarioId,
    agent_type: agentType,
    prompt_text: prompts[agentType],
  });
  toast(`${agentType.replace('_', ' ')} drafted: ${result.action.status}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleAgentActionApprove() {
  const first = state.bootstrap.agentActions?.actions?.find((action) => action.status === 'pending_approval');
  if (!first) {
    toast('No pending agent action.');
    return;
  }
  const result = await api.post(`/api/automation/planning-agents/actions/${first.id}/approve`, { note: 'Approved from automation workspace.' });
  toast(`Agent action ${result.status}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleOpsCheckRun(formData) {
  await api.post('/api/operations/checks', Object.fromEntries(formData.entries()));
  toast('Operational check recorded.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleOpsBackupCreate() {
  await api.post('/api/operations/backups', {});
  toast('Operations backup created.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleRestoreTestRun(formData) {
  const raw = Object.fromEntries(formData.entries());
  if (!raw.backup_key) throw new Error('Create a backup before running a restore test.');
  await api.post('/api/operations/restore-tests', raw);
  toast('Restore test recorded.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleRunbookRegister(formData) {
  await api.post('/api/operations/runbooks', Object.fromEntries(formData.entries()));
  toast('Runbook registered.');
  await loadBootstrap(state.activeScenarioId);
}

async function handlePerformanceLoadTestRun() {
  if (!state.activeScenarioId) return;
  const result = await api.post('/api/performance/load-tests', {
    scenario_id: state.activeScenarioId,
    test_type: 'calculation_benchmark',
    row_count: 5000,
    backend: 'runtime',
  });
  toast(`Benchmark completed in ${result.elapsed_ms} ms.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleBenchmarkHarnessRun() {
  if (!state.activeScenarioId) return;
  const result = await api.post('/api/performance/benchmarks/run', {
    scenario_id: state.activeScenarioId,
    dataset_key: 'campus-realistic-benchmark',
    row_count: 10000,
    backend: 'runtime',
    thresholds: {},
    include_import: true,
    include_reports: true,
  });
  toast(`Benchmark harness ${result.status}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleParallelCubedRun() {
  if (!state.activeScenarioId) return;
  const result = await api.post('/api/performance/parallel-cubed/run', {
    scenario_id: state.activeScenarioId,
    work_type: 'mixed',
    partition_strategy: 'balanced',
    row_count: 5000,
    include_import: true,
    include_reports: true,
  });
  const workers = result.worker_count || result.benchmark?.worker_count || 0;
  toast(`Parallel Cubed ${result.status}: ${workers} workers, ${result.partition_count} partitions.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleIndexStrategySeed() {
  const result = await api.post('/api/performance/index-recommendations/seed', {});
  toast(`${result.count} index recommendations ready.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handlePerformanceJobEnqueue() {
  if (!state.activeScenarioId) return;
  await api.post('/api/performance/jobs', {
    job_type: 'large_import_stress',
    priority: 50,
    payload: { scenario_id: state.activeScenarioId, row_count: 10000 },
  });
  toast('Background job queued.');
  await loadBootstrap(state.activeScenarioId);
}

async function handlePerformanceJobRun() {
  const result = await api.post('/api/performance/jobs/run-next', {});
  toast(result.ran ? `Job ${result.job.status}.` : result.message);
  await loadBootstrap(state.activeScenarioId);
}

async function handleCacheInvalidate() {
  await api.post('/api/performance/cache-invalidations', {
    cache_key: state.activeScenarioId ? `scenario:${state.activeScenarioId}` : 'global',
    scope: state.activeScenarioId ? 'scenario' : 'global',
    reason: 'Manual invalidation from reliability workspace.',
  });
  toast('Cache invalidation recorded.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleRestoreAutomationRun() {
  let backup = state.bootstrap.foundationBackups?.backups?.[0];
  if (!backup) {
    backup = await api.post('/api/operations/backups', {});
  }
  const result = await api.post('/api/performance/restore-automations', {
    backup_key: backup.backup_key,
    verify_only: true,
  });
  toast(`Restore automation ${result.status}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleHealthProbeRun() {
  const result = await api.post('/api/observability/health-probes/run', {});
  toast(`Health probes ${result.status}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleBackupDrillRun() {
  const result = await api.post('/api/observability/backup-restore-drills/run', {});
  toast(`Backup drill ${result.status}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleEnvironmentSave() {
  await api.post('/api/deployment-governance/environments', {
    environment_key: 'staging',
    tenant_key: 'manchester',
    base_url: 'https://mufinances.manchester.edu',
    database_backend: 'postgres',
    sso_required: true,
    domain_guard_required: true,
    settings: { port: 3200, release_channel: 'internal' },
    status: 'ready',
  });
  toast('Deployment environment saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleConfigExport() {
  await api.post('/api/deployment-governance/config-snapshots', {
    environment_key: 'staging',
    direction: 'export',
    payload: {},
  });
  toast('Configuration snapshot exported.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleEnvironmentPromotion() {
  await api.post('/api/deployment-governance/promotions', {
    from_environment: 'staging',
    to_environment: 'production',
    release_version: `B50-${Date.now().toString().slice(-5)}`,
    checklist: { tests_passed: true, backup_verified: true, readiness_complete: true },
  });
  toast('Environment promotion recorded.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleRollbackPlanCreate() {
  await api.post('/api/deployment-governance/rollback-plans', {
    migration_key: '0051_deployment_governance_release_controls',
    rollback_strategy: 'Restore the pre-promotion database backup, redeploy the previous app build, then re-run smoke diagnostics.',
    verification_steps: ['Confirm database integrity', 'Confirm login works', 'Confirm reporting status endpoints respond'],
    status: 'reviewed',
  });
  toast('Migration rollback plan saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleReleaseNotesPublish() {
  await api.post('/api/deployment-governance/release-notes', {
    release_version: 'B50',
    title: 'Deployment governance and release controls',
    notes: { added: ['Environment promotion', 'Config snapshots', 'Rollback plans', 'Readiness checklist'] },
    status: 'published',
  });
  toast('Release notes published.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleAdminDiagnosticsRun() {
  const result = await api.post('/api/deployment-governance/diagnostics/run?scope=release', {});
  toast(`Diagnostics ${result.status}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleReadinessMark() {
  await api.post('/api/deployment-governance/readiness', {
    item_key: 'release-governance-ready',
    category: 'operations',
    title: 'B50 release governance reviewed',
    status: 'ready',
    evidence: { checked_from: 'deployment_governance_workspace' },
  });
  toast('Readiness item marked ready.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleGuidanceTaskComplete() {
  const checklists = state.bootstrap.guidanceTraining?.checklists || [];
  const firstOpen = checklists.flatMap((checklist) =>
    (checklist.tasks || []).map((task) => ({ ...task, checklist_key: checklist.checklist_key })),
  ).find((task) => task.status !== 'completed');
  if (!firstOpen) {
    toast('All visible guidance tasks are complete.');
    return;
  }
  await api.post('/api/guidance/tasks/complete', {
    checklist_key: firstOpen.checklist_key,
    task_key: firstOpen.task_key,
  });
  toast('Guidance task completed.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleTrainingModeStart(modeKey) {
  await api.post('/api/guidance/training/start', {
    mode_key: modeKey,
    scenario_id: state.activeScenarioId,
  });
  toast(`${modeKey} training mode started.`);
  await loadBootstrap(state.activeScenarioId);
}

function signOut() {
  clearSession();
  showAuthGate();
}

async function handleCommentCreate(formData) {
  await api.post('/api/evidence/comments', Object.fromEntries(formData.entries()));
  toast('Comment saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleAttachmentCreate(formData) {
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/evidence/attachments', {
    ...raw,
    size_bytes: Number(raw.size_bytes),
    metadata: { source: 'browser' },
  });
  toast('Attachment metadata saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleBoardPackageCreate(formData) {
  if (!state.activeScenarioId) return;
  await api.post('/api/reporting/board-packages', {
    scenario_id: state.activeScenarioId,
    ...Object.fromEntries(formData.entries()),
  });
  toast('Board package assembled.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleExportArtifactCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/reporting/artifacts', {
    scenario_id: state.activeScenarioId,
    artifact_type: raw.artifact_type,
    file_name: raw.file_name,
    package_id: raw.package_id ? Number(raw.package_id) : null,
    retention_until: raw.retention_until || null,
  });
  toast('Export artifact created.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleSnapshotCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/reporting/snapshots', {
    scenario_id: state.activeScenarioId,
    snapshot_type: raw.snapshot_type,
    retention_until: raw.retention_until || null,
  });
  toast('Report snapshot retained.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleScheduledExtractRun(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  const result = await api.post('/api/reporting/scheduled-extract-runs', {
    scenario_id: state.activeScenarioId,
    export_id: raw.export_id ? Number(raw.export_id) : null,
    destination: raw.destination,
  });
  toast(`Extract complete: ${result.row_count} rows.`);
  await loadBootstrap(state.activeScenarioId);
}

async function ensureProductionReportBook() {
  if (!state.activeScenarioId) return null;
  const report = await api.post('/api/reporting/reports', {
    name: `Production statement ${Date.now().toString().slice(-4)}`,
    report_type: 'ledger_matrix',
    row_dimension: 'department_code',
    column_dimension: 'account_code',
    filters: {},
  });
  const layout = await api.post('/api/reporting/layouts', {
    scenario_id: state.activeScenarioId,
    report_definition_id: report.id,
    name: 'Pixel board layout',
    layout: {
      unit: 'px',
      page_size: 'Letter',
      grid: { x: 72, y: 72, width: 468, row_height: 28 },
      sections: ['financial_statement', 'variance', 'footnotes'],
    },
  });
  const chart = await api.post('/api/reporting/charts', {
    scenario_id: state.activeScenarioId,
    name: 'Board variance chart',
    chart_type: 'bar',
    dataset_type: 'period_range',
    config: { dimension: 'department_code', period_start: state.bootstrap.activeScenario?.start_period || '2026-07', period_end: state.bootstrap.activeScenario?.end_period || '2026-12' },
  });
  return api.post('/api/reporting/report-books', {
    scenario_id: state.activeScenarioId,
    name: 'Production board binder',
    layout_id: layout.id,
    period_start: state.bootstrap.activeScenario?.start_period || '2026-07',
    period_end: state.bootstrap.activeScenario?.end_period || '2026-12',
    report_definition_ids: [report.id],
    chart_ids: [chart.id],
  });
}

async function handlePaginationProfileCreate() {
  if (!state.activeScenarioId) return;
  await api.post('/api/reporting/pagination-profiles', {
    scenario_id: state.activeScenarioId,
    name: 'Board PDF portrait',
    page_size: 'Letter',
    orientation: 'portrait',
    margin_top: 0.5,
    margin_right: 0.5,
    margin_bottom: 0.5,
    margin_left: 0.5,
    rows_per_page: 30,
  });
  toast('PDF pagination profile saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleReportFootnoteCreate() {
  if (!state.activeScenarioId) return;
  await api.post('/api/reporting/footnotes', {
    scenario_id: state.activeScenarioId,
    target_type: 'financial_statement',
    marker: 'A',
    footnote_text: 'Amounts are rounded to whole dollars and sourced from the active planning ledger.',
    display_order: 1,
  });
  toast('Report footnote saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleBinderBreakCreate() {
  const book = await ensureProductionReportBook();
  if (!book) return;
  await api.post('/api/reporting/page-breaks', {
    report_book_id: book.id,
    section_key: 'variance',
    page_number: 2,
    break_before: true,
  });
  toast('Binder page break saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleReportChartFormat() {
  if (!state.activeScenarioId) return;
  let chart = state.bootstrap.productionReporting?.charts?.[0];
  if (!chart) {
    const book = await ensureProductionReportBook();
    await loadBootstrap(state.activeScenarioId);
    chart = state.bootstrap.productionReporting?.charts?.[0] || book?.contents?.charts?.[0];
  }
  if (!chart) {
    toast('Create a chart before formatting.');
    return;
  }
  await api.post(`/api/reporting/charts/${chart.id}/format`, {
    format: {
      palette: ['#7df0c6', '#f6c453', '#e26363'],
      label_position: 'outside_end',
      currency_axis: true,
      show_data_labels: true,
    },
  });
  toast('Chart formatting saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleReportChartRender() {
  if (!state.activeScenarioId) return;
  let chart = state.bootstrap.chartRendering?.charts?.[0] || state.bootstrap.productionReporting?.charts?.[0];
  if (!chart) {
    const book = await ensureProductionReportBook();
    await loadBootstrap(state.activeScenarioId);
    chart = state.bootstrap.chartRendering?.charts?.[0] || book?.contents?.charts?.[0];
  }
  if (!chart) {
    toast('Create a chart before rendering.');
    return;
  }
  const result = await api.post(`/api/reporting/charts/${chart.id}/render`, {
    render_format: 'svg',
    width: 960,
    height: 540,
  });
  await api.post('/api/reporting/artifacts', {
    scenario_id: state.activeScenarioId,
    artifact_type: 'png',
    file_name: `${chart.name || 'chart'} snapshot`,
    chart_id: chart.id,
  });
  toast(`Chart rendered: ${result.file_name}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleReportChartSnapshot() {
  if (!state.activeScenarioId) return;
  let chart = state.bootstrap.chartRendering?.charts?.[0] || state.bootstrap.productionReporting?.charts?.[0];
  if (!chart) {
    const book = await ensureProductionReportBook();
    await loadBootstrap(state.activeScenarioId);
    chart = state.bootstrap.chartRendering?.charts?.[0] || book?.contents?.charts?.[0];
  }
  const snapshot = await api.post('/api/reporting/dashboard-chart-snapshots', {
    scenario_id: state.activeScenarioId,
    chart_id: chart?.id || null,
  });
  toast(`Dashboard chart snapshot retained: ${snapshot.snapshot_key}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleBoardPackageReleaseFlow() {
  if (!state.activeScenarioId) return;
  let recurring = state.bootstrap.productionReporting?.recurring_packages?.[0];
  if (!recurring) {
    const book = await ensureProductionReportBook();
    recurring = await api.post('/api/reporting/recurring-packages', {
      scenario_id: state.activeScenarioId,
      book_id: book.id,
      schedule_cron: '0 8 1 * *',
      destination: 'board-release-package',
      next_run_at: null,
    });
  }
  await api.post(`/api/reporting/recurring-packages/${recurring.id}/release-request`, {});
  await api.post(`/api/reporting/recurring-packages/${recurring.id}/approve-release`, { note: 'Approved from production reporting workspace.' });
  await api.post(`/api/reporting/recurring-packages/${recurring.id}/release`, { note: 'Released for board package distribution.' });
  toast('Board package approved and released.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleVarianceThresholdCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/reporting/variance-thresholds', {
    scenario_id: state.activeScenarioId,
    threshold_key: raw.threshold_key,
    amount_threshold: Number(raw.amount_threshold),
    percent_threshold: raw.percent_threshold ? Number(raw.percent_threshold) : null,
    require_explanation: raw.require_explanation === 'on',
  });
  toast('Variance threshold saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleVarianceExplanationGenerate() {
  if (!state.activeScenarioId) return;
  const result = await api.post(`/api/reporting/variance-explanations/generate?scenario_id=${state.activeScenarioId}`, {});
  toast(`Variance explanations ready: ${result.explanations.length}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleVarianceNarrativeDraft() {
  if (!state.activeScenarioId) return;
  const result = await api.post(`/api/reporting/variance-explanations/draft?scenario_id=${state.activeScenarioId}`, {});
  toast(`AI drafts prepared: ${result.count}.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleUniversityAgentRefresh() {
  state.bootstrap.universityAgent = await api.get('/api/university-agent/workspace');
  renderAutomation();
  toast('University Agent registry refreshed.');
}

async function handleAIExplanationDraft() {
  if (!state.activeScenarioId) return;
  const result = await api.post(`/api/ai-explainability/explanations/draft?scenario_id=${state.activeScenarioId}`, {});
  toast(`Drafted ${result.count} cited explanations.`);
  await loadBootstrap(state.activeScenarioId);
}

async function handleAIExplanationSubmit() {
  const first = state.bootstrap.aiExplanations?.explanations?.[0];
  if (!first) {
    toast('Draft an explanation first.');
    return;
  }
  await api.post(`/api/ai-explainability/explanations/${first.id}/submit`, {});
  toast('AI explanation submitted.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleAIExplanationApprove() {
  const first = state.bootstrap.aiExplanations?.explanations?.[0];
  if (!first) {
    toast('Draft an explanation first.');
    return;
  }
  await api.post(`/api/ai-explainability/explanations/${first.id}/approve`, { note: 'Approved from explainability workspace.' });
  toast('AI explanation approved.');
  await loadBootstrap(state.activeScenarioId);
}

async function openMarketWatch() {
  $('#marketSatellite').classList.remove('hidden');
  const [marketLab, brokerage] = await Promise.all([api.get('/api/market-lab'), api.get('/api/brokerage')]);
  state.marketLab = marketLab;
  state.brokerage = brokerage;
  if (!state.marketSearch) {
    state.marketSearch = await api.get(`/api/market-lab/search?q=${encodeURIComponent($('#marketSearchInput').value || 'DOW')}`);
  }
  renderMarketLab();
}

function closeMarketWatch() {
  $('#marketSatellite').classList.add('hidden');
}

async function handleMarketSearch() {
  const symbol = $('#marketSearchInput').value || 'DOW';
  state.marketSearch = await api.get(`/api/market-lab/search?q=${encodeURIComponent(symbol)}`);
  const first = state.marketSearch?.results?.[0];
  if (first) $('#tradeSymbolInput').value = first.symbol;
  renderMarketLab();
}

async function handleMarketFavorite() {
  const symbol = $('#marketSearchInput').value || state.marketSearch?.results?.[0]?.symbol || 'DOW';
  await api.post('/api/market-lab/watchlist', { symbol });
  state.marketLab = await api.get('/api/market-lab');
  toast(`${symbol.toUpperCase()} saved to favorites.`);
  renderMarketLab();
}

async function handlePaperTrade(side) {
  const symbol = $('#tradeSymbolInput').value || 'DIA';
  const quantity = Number($('#tradeQuantityInput').value || 1);
  state.marketLab = await api.post('/api/market-lab/trades', { symbol, side, quantity });
  toast(`Paper ${side} filled for ${quantity} ${symbol.toUpperCase()}.`);
  renderMarketLab();
}

async function handlePaperReset() {
  state.marketLab = await api.post('/api/market-lab/account', { starting_cash: 100000 });
  state.marketLab = await api.get('/api/market-lab');
  toast('Paper account reset to $100,000.');
  renderMarketLab();
}

async function reloadBrokerageConnectors() {
  state.brokerage = await api.get('/api/brokerage');
  renderBrokerageConnectors();
}

async function handleBrokerageCreate() {
  const provider = $('#brokerageProviderSelect')?.value || 'generic_sandbox';
  const selectedMode = $('#brokerageModeSelect')?.value || 'sandbox';
  const providerEnvironment = selectedMode === 'live' ? 'live' : 'sandbox';
  await api.post('/api/brokerage/connections', {
    provider_key: provider,
    connection_name: provider === 'generic_sandbox' ? 'Sandbox brokerage' : `${provider} read-only`,
    mode: selectedMode === 'live' ? 'read_only' : 'sandbox',
    provider_environment: providerEnvironment,
    read_only_ack: provider === 'generic_sandbox',
    consent_status: provider === 'generic_sandbox' ? 'accepted' : 'not_requested',
    metadata: { purpose: 'paper lab brokerage connector test', source: 'connect brokerage account button' },
  });
  await reloadBrokerageConnectors();
  toast('Brokerage account connection started.');
}

function firstBrokerageConnection() {
  return state.brokerage?.connections?.[0] || null;
}

async function handleBrokerageTest() {
  let connection = firstBrokerageConnection();
  if (!connection) {
    await handleBrokerageCreate();
    connection = firstBrokerageConnection();
  }
  if (!connection) return;
  await api.post(`/api/brokerage/connections/${connection.id}/test`, {});
  await reloadBrokerageConnectors();
  toast('Brokerage connector test logged.');
}

async function handleBrokerageCredentialSetup() {
  let connection = firstBrokerageConnection();
  if (!connection) {
    await handleBrokerageCreate();
    connection = firstBrokerageConnection();
  }
  if (!connection) return;
  const credentialType = connection.provider_key === 'schwab' ? 'oauth_client' : 'api_key';
  const result = await api.post(`/api/brokerage/connections/${connection.id}/credential-setup`, {
    credential_type: credentialType,
    credential_ref: connection.provider_key === 'generic_sandbox' ? null : `vault://${connection.provider_key}/read-only-demo`,
    redirect_uri: '/oauth/brokerage/callback',
  });
  await reloadBrokerageConnectors();
  toast(result.auth_url ? 'OAuth setup URL prepared.' : 'Credential reference saved.');
}

async function handleBrokerageConsent() {
  let connection = firstBrokerageConnection();
  if (!connection) {
    await handleBrokerageCreate();
    connection = firstBrokerageConnection();
  }
  if (!connection) return;
  await api.post(`/api/brokerage/connections/${connection.id}/consent`, {
    consent_version: '2026.04.b66',
    read_only_ack: true,
    real_money_trading_ack: true,
    data_scope_ack: true,
    consent_text: 'I consent to read-only brokerage sync for balances and holdings. Real-money trading remains disabled.',
  });
  await reloadBrokerageConnectors();
  toast('Brokerage read-only consent recorded.');
}

async function handleBrokerageSync() {
  let connection = firstBrokerageConnection();
  if (!connection) {
    await handleBrokerageCreate();
    connection = firstBrokerageConnection();
  }
  if (!connection) return;
  if (connection.consent_status !== 'accepted') {
    await handleBrokerageConsent();
    connection = firstBrokerageConnection();
  }
  state.brokerage = await api.post(`/api/brokerage/connections/${connection.id}/sync`, {});
  renderBrokerageConnectors();
  toast('Brokerage holdings synced.');
}

async function handleVarianceExplanationCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/reporting/variance-explanations', {
    scenario_id: state.activeScenarioId,
    variance_key: raw.variance_key,
    explanation_text: raw.explanation_text,
  });
  toast('Variance commentary saved.');
  await loadBootstrap(state.activeScenarioId);
}

async function handleNarrativeCreate(formData) {
  if (!state.activeScenarioId) return;
  const raw = Object.fromEntries(formData.entries());
  await api.post('/api/reporting/narratives', {
    scenario_id: state.activeScenarioId,
    title: raw.title,
    package_id: raw.package_id ? Number(raw.package_id) : null,
  });
  toast('Board narrative assembled for approval.');
  await loadBootstrap(state.activeScenarioId);
}

function wireDialogs() {
  const scenarioDialog = $('#scenarioDialog');
  const lineDialog = $('#lineDialog');
  const guidedManualDialog = $('#guidedManualDialog');
  const journalDialog = $('#journalDialog');
  const submissionDialog = $('#submissionDialog');
  const assumptionDialog = $('#assumptionDialog');
  const transferDialog = $('#transferDialog');
  const termDialog = $('#termDialog');
  const tuitionRateDialog = $('#tuitionRateDialog');
  const enrollmentInputDialog = $('#enrollmentInputDialog');
  const tuitionRunDialog = $('#tuitionRunDialog');
  const positionDialog = $('#positionDialog');
  const facultyLoadDialog = $('#facultyLoadDialog');
  const grantDialog = $('#grantDialog');
  const capitalDialog = $('#capitalDialog');
  const typedDriverDialog = $('#typedDriverDialog');
  const cloneScenarioDialog = $('#cloneScenarioDialog');
  const scenarioForecastDialog = $('#scenarioForecastDialog');
  const actualsDialog = $('#actualsDialog');
  const reportDefinitionDialog = $('#reportDefinitionDialog');
  const dashboardWidgetDialog = $('#dashboardWidgetDialog');
  const scheduledExportDialog = $('#scheduledExportDialog');
  const boardPackageDialog = $('#boardPackageDialog');
  const exportArtifactDialog = $('#exportArtifactDialog');
  const snapshotDialog = $('#snapshotDialog');
  const extractRunDialog = $('#extractRunDialog');
  const varianceThresholdDialog = $('#varianceThresholdDialog');
  const varianceExplanationDialog = $('#varianceExplanationDialog');
  const narrativeDialog = $('#narrativeDialog');
  const closeChecklistDialog = $('#closeChecklistDialog');
  const closeTemplateDialog = $('#closeTemplateDialog');
  const closeCalendarDialog = $('#closeCalendarDialog');
  const instantiateCloseDialog = $('#instantiateCloseDialog');
  const reconciliationDialog = $('#reconciliationDialog');
  const entityConfirmationDialog = $('#entityConfirmationDialog');
  const consolidationEntityDialog = $('#consolidationEntityDialog');
  const entityOwnershipDialog = $('#entityOwnershipDialog');
  const consolidationSettingDialog = $('#consolidationSettingDialog');
  const intercompanyDialog = $('#intercompanyDialog');
  const eliminationDialog = $('#eliminationDialog');
  const consolidationDialog = $('#consolidationDialog');
  const connectorDialog = $('#connectorDialog');
  const importDialog = $('#importDialog');
  const guidedImportDialog = $('#guidedImportDialog');
  const guidedExportDialog = $('#guidedExportDialog');
  const excelRoundtripDialog = $('#excelRoundtripDialog');
  const syncJobDialog = $('#syncJobDialog');
  const powerBiExportDialog = $('#powerBiExportDialog');
  const mappingTemplateDialog = $('#mappingTemplateDialog');
  const validationRuleDialog = $('#validationRuleDialog');
  const credentialDialog = $('#credentialDialog');
  const opsCheckDialog = $('#opsCheckDialog');
  const restoreTestDialog = $('#restoreTestDialog');
  const runbookDialog = $('#runbookDialog');
  const commentDialog = $('#commentDialog');
  const attachmentDialog = $('#attachmentDialog');

  $('#newScenarioButton').addEventListener('click', () => scenarioDialog.showModal());
  $('#addLineButton').addEventListener('click', () => lineDialog.showModal());
  $('#guidedManualButton').addEventListener('click', () => guidedManualDialog.showModal());
  $('#guidedImportButton').addEventListener('click', () => guidedImportDialog.showModal());
  $('#guidedExportButton').addEventListener('click', () => guidedExportDialog.showModal());
  $('#completeGuidanceTaskButton').addEventListener('click', () => handleGuidanceTaskComplete().catch((error) => toast(error.message)));
  $('#startPlannerTrainingButton').addEventListener('click', () => handleTrainingModeStart('planner').catch((error) => toast(error.message)));
  $('#startControllerTrainingButton').addEventListener('click', () => handleTrainingModeStart('controller').catch((error) => toast(error.message)));
  $('#startAdminTrainingButton').addEventListener('click', () => handleTrainingModeStart('admin').catch((error) => toast(error.message)));
  $('#createExcelTemplateButton').addEventListener('click', () => handleExcelTemplateCreate().catch((error) => toast(error.message)));
  $('#refreshExcelWorkbookButton').addEventListener('click', () => handleOfficeWorkbookRefresh().catch((error) => toast(error.message)));
  $('#publishExcelWorkbookButton').addEventListener('click', () => handleOfficeWorkbookPublish().catch((error) => toast(error.message)));
  $('#importExcelRoundtripButton').addEventListener('click', () => excelRoundtripDialog.showModal());
  $('#createWorkbookPackageButton').addEventListener('click', () => handleWorkbookPackageCreate().catch((error) => toast(error.message)));
  $('#refreshPowerPointButton').addEventListener('click', () => handlePowerPointRefresh().catch((error) => toast(error.message)));
  $('#addOfficeCommentButton').addEventListener('click', () => handleOfficeCommentCreate().catch((error) => toast(error.message)));
  $('#heroImportButton').addEventListener('click', () => importDialog.showModal());
  $('#heroExportButton').addEventListener('click', () => powerBiExportDialog.showModal());
  $('#marketWatchButton').addEventListener('click', () => openMarketWatch().catch((error) => toast(error.message)));
  $('#marketCloseButton').addEventListener('click', closeMarketWatch);
  $('#marketSearchButton').addEventListener('click', () => handleMarketSearch().catch((error) => toast(error.message)));
  $('#marketFavoriteButton').addEventListener('click', () => handleMarketFavorite().catch((error) => toast(error.message)));
  $('#paperBuyButton').addEventListener('click', () => handlePaperTrade('buy').catch((error) => toast(error.message)));
  $('#paperSellButton').addEventListener('click', () => handlePaperTrade('sell').catch((error) => toast(error.message)));
  $('#paperResetButton').addEventListener('click', () => handlePaperReset().catch((error) => toast(error.message)));
  $('#brokerageCreateButton').addEventListener('click', () => handleBrokerageCreate().catch((error) => toast(error.message)));
  $('#brokerageCredentialButton').addEventListener('click', () => handleBrokerageCredentialSetup().catch((error) => toast(error.message)));
  $('#brokerageConsentButton').addEventListener('click', () => handleBrokerageConsent().catch((error) => toast(error.message)));
  $('#brokerageTestButton').addEventListener('click', () => handleBrokerageTest().catch((error) => toast(error.message)));
  $('#brokerageSyncButton').addEventListener('click', () => handleBrokerageSync().catch((error) => toast(error.message)));
  $('#addJournalButton').addEventListener('click', () => journalDialog.showModal());
  $('#approveScenarioButton').addEventListener('click', () => handleScenarioApprove().catch((error) => toast(error.message)));
  $('#publishScenarioButton').addEventListener('click', () => handleScenarioPublish().catch((error) => toast(error.message)));
  $('#createSubmissionButton').addEventListener('click', () => submissionDialog.showModal());
  $('#addAssumptionButton').addEventListener('click', () => assumptionDialog.showModal());
  $('#requestTransferButton').addEventListener('click', () => transferDialog.showModal());
  $('#addTermButton').addEventListener('click', () => termDialog.showModal());
  $('#addTuitionRateButton').addEventListener('click', () => tuitionRateDialog.showModal());
  $('#addEnrollmentInputButton').addEventListener('click', () => enrollmentInputDialog.showModal());
  $('#runTuitionForecastButton').addEventListener('click', () => tuitionRunDialog.showModal());
  $('#addPositionButton').addEventListener('click', () => positionDialog.showModal());
  $('#addFacultyLoadButton').addEventListener('click', () => facultyLoadDialog.showModal());
  $('#addGrantButton').addEventListener('click', () => grantDialog.showModal());
  $('#addCapitalButton').addEventListener('click', () => capitalDialog.showModal());
  $('#addTypedDriverButton').addEventListener('click', () => typedDriverDialog.showModal());
  $('#cloneScenarioButton').addEventListener('click', () => cloneScenarioDialog.showModal());
  $('#runScenarioForecastButton').addEventListener('click', () => scenarioForecastDialog.showModal());
  $('#ingestActualsButton').addEventListener('click', () => actualsDialog.showModal());
  $('#runForecastVarianceButton').addEventListener('click', () => handleForecastVarianceRun().catch((error) => toast(error.message)));
  $('#selectForecastModelButton').addEventListener('click', () => handleForecastModelSelect().catch((error) => toast(error.message)));
  $('#tuneForecastModelButton').addEventListener('click', () => handleForecastModelTune().catch((error) => toast(error.message)));
  $('#runBacktestButton').addEventListener('click', () => handleForecastBacktest().catch((error) => toast(error.message)));
  $('#compareForecastRecommendationsButton').addEventListener('click', () => handleForecastRecommendationCompare().catch((error) => toast(error.message)));
  $('#explainForecastDriversButton').addEventListener('click', () => handleForecastDriverExplain().catch((error) => toast(error.message)));
  $('#createModelButton').addEventListener('click', () => handleModelCreate().catch((error) => toast(error.message)));
  $('#addModelFormulaButton').addEventListener('click', () => handleModelFormulaCreate().catch((error) => toast(error.message)));
  $('#addAllocationRuleButton').addEventListener('click', () => handleAllocationRuleCreate().catch((error) => toast(error.message)));
  $('#buildModelCubeButton').addEventListener('click', () => handleModelCubeBuild().catch((error) => toast(error.message)));
  $('#publishModelButton').addEventListener('click', () => handleModelPublish().catch((error) => toast(error.message)));
  $('#testModelPerformanceButton').addEventListener('click', () => handleModelPerformanceTest().catch((error) => toast(error.message)));
  $('#recalculateModelButton').addEventListener('click', () => handleModelRecalculate().catch((error) => toast(error.message)));
  $('#addProfitabilityPoolButton').addEventListener('click', () => handleProfitabilityPoolCreate().catch((error) => toast(error.message)));
  $('#runProfitabilityAllocationButton').addEventListener('click', () => handleProfitabilityAllocationRun().catch((error) => toast(error.message)));
  $('#snapshotProfitabilityButton').addEventListener('click', () => handleProfitabilitySnapshot().catch((error) => toast(error.message)));
  $('#addReportButton').addEventListener('click', () => reportDefinitionDialog.showModal());
  $('#addWidgetButton').addEventListener('click', () => dashboardWidgetDialog.showModal());
  $('#scheduleExportButton').addEventListener('click', () => scheduledExportDialog.showModal());
  $('#assembleBoardPackageButton').addEventListener('click', () => boardPackageDialog.showModal());
  $('#createArtifactButton').addEventListener('click', () => exportArtifactDialog.showModal());
  $('#createSnapshotButton').addEventListener('click', () => snapshotDialog.showModal());
  $('#runExtractButton').addEventListener('click', () => extractRunDialog.showModal());
  $('#setVarianceThresholdButton').addEventListener('click', () => varianceThresholdDialog.showModal());
  $('#generateVarianceExplanationsButton').addEventListener('click', () => handleVarianceExplanationGenerate().catch((error) => toast(error.message)));
  $('#draftVarianceNarrativesButton').addEventListener('click', () => handleVarianceNarrativeDraft().catch((error) => toast(error.message)));
  $('#addVarianceCommentaryButton').addEventListener('click', () => varianceExplanationDialog.showModal());
  $('#assembleNarrativeButton').addEventListener('click', () => narrativeDialog.showModal());
  $('#createPaginationProfileButton').addEventListener('click', () => handlePaginationProfileCreate().catch((error) => toast(error.message)));
  $('#addReportFootnoteButton').addEventListener('click', () => handleReportFootnoteCreate().catch((error) => toast(error.message)));
  $('#createBinderBreakButton').addEventListener('click', () => handleBinderBreakCreate().catch((error) => toast(error.message)));
  $('#formatReportChartButton').addEventListener('click', () => handleReportChartFormat().catch((error) => toast(error.message)));
  $('#renderReportChartButton').addEventListener('click', () => handleReportChartRender().catch((error) => toast(error.message)));
  $('#snapshotReportChartButton').addEventListener('click', () => handleReportChartSnapshot().catch((error) => toast(error.message)));
  $('#releaseBoardPackageButton').addEventListener('click', () => handleBoardPackageReleaseFlow().catch((error) => toast(error.message)));
  document.addEventListener('click', (event) => {
    const button = event.target.closest('.download-artifact-button');
    if (!button) return;
    downloadArtifact(button.dataset.url, button.dataset.file).catch((error) => toast(error.message));
  });
  $('#addCloseChecklistButton').addEventListener('click', () => closeChecklistDialog.showModal());
  $('#addCloseTemplateButton').addEventListener('click', () => closeTemplateDialog.showModal());
  $('#addCloseCalendarButton').addEventListener('click', () => closeCalendarDialog.showModal());
  $('#instantiateCloseButton').addEventListener('click', () => instantiateCloseDialog.showModal());
  $('#lockPeriodButton').addEventListener('click', () => handlePeriodLock('locked').catch((error) => toast(error.message)));
  $('#unlockPeriodButton').addEventListener('click', () => handlePeriodLock('open').catch((error) => toast(error.message)));
  $('#addReconciliationButton').addEventListener('click', () => reconciliationDialog.showModal());
  $('#addEntityConfirmationButton').addEventListener('click', () => entityConfirmationDialog.showModal());
  $('#addConsolidationEntityButton').addEventListener('click', () => consolidationEntityDialog.showModal());
  $('#addEntityOwnershipButton').addEventListener('click', () => entityOwnershipDialog.showModal());
  $('#addConsolidationSettingButton').addEventListener('click', () => consolidationSettingDialog.showModal());
  $('#addConsolidationRuleButton').addEventListener('click', () => handleConsolidationRuleCreate().catch((error) => toast(error.message)));
  $('#addIntercompanyButton').addEventListener('click', () => intercompanyDialog.showModal());
  $('#addEliminationButton').addEventListener('click', () => eliminationDialog.showModal());
  $('#assembleStatutoryPackButton').addEventListener('click', () => handleStatutoryPackAssemble().catch((error) => toast(error.message)));
  $('#runConsolidationButton').addEventListener('click', () => consolidationDialog.showModal());
  $('#addConnectorButton').addEventListener('click', () => connectorDialog.showModal());
  $('#runImportButton').addEventListener('click', () => importDialog.showModal());
  $('#previewImportButton').addEventListener('click', () => importDialog.showModal());
  $('#addMappingTemplateButton').addEventListener('click', () => mappingTemplateDialog.showModal());
  $('#addValidationRuleButton').addEventListener('click', () => validationRuleDialog.showModal());
  $('#addCredentialButton').addEventListener('click', () => credentialDialog.showModal());
  $('#startConnectorAuthButton').addEventListener('click', () => handleConnectorAuthStart().catch((error) => toast(error.message)));
  $('#runConnectorHealthButton').addEventListener('click', () => handleConnectorHealthRun().catch((error) => toast(error.message)));
  $('#applyMappingPresetButton').addEventListener('click', () => handleMappingPresetApply().catch((error) => toast(error.message)));
  $('#requestMasterDataChangeButton').addEventListener('click', () => handleMasterDataChangeRequest().catch((error) => toast(error.message)));
  $('#approveMasterDataChangeButton').addEventListener('click', () => handleMasterDataChangeApprove().catch((error) => toast(error.message)));
  $('#addMasterDataMappingButton').addEventListener('click', () => handleMasterDataMappingCreate().catch((error) => toast(error.message)));
  $('#requestMetadataApprovalButton').addEventListener('click', () => handleMetadataApprovalRequest().catch((error) => toast(error.message)));
  $('#buildLineageButton').addEventListener('click', () => handleDataLineageBuild().catch((error) => toast(error.message)));
  $('#runSyncJobButton').addEventListener('click', () => syncJobDialog.showModal());
  $('#createPowerBiExportButton').addEventListener('click', () => powerBiExportDialog.showModal());
  $('#runVarianceAssistantButton').addEventListener('click', () => handleAutomationRun('variance').catch((error) => toast(error.message)));
  $('#runAnomalyAssistantButton').addEventListener('click', () => handleAutomationRun('anomaly').catch((error) => toast(error.message)));
  $('#runBudgetAssistantButton').addEventListener('click', () => handleAutomationRun('budget').catch((error) => toast(error.message)));
  $('#runReconciliationAssistantButton').addEventListener('click', () => handleAutomationRun('reconciliation').catch((error) => toast(error.message)));
  $('#runBudgetUpdateAgentButton').addEventListener('click', () => handlePlanningAgentRun('budget_update').catch((error) => toast(error.message)));
  $('#runBulkAdjustmentAgentButton').addEventListener('click', () => handlePlanningAgentRun('bulk_adjustment').catch((error) => toast(error.message)));
  $('#runReportQuestionAgentButton').addEventListener('click', () => handlePlanningAgentRun('report_question').catch((error) => toast(error.message)));
  $('#runAnomalyExplanationAgentButton').addEventListener('click', () => handlePlanningAgentRun('anomaly_explanation').catch((error) => toast(error.message)));
  $('#approveAgentActionButton').addEventListener('click', () => handleAgentActionApprove().catch((error) => toast(error.message)));
  $('#refreshUniversityAgentButton').addEventListener('click', () => handleUniversityAgentRefresh().catch((error) => toast(error.message)));
  $('#draftAIExplanationButton').addEventListener('click', () => handleAIExplanationDraft().catch((error) => toast(error.message)));
  $('#submitAIExplanationButton').addEventListener('click', () => handleAIExplanationSubmit().catch((error) => toast(error.message)));
  $('#approveAIExplanationButton').addEventListener('click', () => handleAIExplanationApprove().catch((error) => toast(error.message)));
  $('#runOpsCheckButton').addEventListener('click', () => opsCheckDialog.showModal());
  $('#createOpsBackupButton').addEventListener('click', () => handleOpsBackupCreate().catch((error) => toast(error.message)));
  $('#runRestoreTestButton').addEventListener('click', () => restoreTestDialog.showModal());
  $('#runHealthProbeButton').addEventListener('click', () => handleHealthProbeRun().catch((error) => toast(error.message)));
  $('#runBackupDrillButton').addEventListener('click', () => handleBackupDrillRun().catch((error) => toast(error.message)));
  $('#registerRunbookButton').addEventListener('click', () => runbookDialog.showModal());
  $('#runPerformanceLoadTestButton').addEventListener('click', () => handlePerformanceLoadTestRun().catch((error) => toast(error.message)));
  $('#runBenchmarkHarnessButton').addEventListener('click', () => handleBenchmarkHarnessRun().catch((error) => toast(error.message)));
  $('#runParallelCubedButton').addEventListener('click', () => handleParallelCubedRun().catch((error) => toast(error.message)));
  $('#seedIndexStrategyButton').addEventListener('click', () => handleIndexStrategySeed().catch((error) => toast(error.message)));
  $('#enqueuePerformanceJobButton').addEventListener('click', () => handlePerformanceJobEnqueue().catch((error) => toast(error.message)));
  $('#runPerformanceJobButton').addEventListener('click', () => handlePerformanceJobRun().catch((error) => toast(error.message)));
  $('#invalidateCacheButton').addEventListener('click', () => handleCacheInvalidate().catch((error) => toast(error.message)));
  $('#runRestoreAutomationButton').addEventListener('click', () => handleRestoreAutomationRun().catch((error) => toast(error.message)));
  $('#saveEnvironmentButton').addEventListener('click', () => handleEnvironmentSave().catch((error) => toast(error.message)));
  $('#exportConfigButton').addEventListener('click', () => handleConfigExport().catch((error) => toast(error.message)));
  $('#promoteEnvironmentButton').addEventListener('click', () => handleEnvironmentPromotion().catch((error) => toast(error.message)));
  $('#addRollbackPlanButton').addEventListener('click', () => handleRollbackPlanCreate().catch((error) => toast(error.message)));
  $('#publishReleaseNotesButton').addEventListener('click', () => handleReleaseNotesPublish().catch((error) => toast(error.message)));
  $('#runAdminDiagnosticsButton').addEventListener('click', () => handleAdminDiagnosticsRun().catch((error) => toast(error.message)));
  $('#markReadinessButton').addEventListener('click', () => handleReadinessMark().catch((error) => toast(error.message)));
  $('#sealAuditButton').addEventListener('click', () => handleAuditSeal().catch((error) => toast(error.message)));
  $('#verifyAuditButton').addEventListener('click', () => handleAuditVerify().catch((error) => toast(error.message)));
  $('#addRetentionPolicyButton').addEventListener('click', () => handleRetentionPolicyCreate().catch((error) => toast(error.message)));
  $('#classifyTaxActivityButton').addEventListener('click', () => handleTaxActivityClassify().catch((error) => toast(error.message)));
  $('#runTaxUpdateCheckButton').addEventListener('click', () => handleTaxUpdateCheckRun().catch((error) => toast(error.message)));
  $('#createCertificationButton').addEventListener('click', () => handleCertificationCreate().catch((error) => toast(error.message)));
  $('#configureSSOProductionButton').addEventListener('click', () => handleSSOProductionConfigure().catch((error) => toast(error.message)));
  $('#addADOUGroupMappingButton').addEventListener('click', () => handleADOUGroupMappingCreate().catch((error) => toast(error.message)));
  $('#runDomainVPNCheckButton').addEventListener('click', () => handleDomainVPNCheckRun().catch((error) => toast(error.message)));
  $('#startImpersonationButton').addEventListener('click', () => handleAdminImpersonationStart().catch((error) => toast(error.message)));
  $('#addSoDPolicyButton').addEventListener('click', () => handleSoDPolicyCreate().catch((error) => toast(error.message)));
  $('#createAccessReviewButton').addEventListener('click', () => handleAccessReviewCreate().catch((error) => toast(error.message)));
  $('#addCommentButton').addEventListener('click', () => commentDialog.showModal());
  $('#addAttachmentButton').addEventListener('click', () => attachmentDialog.showModal());

  $('#scenarioForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleScenarioCreate(new FormData(event.currentTarget));
      scenarioDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#lineForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleLineCreate(new FormData(event.currentTarget));
      lineDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#guidedManualForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleGuidedManualSave(new FormData(event.currentTarget));
      guidedManualDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#journalForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleJournalCreate(new FormData(event.currentTarget));
      journalDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#submissionForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleSubmissionCreate(new FormData(event.currentTarget));
      submissionDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#assumptionForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleAssumptionCreate(new FormData(event.currentTarget));
      assumptionDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#transferForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleTransferCreate(new FormData(event.currentTarget));
      transferDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#termForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleTermCreate(new FormData(event.currentTarget));
      termDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#tuitionRateForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleTuitionRateCreate(new FormData(event.currentTarget));
      tuitionRateDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#enrollmentInputForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleEnrollmentInputCreate(new FormData(event.currentTarget));
      enrollmentInputDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#tuitionRunForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleTuitionRun(new FormData(event.currentTarget));
      tuitionRunDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#positionForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await postCampus('/api/campus-planning/positions', new FormData(event.currentTarget), ['fte', 'annual_salary', 'benefit_rate', 'vacancy_rate']);
      positionDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#facultyLoadForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await postCampus('/api/campus-planning/faculty-loads', new FormData(event.currentTarget), ['sections', 'credit_hours', 'faculty_fte', 'adjunct_cost']);
      facultyLoadDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#grantForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await postCampus('/api/campus-planning/grants', new FormData(event.currentTarget), ['total_award', 'direct_cost_budget', 'indirect_cost_rate', 'spent_to_date']);
      grantDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#capitalForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await postCampus('/api/campus-planning/capital-requests', new FormData(event.currentTarget), ['capital_cost', 'useful_life_years']);
      capitalDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#typedDriverForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleTypedDriverCreate(new FormData(event.currentTarget));
      typedDriverDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#cloneScenarioForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleScenarioClone(new FormData(event.currentTarget));
      cloneScenarioDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#scenarioForecastForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleScenarioForecast(new FormData(event.currentTarget));
      scenarioForecastDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#actualsForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleActualsIngest(new FormData(event.currentTarget));
      actualsDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#reportDefinitionForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleReportDefinitionCreate(new FormData(event.currentTarget));
      reportDefinitionDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#dashboardWidgetForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleDashboardWidgetCreate(new FormData(event.currentTarget));
      dashboardWidgetDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#scheduledExportForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleScheduledExportCreate(new FormData(event.currentTarget));
      scheduledExportDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#boardPackageForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleBoardPackageCreate(new FormData(event.currentTarget));
      boardPackageDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#exportArtifactForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleExportArtifactCreate(new FormData(event.currentTarget));
      exportArtifactDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#snapshotForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleSnapshotCreate(new FormData(event.currentTarget));
      snapshotDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#extractRunForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleScheduledExtractRun(new FormData(event.currentTarget));
      extractRunDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#varianceThresholdForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleVarianceThresholdCreate(new FormData(event.currentTarget));
      varianceThresholdDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#varianceExplanationForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleVarianceExplanationCreate(new FormData(event.currentTarget));
      varianceExplanationDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#narrativeForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleNarrativeCreate(new FormData(event.currentTarget));
      narrativeDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#closeChecklistForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleCloseChecklistCreate(new FormData(event.currentTarget));
      closeChecklistDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#closeTemplateForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleCloseTemplateCreate(new FormData(event.currentTarget));
      closeTemplateDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#closeCalendarForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleCloseCalendarCreate(new FormData(event.currentTarget));
      closeCalendarDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#instantiateCloseForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleCloseTemplatesInstantiate(new FormData(event.currentTarget));
      instantiateCloseDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#reconciliationForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleReconciliationCreate(new FormData(event.currentTarget));
      reconciliationDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#entityConfirmationForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleEntityConfirmationCreate(new FormData(event.currentTarget));
      entityConfirmationDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#consolidationEntityForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleConsolidationEntityCreate(new FormData(event.currentTarget));
      consolidationEntityDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#entityOwnershipForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleEntityOwnershipCreate(new FormData(event.currentTarget));
      entityOwnershipDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#consolidationSettingForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleConsolidationSettingCreate(new FormData(event.currentTarget));
      consolidationSettingDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#intercompanyForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleIntercompanyCreate(new FormData(event.currentTarget));
      intercompanyDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#eliminationForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleEliminationCreate(new FormData(event.currentTarget));
      eliminationDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#consolidationForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleConsolidationRun(new FormData(event.currentTarget));
      consolidationDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#connectorForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleConnectorCreate(new FormData(event.currentTarget));
      connectorDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#importForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      const formData = new FormData(event.currentTarget);
      if (event.submitter?.value === 'preview') {
        await handleImportPreview(formData);
      } else {
        await handleImportRun(formData);
      }
      importDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#guidedImportForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleGuidedImportRun(new FormData(event.currentTarget));
      guidedImportDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#guidedExportForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleGuidedExportRun(new FormData(event.currentTarget));
      guidedExportDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#excelRoundtripForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleExcelRoundtripImport(new FormData(event.currentTarget));
      excelRoundtripDialog.close();
    } catch (error) {
      toast(error.message);
    }
  });

  $('#mappingTemplateForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleMappingTemplateCreate(new FormData(event.currentTarget));
      mappingTemplateDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#validationRuleForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleValidationRuleCreate(new FormData(event.currentTarget));
      validationRuleDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#credentialForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleCredentialCreate(new FormData(event.currentTarget));
      credentialDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#syncJobForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleSyncJobRun(new FormData(event.currentTarget));
      syncJobDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#powerBiExportForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handlePowerBiExportCreate(new FormData(event.currentTarget));
      powerBiExportDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#opsCheckForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleOpsCheckRun(new FormData(event.currentTarget));
      opsCheckDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#restoreTestForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleRestoreTestRun(new FormData(event.currentTarget));
      restoreTestDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#runbookForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleRunbookRegister(new FormData(event.currentTarget));
      runbookDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#commentForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleCommentCreate(new FormData(event.currentTarget));
      commentDialog.close();
    } catch (error) { toast(error.message); }
  });

  $('#attachmentForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleAttachmentCreate(new FormData(event.currentTarget));
      attachmentDialog.close();
    } catch (error) { toast(error.message); }
  });
}

function wireGlobalActions() {
  $('#commandDeckToggle').addEventListener('click', toggleCommandDeck);

  $('#forecastButton').addEventListener('click', async () => {
    try {
      await handleForecast();
    } catch (error) {
      toast(error.message);
    }
  });
  $('#saveWorkflowVisualButton').addEventListener('click', () => handleWorkflowVisualSave().catch((error) => toast(error.message)));
  $('#addProcessCalendarButton').addEventListener('click', () => handleProcessCalendarCreate().catch((error) => toast(error.message)));
  $('#addSubstituteApproverButton').addEventListener('click', () => handleSubstituteApproverCreate().catch((error) => toast(error.message)));
  $('#assembleWorkflowCertificationButton').addEventListener('click', () => handleWorkflowCertificationAssemble().catch((error) => toast(error.message)));
  $('#monitorWorkflowCampaignButton').addEventListener('click', () => handleWorkflowCampaignMonitor().catch((error) => toast(error.message)));

  $('#refreshButton').addEventListener('click', async () => {
    try {
      await loadBootstrap(state.activeScenarioId);
      toast('Refreshed.');
    } catch (error) {
      toast(error.message);
    }
  });

  $('#scenarioSelect').addEventListener('change', async (event) => {
    state.activeScenarioId = Number(event.target.value);
    await loadBootstrap(state.activeScenarioId);
  });

  $('#periodSelect').addEventListener('change', (event) => {
    state.activePeriod = event.target.value;
    window.localStorage.setItem('mufinances.period', state.activePeriod);
    toast(`Period set to ${state.activePeriod}.`);
  });

  $('#saveProfileButton').addEventListener('click', () => handleProfileSave().catch((error) => toast(error.message)));
  $('#validateGridButton').addEventListener('click', () => handleGridValidate().catch((error) => toast(error.message)));
  $('#bulkPasteButton').addEventListener('click', () => handleBulkPaste().catch((error) => toast(error.message)));

  $('#footerSignOutButton').addEventListener('click', signOut);

  $('#loginForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handleLogin(new FormData(event.currentTarget));
      toast('Signed in.');
    } catch (error) {
      toast(error.message);
    }
  });

  $('#passwordChangeForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await handlePasswordChange(new FormData(event.currentTarget));
      event.currentTarget.reset();
    } catch (error) {
      toast(error.message);
    }
  });
}

window.muFinancesApp = {
  batch: 'B140',
  state,
  api,
  helpers: {
    authHeaders,
    clearSession,
    downloadArtifact,
    renderTable,
    setBusy,
    toast,
  },
  workflows: {
    loadBootstrap,
    showAppShell,
    showAuthGate,
    toggleCommandDeck,
  },
};
window.muFinancesState = state;

(async function init() {
wireDialogs();

  wireGlobalActions();
  window.addEventListener('hashchange', applyRouteFromHash);
  if (new URLSearchParams(window.location.search).get('logout') === '1') {
    clearSession();
    window.history.replaceState({}, document.title, window.location.pathname);
  }
  const sessionOk = await validateSession();
  if (!sessionOk) {
    showAuthGate();
    await loadAuthBootstrap();
    return;
  }
  try {
    await loadBootstrap();
    showAppShell();
  } catch (error) {
    console.error(error);
    if (error.message === 'Password change required before continuing.') {
      return;
    }
    clearSession();
    showAuthGate();
    toast(`Could not load: ${error.message}`);
  }
})();
